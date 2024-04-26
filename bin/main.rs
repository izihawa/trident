use axum::body::Body;
use axum::extract::{Host, Query, Request};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{
    extract::{Path, State},
    Json, Router,
};
use clap::{Parser, Subcommand};
use futures::TryStreamExt;
use headers::{HeaderMap, HeaderMapExt, Range};
use hyper::header;
use iroh::rpc_protocol::ShareMode;
use iroh::sync::store::DownloadPolicy;
use iroh_base::hash::Hash;
use iroh_base::node_addr::NodeAddr;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tokio::runtime::Builder;
use tokio::signal;
use tokio::sync::RwLock;
use tokio_util::io::{ReaderStream, StreamReader};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tower_http::cors::CorsLayer;
use tower_http::trace::{self, TraceLayer};
use tracing::{info, info_span, Instrument, Level};
use tracing_subscriber::EnvFilter;
use trident_storage::config::{Config, TableConfig};
use trident_storage::error::Error;

use trident_storage::iroh_node::IrohNode;
use trident_storage::ranges::parse_byte_range;

/// Simple program to greet a person
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Launch server
    Serve {
        /// Config path
        #[arg(short, long)]
        config_path: PathBuf,
    },
    /// Generate default config
    GenerateConfig {
        /// Base path for Trident data
        base_path: PathBuf,
        /// The number of auto-generated sub-directories for keeping data
        shards: Option<u32>,
    },
}

#[derive(Deserialize)]
struct TablesCreateRequest {
    storage: Option<String>,
}

#[derive(Deserialize)]
struct TablesImportRequest {
    ticket: String,
    #[serde(default)]
    storage: Option<String>,
    #[serde(default)]
    download_policy: Option<DownloadPolicy>,
}

#[derive(Deserialize)]
struct TablesSyncRequest {
    #[serde(default)]
    download_policy: Option<DownloadPolicy>,
    threads: u32,
}

#[derive(Deserialize)]
struct TablesForeignInsertRequest {
    from_table: String,
    from_key: String,
    to_table: String,
    to_key: String,
}

#[derive(Clone)]
struct AppState {
    iroh_node: Arc<RwLock<IrohNode>>,
    config: Arc<RwLock<Config>>,
    config_path: PathBuf,
}

#[derive(Serialize)]
struct TablesExistsResponse {
    pub exists: bool,
}

#[derive(Serialize)]
struct TablesPeersResponse {
    pub peers: Option<Vec<NodeAddr>>,
}

#[derive(Serialize)]
struct TablesLsResponse {
    pub tables: HashMap<String, TableConfig>,
}

async fn create_state(
    config_path: &std::path::Path,
    cancellation_token: CancellationToken,
    task_tracker: TaskTracker,
) -> Result<AppState, Error> {
    let config = Arc::new(RwLock::new(Config::load_config(config_path).await?));
    let state = AppState {
        iroh_node: Arc::new(RwLock::new(
            IrohNode::new(config.clone(), cancellation_token, task_tracker).await?,
        )),
        config: config.clone(),
        config_path: config_path.to_path_buf(),
    };
    config.read().await.save_config(config_path).await?;
    Ok(state)
}

async fn app() -> Result<(), Error> {
    let args = Args::parse();
    match args.command {
        Commands::Serve {
            config_path: config,
        } => {
            let cancellation_token = CancellationToken::new();
            let task_tracker = TaskTracker::new();

            let state =
                create_state(&config, cancellation_token.clone(), task_tracker.clone()).await?;
            let iroh_node = state.iroh_node.clone();
            let config = state.config.clone();

            // build our application with a route
            let mut router = Router::new()
                .route("/blobs/:hash", get(blobs_get))
                .route("/tables/", get(tables_ls))
                .route("/tables/:table/", post(tables_create))
                .route("/tables/:table/exists/", get(tables_exists))
                .route("/tables/:table/peers/", get(tables_peers))
                .route("/tables/:table/", delete(tables_drop))
                .route("/tables/:table/import/", post(tables_import))
                .route("/tables/:table/sync/", post(tables_sync))
                .route("/tables/:table/", get(table_ls))
                .route("/tables/:table/share/:mode/", get(table_share))
                .route("/tables/:table/*key", get(table_get))
                .route("/tables/:table/*key", put(table_insert))
                .route("/tables/:table/*key", delete(table_delete))
                .route("/tables/foreign_insert/", post(table_foreign_insert));

            if config.read().await.http.hostname.is_some() {
                router = router.route("/:table/*key", get(table_root_get));
            }

            let app = router
                .layer(
                    TraceLayer::new_for_http()
                        .make_span_with(trace::DefaultMakeSpan::new().level(Level::INFO))
                        .on_response(trace::DefaultOnResponse::new().level(Level::INFO)),
                )
                .layer(CorsLayer::permissive())
                .with_state(state);

            // run our app with hyper, listening globally on port 3000
            let listener = tokio::net::TcpListener::bind(&config.read().await.http.endpoint)
                .await
                .map_err(Error::io_error)?;

            axum::serve(listener, app)
                .with_graceful_shutdown(
                    shutdown_signal(cancellation_token, task_tracker)
                        .instrument(info_span!(parent: None, "shutdown_signal")),
                )
                .await
                .map_err(Error::io_error)?;

            iroh_node
                .read()
                .await
                .send_shutdown()
                .await
                .map_err(Error::failed_shutdown)?;

            match Arc::try_unwrap(iroh_node) {
                Ok(iroh_node) => iroh_node
                    .into_inner()
                    .shutdown()
                    .map_err(Error::failed_shutdown)?,
                Err(_) => Err(Error::io_error("iroh_node cannot be destructed"))?,
            };
        }
        Commands::GenerateConfig { base_path, shards } => {
            println!(
                "{}",
                serde_yaml::to_string(&Config::new(&base_path, shards)).expect("unreachable")
            )
        }
    }
    Ok(())
}

async fn shutdown_signal(cancellation_token: CancellationToken, task_tracker: TaskTracker) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    task_tracker.close();
    cancellation_token.cancel();
    info!(tasks = task_tracker.len(), "stopping_tasks");
    task_tracker.wait().await;
    info!("stopped_tasks");
}

fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_env_filter(EnvFilter::from_default_env())
        .compact()
        .init();
    Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(app())
}

async fn blobs_get(
    State(state): State<AppState>,
    method: Method,
    Path(hash_str): Path<String>,
) -> Response {
    let Ok(hash) = Hash::from_str(&hash_str).map_err(Error::blobs) else {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::default())
            .unwrap();
    };
    match state.iroh_node.read().await.blobs_get(hash).await {
        Ok(Some((reader, file_size))) => match method {
            Method::HEAD => Response::builder()
                .header(header::CONTENT_LENGTH, file_size)
                .header(
                    header::CONTENT_TYPE,
                    mime_guess::mime::OCTET_STREAM.to_string(),
                )
                .header("X-Iroh-Hash", hash_str)
                .body(Body::default())
                .unwrap(),
            Method::GET => Response::builder()
                .header(header::CONTENT_LENGTH, file_size)
                .header(
                    header::CONTENT_TYPE,
                    mime_guess::mime::OCTET_STREAM.to_string(),
                )
                .header("X-Iroh-Hash", hash_str)
                .body(Body::from_stream(ReaderStream::new(reader)))
                .unwrap(),
            _ => unreachable!(),
        },
        Ok(None) => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::default())
            .unwrap(),
        Err(e) => e.into_response(),
    }
}

async fn tables_create(
    State(state): State<AppState>,
    Path(table): Path<String>,
    Json(tables_create_request): Json<TablesCreateRequest>,
) -> Response {
    match state
        .iroh_node
        .write()
        .await
        .tables_create(&table, tables_create_request.storage)
        .await
    {
        Ok(id) => {
            match state
                .config
                .read()
                .await
                .save_config(&state.config_path)
                .await
            {
                Ok(_) => Response::builder()
                    .body(Body::from(id.to_string()))
                    .unwrap(),
                Err(error) => error.into_response(),
            }
        }
        Err(e) => e.into_response(),
    }
}

async fn tables_exists(State(state): State<AppState>, Path(table): Path<String>) -> Response {
    Json(TablesExistsResponse {
        exists: state.iroh_node.read().await.tables_exists(&table).await,
    })
    .into_response()
}

async fn tables_peers(State(state): State<AppState>, Path(table): Path<String>) -> Response {
    match state.iroh_node.read().await.tables_peers(&table).await {
        Ok(peers) => Json(TablesPeersResponse { peers }).into_response(),
        Err(error) => error.into_response(),
    }
}

async fn tables_import(
    State(state): State<AppState>,
    Path(table): Path<String>,
    Json(tables_import_request): Json<TablesImportRequest>,
) -> Response {
    match state
        .iroh_node
        .write()
        .await
        .tables_import(
            &table,
            &tables_import_request.ticket,
            tables_import_request.storage,
            tables_import_request
                .download_policy
                .unwrap_or_else(|| DownloadPolicy::EverythingExcept(vec![])),
        )
        .await
    {
        Ok(id) => {
            match state
                .config
                .read()
                .await
                .save_config(&state.config_path)
                .await
            {
                Ok(_) => Response::builder()
                    .body(Body::from(id.to_string()))
                    .unwrap(),
                Err(error) => error.into_response(),
            }
        }
        Err(e) => e.into_response(),
    }
}

async fn tables_sync(
    State(state): State<AppState>,
    Path(table): Path<String>,
    Json(tables_sync_request): Json<TablesSyncRequest>,
) -> Response {
    match state
        .iroh_node
        .read()
        .await
        .tables_sync(
            &table,
            tables_sync_request.download_policy,
            tables_sync_request.threads,
        )
        .await
    {
        Ok(_) => Response::builder().body(Body::default()).unwrap(),
        Err(e) => e.into_response(),
    }
}

async fn tables_ls(State(state): State<AppState>) -> Response {
    Json(TablesLsResponse {
        tables: state.iroh_node.read().await.tables_ls().await,
    })
    .into_response()
}

async fn tables_drop(State(state): State<AppState>, Path(table): Path<String>) -> Response {
    match state.iroh_node.write().await.tables_drop(&table).await {
        Ok(()) => {
            match state
                .config
                .read()
                .await
                .save_config(&state.config_path)
                .await
            {
                Ok(_) => Response::builder().body(Body::default()).unwrap(),
                Err(error) => error.into_response(),
            }
        }
        Err(e) => e.into_response(),
    }
}

async fn table_share(
    State(state): State<AppState>,
    Path((table, mode)): Path<(String, ShareMode)>,
) -> Response {
    match state.iroh_node.read().await.table_share(&table, mode).await {
        Ok(ticket) => Response::builder()
            .body(Body::from(ticket.to_string()))
            .unwrap(),
        Err(e) => e.into_response(),
    }
}

async fn table_insert(
    State(state): State<AppState>,
    Path((table, key)): Path<(String, String)>,
    request: Request,
) -> Response {
    let stream = request.into_body().into_data_stream();
    let body_with_io_error =
        stream.map_err(|err| tokio::io::Error::new(tokio::io::ErrorKind::Other, err));
    let body_reader = StreamReader::new(body_with_io_error);
    match state
        .iroh_node
        .read()
        .await
        .table_insert(&table, &key, body_reader)
        .await
    {
        Ok(hash) => Response::builder()
            .header("X-Iroh-Hash", hash.to_string())
            .body(Body::default())
            .unwrap(),
        Err(e) => e.into_response(),
    }
}

async fn table_foreign_insert(
    State(state): State<AppState>,
    Query(query): Query<TablesForeignInsertRequest>,
) -> Response {
    match state
        .iroh_node
        .read()
        .await
        .tables_foreign_insert(
            &query.from_table,
            &query.from_key,
            &query.to_table,
            &query.to_key,
        )
        .await
    {
        Ok(hash) => Response::builder()
            .body(Body::from(hash.to_string()))
            .unwrap(),
        Err(e) => e.into_response(),
    }
}

async fn table_root_get(
    State(state): State<AppState>,
    method: Method,
    headers: HeaderMap,
    Host(subdomain): Host,
    Path(key): Path<String>,
) -> Response {
    let Some(config_hostname) = state.config.read().await.http.hostname.clone() else {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::default())
            .unwrap();
    };

    let table = match subdomain.strip_suffix(&format!(".{}", config_hostname)) {
        None => {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::default())
                .unwrap()
        }
        Some(table) => table,
    };
    table_get(
        State(state),
        method,
        headers,
        Path((table.to_string(), key)),
    )
    .await
}
async fn table_get(
    State(state): State<AppState>,
    method: Method,
    headers: HeaderMap,
    Path((table, key)): Path<(String, String)>,
) -> Response {
    let iroh_node = state.iroh_node.read().await;
    let entry = iroh_node.table_get(&table, &key).await;
    let entry = match entry {
        Ok(Some(entry)) => entry,
        Ok(None) => {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::default())
                .unwrap()
        }
        Err(e) => return e.into_response(),
    };

    let response_builder =
        Response::builder().header("X-Iroh-Hash", entry.content_hash().to_string());

    let (reader, response_builder) = match headers.typed_get::<Range>() {
        None => (
            iroh_node
                .client()
                .blobs
                .read(entry.content_hash())
                .await
                .map_err(Error::blobs),
            response_builder
                .status(StatusCode::OK)
                .header(header::CONTENT_LENGTH, entry.content_len())
                .header(
                    header::CONTENT_TYPE,
                    mime_guess::from_path(&key)
                        .first_or_octet_stream()
                        .to_string(),
                ),
        ),
        Some(range_value) => {
            let (start, end) = match parse_byte_range(range_value).map_err(Error::blobs) {
                Ok((start, end)) => (start, end),
                Err(e) => return e.into_response(),
            };
            let offset = start.unwrap_or(0);
            let length = end.map(|end| end - offset);
            let definite_length = length.unwrap_or(entry.content_len() - offset);
            (
                iroh_node
                    .client()
                    .blobs
                    .read_at(entry.content_hash(), offset, length.map(|x| x as usize))
                    .await
                    .map_err(Error::blobs),
                response_builder
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header(header::ACCEPT_RANGES, "bytes")
                    .header(header::CONTENT_LENGTH, definite_length)
                    .header(
                        header::CONTENT_RANGE,
                        format_content_range(start, end.map(|end| end - 1), entry.content_len()),
                    )
                    .header(
                        header::CONTENT_TYPE,
                        if definite_length == entry.content_len() {
                            mime_guess::from_path(&key)
                                .first_or_octet_stream()
                                .to_string()
                        } else {
                            mime_guess::mime::OCTET_STREAM.to_string()
                        },
                    ),
            )
        }
    };
    let reader = match reader {
        Ok(reader) => reader,
        Err(e) => return e.into_response(),
    };
    match method {
        Method::HEAD => response_builder.body(Body::default()).unwrap(),
        Method::GET => response_builder
            .body(Body::from_stream(ReaderStream::new(reader)))
            .unwrap(),
        _ => unreachable!(),
    }
}

async fn table_delete(
    State(state): State<AppState>,
    Path((table, key)): Path<(String, String)>,
) -> Response {
    match state
        .iroh_node
        .read()
        .await
        .table_delete(&table, &key)
        .await
    {
        Ok(removed) => Response::builder()
            .body(Body::from(format!("{}", removed)))
            .unwrap(),
        Err(e) => e.into_response(),
    }
}

async fn table_ls(State(state): State<AppState>, Path(table): Path<String>) -> Response {
    match state.iroh_node.read().await.table_keys(&table) {
        Some(stream) => Response::builder().body(Body::from_stream(stream)).unwrap(),
        None => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::default())
            .unwrap(),
    }
}

fn format_content_range(start: Option<u64>, end: Option<u64>, size: u64) -> String {
    format!(
        "bytes {}-{}/{}",
        start.map(|x| x.to_string()).unwrap_or_default(),
        end.map(|x| x.to_string())
            .unwrap_or_else(|| (size - 1).to_string()),
        size
    )
}
