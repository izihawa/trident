use axum::body::Body;
use axum::extract::{Query, Request};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{
    extract::{Path, State},
    Json, Router,
};
use clap::{Parser, Subcommand};
use futures::TryStreamExt;
use iroh::sync::store::DownloadPolicy;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::runtime::Builder;
use tokio::signal;
use tokio::sync::RwLock;
use tokio_util::io::{ReaderStream, StreamReader};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tower_http::trace::{self, TraceLayer};
use tracing::{info, info_span, Instrument, Level};
use tracing_subscriber::EnvFilter;
use trident_storage::config::{Config, SinkConfig, TableConfig};
use trident_storage::error::Error;

use trident_storage::iroh_node::IrohNode;

fn return_true() -> bool {
    true
}
fn return_false() -> bool {
    false
}

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
        #[arg(default_value_t = 1)]
        shards: u32,
    },
}

#[derive(Deserialize)]
struct TablesCreateRequest {
    storage: String,
    #[serde(default)]
    sinks: Vec<String>,
    #[serde(default = "return_true")]
    keep_blob: bool,
}

#[derive(Deserialize)]
struct TablesImportRequest {
    ticket: String,
    storage: String,
    #[serde(default)]
    download_policy: Option<DownloadPolicy>,
    #[serde(default)]
    sinks: Vec<String>,
    #[serde(default = "return_true")]
    keep_blob: bool,
}

#[derive(Deserialize)]
struct TablesSyncRequest {
    #[serde(default)]
    download_policy: Option<DownloadPolicy>,
    threads: u32,
    #[serde(default = "return_false")]
    should_send_to_sink: bool,
}

#[derive(Deserialize)]
struct TablesInsertRequest {}

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
struct SinksLsResponse {
    pub sinks: HashMap<String, SinkConfig>,
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
            let app = Router::new()
                .route("/sinks/", get(sinks_ls))
                .route("/sinks/:sink/", post(sinks_create))
                .route("/tables/", get(tables_ls))
                .route("/tables/:table/", post(tables_create))
                .route("/tables/:table/exists/", get(tables_exists))
                .route("/tables/:table/", delete(tables_drop))
                .route("/tables/:table/import/", post(tables_import))
                .route("/tables/:table/integrity/", post(tables_integrity))
                .route("/tables/:table/sync/", post(tables_sync))
                .route("/tables/:table/", get(table_ls))
                .route("/tables/:table/share/", get(table_share))
                .route("/tables/:table/*key", get(table_get))
                .route("/tables/:table/*key", put(table_insert))
                .route("/tables/:table/*key", delete(table_delete))
                .route("/tables/foreign_insert/", post(table_foreign_insert))
                .layer(
                    TraceLayer::new_for_http()
                        .make_span_with(trace::DefaultMakeSpan::new().level(Level::INFO))
                        .on_response(trace::DefaultOnResponse::new().level(Level::INFO)),
                )
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

async fn sinks_ls(State(state): State<AppState>) -> Response {
    Json(SinksLsResponse {
        sinks: state.iroh_node.read().await.sinks_ls().await,
    })
    .into_response()
}

async fn sinks_create(
    State(state): State<AppState>,
    Path(sink): Path<String>,
    Json(sink_config): Json<SinkConfig>,
) -> Response {
    match state
        .iroh_node
        .read()
        .await
        .sinks_create(&sink, sink_config)
        .await
    {
        Ok(_) => {
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

async fn tables_create(
    State(state): State<AppState>,
    Path(table): Path<String>,
    Json(tables_create_request): Json<TablesCreateRequest>,
) -> Response {
    match state
        .iroh_node
        .write()
        .await
        .tables_create(
            &table,
            &tables_create_request.storage,
            tables_create_request.sinks,
            tables_create_request.keep_blob,
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

async fn tables_exists(State(state): State<AppState>, Path(table): Path<String>) -> Response {
    Json(TablesExistsResponse {
        exists: state.iroh_node.read().await.tables_exists(&table).await,
    })
    .into_response()
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
            &tables_import_request.storage,
            tables_import_request
                .download_policy
                .unwrap_or_else(|| DownloadPolicy::EverythingExcept(vec![])),
            tables_import_request.sinks,
            tables_import_request.keep_blob,
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

async fn tables_integrity(State(state): State<AppState>, Path(table): Path<String>) -> Response {
    match state.iroh_node.read().await.tables_integrity(&table).await {
        Ok(_) => Response::builder().body(Body::default()).unwrap(),
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
            tables_sync_request.should_send_to_sink,
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

async fn table_share(State(state): State<AppState>, Path(table): Path<String>) -> Response {
    match state.iroh_node.read().await.table_share(&table).await {
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
            .body(Body::from(hash.to_string()))
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

async fn table_get(
    State(state): State<AppState>,
    method: Method,
    Path((table, key)): Path<(String, String)>,
) -> Response {
    match state.iroh_node.read().await.table_get(&table, &key).await {
        Ok(Some((reader, file_size))) => match method {
            Method::HEAD => Response::builder()
                .header("Content-Length", file_size)
                .body(Body::default())
                .unwrap(),
            Method::GET => Response::builder()
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
