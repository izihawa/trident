use axum::body::Body;
use axum::extract::{Query, Request};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, post, put};
use axum::{
    extract::{Path, State},
    routing::get,
    Json, Router,
};
use clap::Parser;
use futures::TryStreamExt;
use iroh::sync::store::DownloadPolicy;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Builder;
use tokio::sync::RwLock;
use tokio_util::io::{ReaderStream, StreamReader};
use tower_http::trace::{self, TraceLayer};
use tracing::Level;
use tracing_subscriber::EnvFilter;
use trident_storage::config::{
    load_config, save_config, Config, MirroringConfig, SinkConfig, TableConfig,
};
use trident_storage::error::Error;

use trident_storage::iroh_node::IrohNode;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Config path
    #[arg(short, long)]
    config: String,
}

#[derive(Deserialize)]
struct TablesCreateRequest {
    storage: Option<String>,
    mirroring: Option<MirroringConfig>,
}

#[derive(Deserialize)]
struct TablesImportRequest {
    ticket: String,
    download_policy: DownloadPolicy,
    storage: Option<String>,
    mirroring: Option<MirroringConfig>,
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
    config_path: String,
}

#[derive(Serialize)]
struct TableExistsResponse {
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

async fn create_app(args: Args) -> Result<AppState, Error> {
    let config = Arc::new(RwLock::new(load_config(&args.config).await?));
    let state = AppState {
        iroh_node: Arc::new(RwLock::new(IrohNode::new(config.clone()).await?)),
        config: config.clone(),
        config_path: args.config.clone(),
    };
    save_config(&args.config, &config.read().await.clone()).await?;
    Ok(state)
}

async fn app() -> Result<(), Error> {
    let args = Args::parse();

    let state = create_app(args).await?;
    let config = state.config.clone();

    // build our application with a route
    let app = Router::new()
        .route("/sinks/", get(sinks_ls))
        .route("/sinks/:sink/", post(sinks_create))
        .route("/tables/", get(tables_ls))
        .route("/tables/:table/", post(tables_create))
        .route("/tables/:table/", delete(tables_drop))
        .route("/tables/:table/import/", post(tables_import))
        .route("/tables/:table/", get(table_ls))
        .route("/tables/:table/share/", get(table_share))
        .route("/tables/:table/:key/", get(table_get))
        .route("/tables/:table/:key/exists/", get(table_exists))
        .route("/tables/:table/:key/", put(table_insert))
        .route("/tables/:table/:key/", delete(table_delete))
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
        .unwrap();

    axum::serve(listener, app).await.unwrap();
    Ok(())
}

fn main() -> Result<(), Error> {
    // initialize tracing
    tracing_subscriber::fmt()
        .with_target(false)
        .with_env_filter(EnvFilter::from_default_env())
        .compact()
        .init();
    /*console_subscriber::ConsoleLayer::builder()
    // set how long the console will retain data from completed tasks
    .retention(Duration::from_secs(120))
    // set the address the server is bound to
    .server_addr(([0, 0, 0, 0], 6669))
    // ... other configurations ...
    .init();*/
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
        Ok(_) => StatusCode::OK.into_response(),
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
            tables_create_request.storage.as_deref(),
            tables_create_request.mirroring,
        )
        .await
    {
        Ok(id) => {
            save_config(&state.config_path, &state.config.read().await.clone())
                .await
                .unwrap();
            Response::builder()
                .body(Body::from(id.to_string()))
                .unwrap()
        }
        Err(e) => e.into_response(),
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
            tables_import_request.download_policy,
            tables_import_request.storage.as_deref(),
            tables_import_request.mirroring,
        )
        .await
    {
        Ok(id) => {
            save_config(&state.config_path, &state.config.read().await.clone())
                .await
                .unwrap();
            Response::builder()
                .body(Body::from(id.to_string()))
                .unwrap()
        }
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
            save_config(&state.config_path, &state.config.read().await.clone())
                .await
                .unwrap();
            Response::builder().body(Body::default()).unwrap()
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
    Path((table, key)): Path<(String, String)>,
) -> Response {
    match state.iroh_node.read().await.table_get(&table, &key).await {
        Ok(reader) => Response::builder()
            .body(Body::from_stream(ReaderStream::new(reader)))
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

async fn table_exists(
    State(state): State<AppState>,
    Path((table, key)): Path<(String, String)>,
) -> Response {
    match state
        .iroh_node
        .read()
        .await
        .table_exists(&table, &key)
        .await
    {
        Ok(exists) => Json(TableExistsResponse { exists }).into_response(),
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
