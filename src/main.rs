use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::RwLock;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::routing::post;
use axum::Json;
use axum::Router;
use clap::Parser;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tower_http::trace::{self, TraceLayer};
use tracing::Level;

use dumbdb::{Database, GetItemCommand, PutItemCommand, Record, TableDefinition};

const DEFAULT_PORT: u16 = 3000;

/// Our server's CLI
#[derive(clap::Parser, Debug)]
#[command(version, about, long_about = None)]
struct ServerOptions {
    /// Path to a database directory. The directory can be empty but it should exist.
    #[arg(short, long)]
    database_path: String,

    /// Port on which to run the server.
    #[arg(short, long, default_value_t = DEFAULT_PORT)]
    port: u16,
}

struct AppState {
    db: RwLock<Database>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();

    let server_options = ServerOptions::parse();

    let mut db = Database::new(&server_options.database_path).unwrap();

    _populate_data(&mut db, 0, 10, true).unwrap();

    let shared_state = Arc::new(AppState {
        db: RwLock::new(db),
    });

    // our router
    let app = Router::new()
        .route("/", get(root))
        .route("/healthz", get(healthz))
        .route("/api/v1/ddl/create_table", post(create_table_handler))
        .route("/api/v1/dml/get_item", post(get_item_handler))
        .route("/api/v1/dml/put_item", post(put_item_handler))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(trace::DefaultMakeSpan::new().level(Level::INFO))
                .on_response(trace::DefaultOnResponse::new().level(Level::INFO)),
        )
        .with_state(shared_state);

    let addr = SocketAddr::from(([0, 0, 0, 0], server_options.port));
    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    tracing::info!("dumbdb listening on {}", addr);
    axum::serve(listener, app).await.unwrap();
    //axum::Server::bind(&"127.0.0.1:8080".parse().unwrap())
    //    .serve(app.into_make_service())
    //    .await
    //    .unwrap();
}

async fn root() -> &'static str {
    "dumbdb: Hello, World!"
}

async fn healthz() -> &'static str {
    "OK"
}

async fn create_table_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<TableDefinition>,
) -> Result<Json<SuccessMessage>, AppError> {
    let mut db = state.db.write().unwrap();
    db.create_table(payload)?;
    Ok(axum::response::Json(SuccessMessage::new("table created")))
}

async fn get_item_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<GetItemCommand>,
) -> Result<Json<Option<Record>>, AppError> {
    let db = state.db.read().unwrap();
    let result = db.get_item(payload)?;
    Ok(axum::response::Json(result))
}

async fn put_item_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<PutItemCommand>,
) -> Result<Json<SuccessMessage>, AppError> {
    let mut db = state.db.write().unwrap();
    db.put_item(payload)?;
    Ok(axum::response::Json(SuccessMessage::default()))
}

#[derive(Debug, Serialize, Deserialize)]
struct SuccessMessage {
    message: String,
}

impl SuccessMessage {
    pub fn new(prefix: &str) -> Self {
        Self {
            message: format!("{} successfully.", prefix),
        }
    }
}

impl Default for SuccessMessage {
    fn default() -> Self {
        Self {
            message: "success".to_string(),
        }
    }
}

#[derive(Debug)]
struct AppError(anyhow::Error);

// This enables using `?` on functions that return `Result<_, anyhow::Error>` to
// turn them into `Result<_, AppError>`. That way you don't need to do that
// manually.
impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {}", self.0),
        )
            .into_response()
    }
}

fn _populate_data(
    db: &mut Database,
    from: usize,
    to: usize,
    create_table: bool,
) -> Result<(), AppError> {
    if create_table {
        let authors_table = json!({
            "name": "authors",
            "columns": [
                {
                    "name": "id",
                    "type": "Integer",
                },
                {
                    "name": "name",
                    "type": "Text",
                }
            ],
            "primary_key": "id"
        });

        db.create_table(serde_json::from_value(authors_table)?)?;
    }

    for i in from..to {
        let author_item = _create_put_item(i as u64)?;
        db.put_item(author_item)?;
    }

    Ok(())
}

fn _create_get_item(id: u64) -> anyhow::Result<GetItemCommand> {
    Ok(serde_json::from_value(json!({
        "table_name": "authors",
        "key": id.to_string(),
    }))?)
}

fn _create_put_item(id: u64) -> anyhow::Result<PutItemCommand> {
    let rand_string: String = thread_rng()
        .sample_iter(&Alphanumeric)
        .take(6)
        .map(char::from)
        .collect();

    let rand_string_2: String = thread_rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect();
    let rand_num = rand::thread_rng().gen_range(6..99);

    //Ok(serde_json::from_value(json!({
    //    "table_name": "users",
    //    "item": {
    //        "email": format!("{}@example.com", rand_string),
    //        "username": rand_string_2,
    //        "age": rand_num,
    //    }
    //}))?)

    Ok(serde_json::from_value(json!({
        "table_name": "authors",
        "item": {
            "id": id,
            "name": rand_string,
        }
    }))?)
}
