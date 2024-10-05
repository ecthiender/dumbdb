use axum::routing::get;
use axum::routing::post;
use axum::Router;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use dumbdb::{CreateTableCommand, Database, GetItemCommand, PutItemCommand, Record};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tower_http::trace::{self, TraceLayer};
use tracing::Level;

struct AppState {
    db: Database,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();

    let db = Database::new("./data/dumbdb").unwrap();
    let shared_state = Arc::new(AppState { db });

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

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
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
    Json(payload): Json<CreateTableCommand>,
) -> Result<Json<SuccessMessage>, AppError> {
    let mut db = state.db.clone();
    db.create_table(payload)?;
    Ok(axum::response::Json(SuccessMessage::new("table created")))
}

async fn get_item_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<GetItemCommand>,
) -> Result<Json<Record>, AppError> {
    let result = state.db.get_item(payload)?;
    Ok(axum::response::Json(result))
}

async fn put_item_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<PutItemCommand>,
) -> Result<Json<SuccessMessage>, AppError> {
    let mut db = state.db.clone();
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

fn _other_mn() -> anyhow::Result<()> {
    println!("Hello, world! Executing commands in dumbdb ----> ");
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

    let mut db = Database::new("./data/dumbdb")?;
    db.create_table(serde_json::from_value(authors_table)?)?;

    for i in 0..10000 {
        let author_item = _create_put_item(i)?;
        db.put_item(author_item)?;
    }

    for i in 5672..8764 {
        let cmd = _create_get_item(i)?;
        let record = db.get_item(cmd)?;
        println!("Get Item of {}: Result: {:?}", i, record);
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
        .take(30)
        .map(char::from)
        .collect();

    Ok(serde_json::from_value(json!({
        "table_name": "authors",
        "item": {
            "id": id,
            "name": rand_string,
        }
    }))?)
}
