//! Read-only HTTP status and catalog UI.
//!
//! Auger otherwise only speaks the PostgreSQL wire protocol, which makes two
//! ordinary questions awkward to answer: "is the gateway actually talking to
//! MongoDB?" and "what did sampling decide this collection looks like?". The
//! second one is not answerable over SQL at all — a client sees the final Arrow
//! type but not the BSON types it was reconciled from, so a column that came out
//! `Utf8` because the field holds both strings and numbers is indistinguishable
//! from one that was always a string. This surfaces both.
//!
//! Everything here is read-only apart from `?refresh=1`, which re-infers a
//! single table's schema. It binds only when `[server] http_listen` is set.

use std::sync::Arc;
use std::time::Instant;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use bson::doc;
use datafusion::arrow::datatypes::{DataType, Field};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::catalog::infer::{BsonTag, field_is_mixed, field_path};
use crate::catalog::provider::MongoCatalog;
use crate::catalog::store::CatalogStore;
use crate::config::Config;
use crate::mongo::client::{MongoConnection, redact};

#[derive(Clone)]
pub struct AppState {
    pub conn: MongoConnection,
    pub catalog: Arc<MongoCatalog>,
    pub store: Arc<CatalogStore>,
    pub config: Arc<Config>,
    pub started: Instant,
}

/// An error carried back to the browser as JSON rather than an empty 500.
struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

type ApiResult = Result<Json<Value>, ApiError>;

pub async fn serve(addr: String, state: AppState) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/", get(index))
        .route("/api/health", get(health))
        .route("/api/catalog", get(catalog))
        .route("/api/table/{db}/{table}", get(table))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|e| anyhow::anyhow!("binding http UI to {addr}: {e}"))?;
    tracing::info!(%addr, "http UI listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn index() -> Html<&'static str> {
    Html(include_str!("ui.html"))
}

/// Liveness of the gateway *and* of its MongoDB connection, which are not the
/// same thing: Auger keeps serving SQL (and failing every scan) if Mongo goes
/// away, so a plain "process is up" check would be misleading.
async fn health(State(st): State<AppState>) -> Json<Value> {
    let ping = st
        .conn
        .client()
        .database("admin")
        .run_command(doc! { "ping": 1 })
        .await;
    let (reachable, error) = match ping {
        Ok(_) => (true, None),
        Err(e) => (false, Some(e.to_string())),
    };

    let cached = st
        .catalog
        .databases()
        .iter()
        .map(|db| st.store.known_tables(db).len())
        .sum::<usize>();

    Json(json!({
        "ok": reachable,
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_secs": st.started.elapsed().as_secs(),
        "mongo": {
            "uri": redact(&st.config.mongo.uri),
            "reachable": reachable,
            "error": error,
        },
        "sql": {
            "listen": st.config.server.listen,
            "auth": st.config.server.auth,
        },
        "catalog": {
            "sample_size": st.config.catalog.sample_size,
            "recent_bias": st.config.catalog.recent_bias,
            "max_depth": st.config.catalog.max_depth,
            "rare_field_threshold": st.config.catalog.rare_field_threshold,
            "refresh_interval_secs": st.config.catalog.refresh_interval_secs,
            "cached_tables": cached,
        },
    }))
}

/// Every exposed database and its collections. Deliberately does not infer
/// anything: listing is cheap, sampling is not, so a table's shape is only
/// loaded when someone asks for that table.
async fn catalog(State(st): State<AppState>) -> ApiResult {
    let mut databases = Vec::new();
    for db in st.catalog.databases() {
        let names = st.conn.collections(db).await.map_err(|e| {
            ApiError(StatusCode::BAD_GATEWAY, format!("listing {db}: {e}"))
        })?;
        let tables = names
            .into_iter()
            .map(|name| {
                let cached = st.store.get(&CatalogStore::key(db, &name));
                json!({
                    "name": name,
                    "inferred": cached.is_some(),
                    "columns": cached.as_ref().map(|c| c.schema.fields().len()),
                    "doc_count": cached.as_ref().map(|c| c.stats.doc_count),
                    "inferred_at": cached.as_ref().map(|c| c.inferred_at),
                })
            })
            .collect::<Vec<_>>();
        databases.push(json!({ "name": db, "tables": tables }));
    }
    Ok(Json(json!({ "databases": databases })))
}

#[derive(Debug, Deserialize)]
struct TableQuery {
    /// Force re-sampling instead of serving the cached schema.
    #[serde(default)]
    refresh: bool,
}

async fn table(
    Path((db, table)): Path<(String, String)>,
    Query(q): Query<TableQuery>,
    State(st): State<AppState>,
) -> ApiResult {
    if !st.catalog.databases().iter().any(|d| d == &db) {
        return Err(ApiError(StatusCode::NOT_FOUND, format!("no such database: {db}")));
    }

    let key = CatalogStore::key(&db, &table);
    let ttl = st.config.catalog.refresh_interval_secs;
    let cached = if q.refresh { None } else { st.store.get_fresh(&key, ttl) };

    let entry = match cached {
        Some(entry) => entry,
        None => {
            let schema = st
                .conn
                .infer_schema(&db, &table, &st.config.catalog)
                .await
                .map_err(|e| {
                    ApiError(StatusCode::BAD_GATEWAY, format!("inferring {key}: {e}"))
                })?;
            let stats = st.conn.stats(&db, &table).await;
            st.store.put(key.clone(), schema, stats)
        }
    };

    let columns: Vec<Value> = entry.schema.fields().iter().map(|f| field_json(f)).collect();
    Ok(Json(json!({
        "db": db,
        "table": table,
        "inferred_at": entry.inferred_at,
        "stats": {
            "doc_count": entry.stats.doc_count,
            "avg_doc_size": entry.stats.avg_doc_size,
            "total_size": entry.stats.total_size,
            "indexed_paths": entry.stats.indexed_paths,
        },
        "columns": columns,
    })))
}

/// One column, with the nested children of a struct or list inlined so the UI
/// can render the document shape rather than a flat list of top-level names.
fn field_json(f: &Field) -> Value {
    let children: Vec<Value> = match f.data_type() {
        DataType::Struct(fields) => fields.iter().map(|c| field_json(c)).collect(),
        DataType::List(item) => vec![field_json(item)],
        _ => Vec::new(),
    };
    json!({
        "name": f.name(),
        "path": field_path(f),
        "type": f.data_type().to_string(),
        "bson": BsonTag::of(f).as_str(),
        "mixed": field_is_mixed(f),
        "nullable": f.is_nullable(),
        "children": children,
    })
}
