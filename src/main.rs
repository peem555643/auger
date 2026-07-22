//! Auger — a SQL gateway for MongoDB.
//!
//! Connects to a MongoDB deployment, infers a stable Arrow schema for every
//! collection, and serves SQL over the PostgreSQL wire protocol with as much of
//! each query as possible pushed into the server's aggregation pipeline.

mod catalog;
mod config;
mod mongo;
mod server;

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use datafusion::catalog::{CatalogProvider, MemorySchemaProvider};
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::catalog::provider::MongoCatalog;
use crate::catalog::store::CatalogStore;
use crate::config::Config;
use crate::mongo::client::MongoConnection;

/// Name of the SQL catalog that holds every Mongo database.
const CATALOG: &str = "auger";

#[derive(Debug, Parser)]
#[command(name = "auger", version, about = "SQL gateway for MongoDB")]
struct Cli {
    /// Path to a TOML configuration file.
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// MongoDB connection string. Overrides the configuration file.
    #[arg(long, env = "AUGER_MONGO_URI")]
    mongo_uri: Option<String>,

    /// Address to listen on for PostgreSQL clients.
    #[arg(long, env = "AUGER_LISTEN")]
    listen: Option<String>,

    /// Documents to sample per collection when inferring a schema.
    #[arg(long)]
    sample_size: Option<i64>,

    /// File in which inferred schemas are cached between restarts.
    #[arg(long, env = "AUGER_CATALOG_CACHE")]
    catalog_cache: Option<PathBuf>,

    /// Print the discovered catalog and exit without starting the server.
    #[arg(long)]
    describe: bool,

    #[arg(long, env = "AUGER_LOG", default_value = "info")]
    log: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_tracing(&cli.log);

    let config = Arc::new(build_config(&cli)?);
    tracing::info!(
        mongo = %mongo::client::redact(&config.mongo.uri),
        "starting auger {}",
        env!("CARGO_PKG_VERSION")
    );

    let conn = MongoConnection::connect(config.mongo.clone()).await?;
    let store = Arc::new(CatalogStore::new(config.catalog.cache_path.clone()));
    let mongo_catalog = Arc::new(
        MongoCatalog::new(conn.clone(), Arc::clone(&store), Arc::clone(&config)).await?,
    );

    if cli.describe {
        return describe(&conn, &mongo_catalog, &config).await;
    }

    let ctx = build_session(mongo_catalog, &config).await?;
    server::serve(Arc::new(ctx), config).await
}

/// Merge the configuration file with command-line overrides.
fn build_config(cli: &Cli) -> anyhow::Result<Config> {
    let mut config = Config::load(cli.config.as_deref())?;
    if let Some(uri) = &cli.mongo_uri {
        config.mongo.uri = uri.clone();
    }
    if let Some(listen) = &cli.listen {
        config.server.listen = listen.clone();
    }
    if let Some(n) = cli.sample_size {
        config.catalog.sample_size = n;
    }
    if let Some(path) = &cli.catalog_cache {
        config.catalog.cache_path = Some(path.clone());
    }
    Ok(config)
}

async fn build_session(
    mongo_catalog: Arc<MongoCatalog>,
    config: &Arc<Config>,
) -> anyhow::Result<SessionContext> {
    // Unqualified table names resolve against the first Mongo database, so
    // `SELECT * FROM orders` works the way it would against a real database.
    let default_schema = mongo_catalog
        .databases()
        .first()
        .cloned()
        .unwrap_or_else(|| "public".to_string());

    // The compatibility views and any user-defined views need somewhere to
    // live, and `public` has to exist because clients assume it does.
    for name in ["pg_catalog", "public"] {
        mongo_catalog
            .register_schema(name, Arc::new(MemorySchemaProvider::new()))
            .map_err(|e| anyhow::anyhow!("registering {name}: {e}"))?;
    }

    let mut session_config = SessionConfig::new()
        .with_information_schema(true)
        .with_default_catalog_and_schema(CATALOG, &default_schema)
        .with_create_default_catalog_and_schema(false)
        .with_batch_size(config.server.batch_size);

    // MongoDB field names are case-sensitive and overwhelmingly camelCase.
    // SQL's default of folding unquoted identifiers to lower case would make
    // `SELECT createdAt` fail against a field that really is `createdAt`, and
    // forcing users to double-quote every column is not a usable gateway.
    session_config.options_mut().sql_parser.enable_ident_normalization = false;

    let ctx = SessionContext::new_with_config(session_config);
    ctx.register_catalog(CATALOG, mongo_catalog);
    server::compat::register(&ctx, CATALOG, &default_schema).await?;

    Ok(ctx)
}

/// `--describe`: report what the gateway can see, then exit.
///
/// Worth having as a first-run check — it separates "cannot reach Mongo" and
/// "the collection is invisible" from "the SQL is wrong", which otherwise all
/// look the same from a client.
async fn describe(
    conn: &MongoConnection,
    catalog: &Arc<MongoCatalog>,
    config: &Arc<Config>,
) -> anyhow::Result<()> {
    for db in catalog.databases() {
        let collections = conn.collections(db).await?;
        println!("schema {db} ({} tables)", collections.len());
        for name in collections {
            let schema = conn.infer_schema(db, &name, &config.catalog).await?;
            let stats = conn.stats(db, &name).await;
            println!("  {name}  ({} rows)", stats.doc_count);
            for field in schema.fields() {
                let tag = catalog::infer::BsonTag::of(field).as_str();
                let flag = if catalog::infer::field_is_mixed(field) { " [mixed]" } else { "" };
                println!(
                    "    {:<28} {:<28} bson={tag}{flag}",
                    field.name(),
                    field.data_type().to_string(),
                );
            }
        }
    }
    Ok(())
}

fn init_tracing(level: &str) {
    use tracing_subscriber::{EnvFilter, fmt};
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("auger={level},warn")));
    fmt().with_env_filter(filter).with_target(false).init();
}
