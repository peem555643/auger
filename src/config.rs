//! Runtime configuration, loaded from a single TOML file.
//!
//! Everything has a working default, so `auger --mongo-uri mongodb://localhost`
//! is enough to get a server up; the file exists for tuning.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub server: ServerConfig,
    pub mongo: MongoConfig,
    pub catalog: CatalogConfig,
    pub pushdown: PushdownConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    /// Address the PostgreSQL wire listener binds to.
    pub listen: String,
    /// Address the read-only HTTP status/catalog UI binds to. `None` — the
    /// default — starts no HTTP listener at all: the UI exposes collection and
    /// field names, so it is opt-in rather than something a deployment grows
    /// without deciding to.
    pub http_listen: Option<String>,
    /// Let the HTTP UI *execute* read-only SQL (a query console), not just
    /// browse the catalog. Off even when the UI is up: the UI has no auth of
    /// its own, so running arbitrary queries against the data is a separate,
    /// deliberate choice. Only `SELECT`/`WITH`/`EXPLAIN`/`SHOW`/`DESCRIBE` are
    /// accepted, results are row-capped, and `statement_timeout_secs` applies.
    pub http_query: bool,
    /// `trust` accepts any user; `md5` and `scram` check `users`.
    pub auth: AuthMode,
    /// user -> cleartext password, consulted by the `md5`/`scram` auth modes.
    pub users: std::collections::HashMap<String, String>,
    /// Rows per Arrow batch handed to the wire encoder.
    pub batch_size: usize,
    /// Reject any query still running after this many seconds. 0 disables.
    pub statement_timeout_secs: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:5433".into(),
            http_listen: None,
            http_query: false,
            auth: AuthMode::Trust,
            users: Default::default(),
            batch_size: 8192,
            statement_timeout_secs: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthMode {
    Trust,
    Md5,
    Scram,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct MongoConfig {
    pub uri: String,
    /// Databases to expose as SQL schemas. Empty means "every non-system database".
    pub databases: Vec<String>,
    /// Collections matching these glob-ish prefixes are hidden.
    pub exclude_collections: Vec<String>,
    pub connect_timeout_secs: u64,
    /// `$maxTimeMS` applied to every server-side operation. 0 disables.
    pub server_timeout_secs: u64,
    /// Passed to the driver as the cursor batch size.
    pub cursor_batch_size: u32,
    /// Allow the server to spill aggregation stages to disk.
    pub allow_disk_use: bool,
}

impl Default for MongoConfig {
    fn default() -> Self {
        Self {
            uri: "mongodb://localhost:27017".into(),
            databases: Vec::new(),
            exclude_collections: Vec::new(),
            connect_timeout_secs: 10,
            server_timeout_secs: 0,
            cursor_batch_size: 4096,
            allow_disk_use: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CatalogConfig {
    /// How many documents to sample when inferring a collection's schema.
    pub sample_size: i64,
    /// Take this fraction of the sample from the newest documents (by `_id`)
    /// rather than uniformly at random. Newly-added fields show up in recent
    /// documents first, and a pure `$sample` tends to miss them.
    pub recent_bias: f64,
    /// Maximum nesting depth materialised as Arrow structs. Deeper subtrees
    /// become a single JSON `Utf8` column instead of exploding the schema.
    pub max_depth: usize,
    /// A field seen in fewer than this fraction of sampled documents is still
    /// exposed, but is never trusted for `Exact` filter pushdown.
    pub rare_field_threshold: f64,
    /// Where the inferred schemas are persisted so restarts are instant and,
    /// more importantly, so a collection's schema does not drift between queries.
    pub cache_path: Option<PathBuf>,
    /// Re-infer a collection's schema after this long. 0 means "never re-infer
    /// automatically"; `CALL auger_refresh(...)` always works.
    pub refresh_interval_secs: u64,
    /// Hand-written schema overrides: `[catalog.overrides."mydb.mycoll"]`.
    pub overrides: std::collections::HashMap<String, Vec<ColumnOverride>>,
}

impl Default for CatalogConfig {
    fn default() -> Self {
        Self {
            sample_size: 1000,
            recent_bias: 0.25,
            max_depth: 4,
            rare_field_threshold: 0.01,
            cache_path: None,
            refresh_interval_secs: 3600,
            overrides: Default::default(),
        }
    }
}

/// A user-supplied column definition that wins over whatever sampling inferred.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ColumnOverride {
    /// Dotted path into the document, e.g. `payload.amount`.
    pub path: String,
    /// SQL type name, parsed by DataFusion's type parser (`BIGINT`, `TEXT`, ...).
    pub sql_type: String,
    #[serde(default = "default_true")]
    pub nullable: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct PushdownConfig {
    /// Translate `WHERE` into `$match`.
    pub filters: bool,
    /// Translate `GROUP BY` + aggregates into `$group`.
    pub aggregates: bool,
    /// Translate `ORDER BY` into `$sort`.
    pub sorts: bool,
    /// Translate `LIMIT`/`OFFSET` into `$limit`/`$skip`.
    pub limits: bool,
    /// Split a single collection scan across N cursors using `_id` ranges.
    /// 0 means "pick automatically from collection size and CPU count".
    pub scan_parallelism: usize,
    /// Do not split scans for collections smaller than this many documents.
    pub parallel_scan_min_docs: u64,
    /// Emit the generated aggregation pipeline into `EXPLAIN` output.
    pub explain_pipeline: bool,
}

impl Default for PushdownConfig {
    fn default() -> Self {
        Self {
            filters: true,
            aggregates: true,
            sorts: true,
            limits: true,
            scan_parallelism: 0,
            parallel_scan_min_docs: 100_000,
            explain_pipeline: true,
        }
    }
}

impl Config {
    pub fn load(path: Option<&std::path::Path>) -> anyhow::Result<Self> {
        match path {
            Some(p) => {
                let raw = std::fs::read_to_string(p)
                    .map_err(|e| anyhow::anyhow!("reading config {}: {e}", p.display()))?;
                Ok(toml::from_str(&raw)?)
            }
            None => Ok(Self::default()),
        }
    }

    /// Effective scan parallelism for a collection of `doc_count` documents.
    pub fn effective_parallelism(&self, doc_count: u64) -> usize {
        if doc_count < self.pushdown.parallel_scan_min_docs {
            return 1;
        }
        match self.pushdown.scan_parallelism {
            0 => std::thread::available_parallelism()
                .map(|n| n.get().min(8))
                .unwrap_or(1),
            n => n.max(1),
        }
    }
}
