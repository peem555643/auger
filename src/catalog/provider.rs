//! DataFusion catalog wiring: one SQL catalog holding one schema per MongoDB
//! database, each listing its collections as tables.
//!
//! Schemas are inferred lazily on first reference and then served from the
//! persistent [`CatalogStore`], so the cost of sampling is paid once and a
//! table's shape does not change between two queries in the same session.

use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::error::{DataFusionError, Result};

use crate::catalog::store::CatalogStore;
use crate::config::Config;
use crate::mongo::client::MongoConnection;
use crate::mongo::provider::MongoTableProvider;

/// One SQL catalog over an entire MongoDB deployment.
///
/// Alongside the Mongo databases it holds ordinary in-memory schemas, so
/// `pg_catalog` and `public` live in the same catalog as the data. Keeping them
/// together is what lets a client write `pg_catalog.pg_class` and
/// `mydb.orders` in the same session without three-part names.
#[derive(Debug)]
pub struct MongoCatalog {
    databases: Vec<String>,
    /// Non-Mongo schemas registered on top, keyed by name.
    extra: std::sync::RwLock<std::collections::HashMap<String, Arc<dyn SchemaProvider>>>,
    conn: MongoConnection,
    store: Arc<CatalogStore>,
    config: Arc<Config>,
}

impl MongoCatalog {
    pub async fn new(
        conn: MongoConnection,
        store: Arc<CatalogStore>,
        config: Arc<Config>,
    ) -> anyhow::Result<Self> {
        let databases = conn.databases().await?;
        tracing::info!(count = databases.len(), "discovered databases");
        Ok(Self { databases, extra: Default::default(), conn, store, config })
    }

    pub fn databases(&self) -> &[String] {
        &self.databases
    }
}

impl CatalogProvider for MongoCatalog {
    fn schema_names(&self) -> Vec<String> {
        let mut names = self.databases.clone();
        if let Ok(extra) = self.extra.read() {
            names.extend(extra.keys().cloned());
        }
        names
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        // Registered schemas win, so a Mongo database called `pg_catalog`
        // cannot shadow the compatibility views.
        if let Some(found) = self.extra.read().ok().and_then(|e| e.get(name).cloned()) {
            return Some(found);
        }
        self.databases.iter().any(|d| d == name).then(|| {
            Arc::new(MongoSchema {
                database: name.to_string(),
                conn: self.conn.clone(),
                store: Arc::clone(&self.store),
                config: Arc::clone(&self.config),
                collections: std::sync::RwLock::new(None),
            }) as Arc<dyn SchemaProvider>
        })
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        let mut extra = self
            .extra
            .write()
            .map_err(|_| DataFusionError::Internal("catalog lock poisoned".into()))?;
        Ok(extra.insert(name.to_string(), schema))
    }
}

/// One SQL schema over a single MongoDB database.
#[derive(Debug)]
pub struct MongoSchema {
    database: String,
    conn: MongoConnection,
    store: Arc<CatalogStore>,
    config: Arc<Config>,
    /// Collection listing, cached after the first successful lookup.
    collections: std::sync::RwLock<Option<Vec<String>>>,
}

impl MongoSchema {
    /// Cached collection listing.
    ///
    /// `SchemaProvider::table_names` is synchronous, so the listing has to be
    /// available without awaiting. It is refreshed by [`Self::refresh`], which
    /// every async entry point calls before it needs the list.
    fn cached_names(&self) -> Vec<String> {
        self.collections
            .read()
            .ok()
            .and_then(|g| g.clone())
            .unwrap_or_else(|| self.store.known_tables(&self.database))
    }

    async fn refresh(&self) -> Vec<String> {
        match self.conn.collections(&self.database).await {
            Ok(names) => {
                if let Ok(mut guard) = self.collections.write() {
                    *guard = Some(names.clone());
                }
                names
            }
            Err(e) => {
                tracing::warn!(db = %self.database, error = %e, "could not list collections");
                self.cached_names()
            }
        }
    }
}

#[async_trait::async_trait]
impl SchemaProvider for MongoSchema {
    fn table_names(&self) -> Vec<String> {
        self.cached_names()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.cached_names().iter().any(|n| n == name)
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        // Refresh on a miss so a collection created after startup resolves
        // without restarting the gateway.
        if !self.table_exist(name) {
            let names = self.refresh().await;
            if !names.iter().any(|n| n == name) {
                return Ok(None);
            }
        }

        let key = CatalogStore::key(&self.database, name);
        let ttl = self.config.catalog.refresh_interval_secs;

        let entry = match self.store.get_fresh(&key, ttl) {
            Some(cached) => cached,
            None => {
                let schema = self
                    .conn
                    .infer_schema(&self.database, name, &self.config.catalog)
                    .await
                    .map_err(|e| {
                        DataFusionError::External(
                            format!("inferring schema for {key}: {e}").into(),
                        )
                    })?;
                let stats = self.conn.stats(&self.database, name).await;
                self.store.put(key.clone(), schema, stats)
            }
        };

        Ok(Some(Arc::new(MongoTableProvider::new(
            self.conn.clone(),
            self.database.clone(),
            name.to_string(),
            entry.schema,
            entry.stats,
            Arc::clone(&self.config),
        ))))
    }
}
