//! The MongoDB side of the gateway: connection setup, schema sampling,
//! statistics collection and `_id` range discovery for parallel scans.

use std::sync::Arc;
use std::time::Duration;

use bson::{Bson, Document, doc};
use datafusion::arrow::datatypes::SchemaRef;
use futures::TryStreamExt;
use mongodb::options::ClientOptions;
use mongodb::{Client, Collection};

use crate::catalog::infer::Sampler;
use crate::catalog::store::CollectionStats;
use crate::config::{CatalogConfig, MongoConfig};

/// Collections MongoDB manages itself; exposing them as SQL tables is noise.
const SYSTEM_DATABASES: [&str; 3] = ["admin", "local", "config"];

#[derive(Debug, Clone)]
pub struct MongoConnection {
    client: Client,
    cfg: Arc<MongoConfig>,
}

impl MongoConnection {
    pub async fn connect(cfg: MongoConfig) -> anyhow::Result<Self> {
        let mut options = ClientOptions::parse(&cfg.uri).await?;
        options.connect_timeout = Some(Duration::from_secs(cfg.connect_timeout_secs));
        options.app_name = Some("auger".to_string());
        let client = Client::with_options(options)?;

        // `Client::with_options` is lazy, so fail fast here instead of at the
        // first query — a bad URI should not look like a broken table.
        client
            .database("admin")
            .run_command(doc! { "ping": 1 })
            .await
            .map_err(|e| anyhow::anyhow!("cannot reach MongoDB at {}: {e}", redact(&cfg.uri)))?;

        Ok(Self { client, cfg: Arc::new(cfg) })
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn config(&self) -> &MongoConfig {
        &self.cfg
    }

    fn collection(&self, db: &str, name: &str) -> Collection<Document> {
        self.client.database(db).collection(name)
    }

    /// Apply the configured server-side time limit, if any.
    fn max_time(&self) -> Option<Duration> {
        (self.cfg.server_timeout_secs > 0)
            .then(|| Duration::from_secs(self.cfg.server_timeout_secs))
    }

    /// Databases exposed as SQL schemas.
    pub async fn databases(&self) -> anyhow::Result<Vec<String>> {
        if !self.cfg.databases.is_empty() {
            return Ok(self.cfg.databases.clone());
        }
        let names = self.client.list_database_names().await?;
        Ok(names
            .into_iter()
            .filter(|n| !SYSTEM_DATABASES.contains(&n.as_str()))
            .collect())
    }

    /// Collections exposed as SQL tables within a database.
    pub async fn collections(&self, db: &str) -> anyhow::Result<Vec<String>> {
        let names = self.client.database(db).list_collection_names().await?;
        Ok(names
            .into_iter()
            .filter(|n| !n.starts_with("system."))
            .filter(|n| !self.cfg.exclude_collections.iter().any(|p| matches_prefix(p, n)))
            .collect())
    }

    /// Sample a collection and infer its Arrow schema.
    ///
    /// The sample deliberately mixes two populations. A pure `$sample` is
    /// uniform over the collection, which sounds right but systematically
    /// misses fields added recently — in a collection of ten million documents
    /// a field present only in the newest thousand has a vanishing chance of
    /// appearing. Taking part of the budget from the `_id` tail fixes that at
    /// the cost of one extra indexed query.
    pub async fn infer_schema(
        &self,
        db: &str,
        collection: &str,
        cfg: &CatalogConfig,
    ) -> anyhow::Result<SchemaRef> {
        let coll = self.collection(db, collection);
        let total = cfg.sample_size.max(1);
        let recent = ((total as f64) * cfg.recent_bias.clamp(0.0, 1.0)) as i64;
        let random = (total - recent).max(0);

        let mut sampler = Sampler::new();

        if recent > 0 {
            let pipeline = vec![doc! { "$sort": { "_id": -1 } }, doc! { "$limit": recent }];
            self.drain_into(&coll, pipeline, &mut sampler).await?;
        }
        if random > 0 {
            let pipeline = vec![doc! { "$sample": { "size": random } }];
            // `$sample` needs a non-empty collection; an error here is not fatal
            // as long as the recency pass found something.
            if let Err(e) = self.drain_into(&coll, pipeline, &mut sampler).await {
                tracing::debug!(db, collection, error = %e, "random sampling pass failed");
            }
        }

        if sampler.is_empty() {
            // An empty collection is a real table with no columns yet. Give it
            // an `_id` so `SELECT * FROM t` returns zero rows instead of failing.
            tracing::info!(db, collection, "collection is empty; using a placeholder schema");
            let mut placeholder = Sampler::new();
            placeholder.observe(&doc! { "_id": bson::oid::ObjectId::new() });
            return Ok(Arc::new(placeholder.finish(cfg.max_depth)));
        }

        let schema = sampler.finish(cfg.max_depth);
        tracing::info!(
            db,
            collection,
            sampled = sampler.docs_seen(),
            columns = schema.fields().len(),
            "inferred schema"
        );
        Ok(Arc::new(schema))
    }

    async fn drain_into(
        &self,
        coll: &Collection<Document>,
        pipeline: Vec<Document>,
        sampler: &mut Sampler,
    ) -> anyhow::Result<()> {
        let mut action = coll.aggregate(pipeline).batch_size(self.cfg.cursor_batch_size);
        if let Some(t) = self.max_time() {
            action = action.max_time(t);
        }
        let mut cursor = action.await?;
        while let Some(doc) = cursor.try_next().await? {
            sampler.observe(&doc);
        }
        Ok(())
    }

    /// Size and index statistics, used for cost estimates and for deciding
    /// whether a scan is worth splitting.
    pub async fn stats(&self, db: &str, collection: &str) -> CollectionStats {
        let mut stats = CollectionStats::default();

        // `$collStats` is cheap (it reads catalog metadata, not documents) but
        // is unavailable on some managed tiers, so treat failure as non-fatal.
        let coll = self.collection(db, collection);
        match coll.aggregate(vec![doc! { "$collStats": { "storageStats": {} } }]).await {
            Ok(mut cursor) => {
                if let Ok(Some(doc)) = cursor.try_next().await {
                    if let Ok(storage) = doc.get_document("storageStats") {
                        stats.doc_count = storage.get_i64("count").ok().unwrap_or_else(|| {
                            storage.get_i32("count").ok().unwrap_or(0) as i64
                        }) as u64;
                        stats.avg_doc_size = read_number(storage, "avgObjSize").unwrap_or(0);
                        stats.total_size = read_number(storage, "size").unwrap_or(0);
                    }
                }
            }
            Err(e) => {
                tracing::debug!(db, collection, error = %e, "$collStats unavailable");
                if let Ok(n) = coll.estimated_document_count().await {
                    stats.doc_count = n;
                }
            }
        }

        if let Ok(mut cursor) = coll.list_indexes().await {
            while let Ok(Some(index)) = cursor.try_next().await {
                stats.indexed_paths.extend(index.keys.keys().cloned());
            }
        }

        stats
    }

    /// Sample `_id` values, sorted, for splitting a scan into ranges.
    ///
    /// Oversampling relative to the partition count keeps the cut points near
    /// the true quantiles, so partitions come out roughly equal in size.
    pub async fn sample_ids(
        &self,
        db: &str,
        collection: &str,
        partitions: usize,
    ) -> anyhow::Result<Vec<Bson>> {
        if partitions <= 1 {
            return Ok(Vec::new());
        }
        let size = (partitions * 64).min(10_000) as i64;
        let pipeline = vec![
            doc! { "$sample": { "size": size } },
            doc! { "$project": { "_id": 1 } },
            doc! { "$sort": { "_id": 1 } },
        ];
        let mut cursor = self.collection(db, collection).aggregate(pipeline).await?;
        let mut ids = Vec::with_capacity(size as usize);
        while let Some(doc) = cursor.try_next().await? {
            if let Some(id) = doc.get("_id") {
                ids.push(id.clone());
            }
        }
        Ok(ids)
    }
}

/// `count`/`size` come back as int32 or int64 depending on magnitude.
fn read_number(doc: &Document, key: &str) -> Option<u64> {
    match doc.get(key)? {
        Bson::Int32(v) => u64::try_from(*v).ok(),
        Bson::Int64(v) => u64::try_from(*v).ok(),
        Bson::Double(v) if *v >= 0.0 => Some(*v as u64),
        _ => None,
    }
}

/// `prefix*` matches by prefix; anything else must match exactly.
fn matches_prefix(pattern: &str, name: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => name.starts_with(prefix),
        None => pattern == name,
    }
}

/// Strip credentials from a connection string before it reaches a log or an
/// error message shown to a client.
pub fn redact(uri: &str) -> String {
    let Some((scheme, rest)) = uri.split_once("://") else {
        return uri.to_string();
    };
    match rest.split_once('@') {
        Some((_creds, host)) => format!("{scheme}://***@{host}"),
        None => uri.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redaction_removes_credentials() {
        assert_eq!(
            redact("mongodb://user:secret@host:27017/db"),
            "mongodb://***@host:27017/db"
        );
        assert_eq!(redact("mongodb://host:27017"), "mongodb://host:27017");
        assert_eq!(redact("not-a-uri"), "not-a-uri");
    }

    #[test]
    fn exclusion_patterns_support_a_trailing_star() {
        assert!(matches_prefix("tmp_*", "tmp_import"));
        assert!(!matches_prefix("tmp_*", "orders"));
        assert!(matches_prefix("orders", "orders"));
        assert!(!matches_prefix("order", "orders"));
    }

    #[test]
    fn numbers_are_read_from_either_int_width() {
        let d = doc! { "a": 5i32, "b": 5i64, "c": 5.0f64, "d": "x" };
        assert_eq!(read_number(&d, "a"), Some(5));
        assert_eq!(read_number(&d, "b"), Some(5));
        assert_eq!(read_number(&d, "c"), Some(5));
        assert_eq!(read_number(&d, "d"), None);
    }
}
