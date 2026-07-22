//! Persistent schema catalog.
//!
//! Inference is expensive and, more importantly, *non-deterministic*: two
//! samples of the same collection can disagree. Freezing the result on disk
//! means a restart does not silently change a column's type underneath a
//! dashboard, and it means planning does not pay for sampling on every query.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use serde::{Deserialize, Serialize};

/// A cached schema plus the statistics the planner uses for costing.
#[derive(Debug, Clone)]
pub struct CachedTable {
    pub schema: SchemaRef,
    pub stats: CollectionStats,
    /// Unix seconds when this entry was inferred.
    pub inferred_at: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CollectionStats {
    pub doc_count: u64,
    pub avg_doc_size: u64,
    pub total_size: u64,
    /// Field paths that carry an index, used to prefer index-friendly plans.
    pub indexed_paths: Vec<String>,
}

// ---------------------------------------------------------------------------
// On-disk representation
// ---------------------------------------------------------------------------
//
// Arrow's own schema serde does not round-trip field metadata through JSON in a
// stable way across versions, so the catalog uses its own explicit form. It is
// small, human-readable and diffable, which matters when an operator wants to
// see why a column came out as text.

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredCatalog {
    version: u32,
    tables: HashMap<String, StoredTable>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredTable {
    fields: Vec<StoredField>,
    stats: CollectionStats,
    inferred_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredField {
    name: String,
    /// Arrow `DataType` rendered by `Debug`-free explicit encoding.
    ty: StoredType,
    nullable: bool,
    #[serde(default)]
    metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "k", rename_all = "snake_case")]
enum StoredType {
    Boolean,
    Int32,
    Int64,
    Float64,
    Utf8,
    Binary,
    Decimal { precision: u8, scale: i8 },
    TimestampMs { tz: Option<String> },
    Struct { fields: Vec<StoredField> },
    List { item: Box<StoredField> },
}

const CATALOG_VERSION: u32 = 1;

impl StoredType {
    fn from_arrow(dt: &DataType) -> Option<Self> {
        use datafusion::arrow::datatypes::TimeUnit;
        Some(match dt {
            DataType::Boolean => Self::Boolean,
            DataType::Int32 => Self::Int32,
            DataType::Int64 => Self::Int64,
            DataType::Float64 => Self::Float64,
            DataType::Utf8 => Self::Utf8,
            DataType::Binary => Self::Binary,
            DataType::Decimal128(p, s) => Self::Decimal { precision: *p, scale: *s },
            DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                Self::TimestampMs { tz: tz.as_ref().map(|t| t.to_string()) }
            }
            DataType::Struct(fields) => Self::Struct {
                fields: fields.iter().map(|f| StoredField::from_arrow(f)).collect::<Option<_>>()?,
            },
            DataType::List(item) => Self::List { item: Box::new(StoredField::from_arrow(item)?) },
            _ => return None,
        })
    }

    fn to_arrow(&self) -> DataType {
        use datafusion::arrow::datatypes::{Fields, TimeUnit};
        match self {
            Self::Boolean => DataType::Boolean,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
            Self::Float64 => DataType::Float64,
            Self::Utf8 => DataType::Utf8,
            Self::Binary => DataType::Binary,
            Self::Decimal { precision, scale } => DataType::Decimal128(*precision, *scale),
            Self::TimestampMs { tz } => {
                DataType::Timestamp(TimeUnit::Millisecond, tz.as_ref().map(|t| t.as_str().into()))
            }
            Self::Struct { fields } => {
                DataType::Struct(Fields::from(
                    fields.iter().map(|f| f.to_arrow()).collect::<Vec<_>>(),
                ))
            }
            Self::List { item } => DataType::List(Arc::new(item.to_arrow())),
        }
    }
}

impl StoredField {
    fn from_arrow(f: &Field) -> Option<Self> {
        Some(Self {
            name: f.name().clone(),
            ty: StoredType::from_arrow(f.data_type())?,
            nullable: f.is_nullable(),
            metadata: f.metadata().clone(),
        })
    }

    fn to_arrow(&self) -> Field {
        Field::new(&self.name, self.ty.to_arrow(), self.nullable)
            .with_metadata(self.metadata.clone())
    }
}

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

/// In-memory catalog with optional write-through to a JSON file.
///
/// A plain `std::sync::RwLock` is enough: every critical section is a map
/// lookup plus an `Arc` clone, and readers vastly outnumber writers.
#[derive(Debug)]
pub struct CatalogStore {
    path: Option<PathBuf>,
    tables: std::sync::RwLock<HashMap<String, CachedTable>>,
}

impl CatalogStore {
    pub fn new(path: Option<PathBuf>) -> Self {
        let store = Self { path, tables: Default::default() };
        if let Some(p) = store.path.as_deref() {
            if let Err(e) = store.load_from(p) {
                tracing::warn!(path = %p.display(), error = %e, "ignoring unreadable schema cache");
            }
        }
        store
    }

    /// `db.collection` is the catalog key; it matches the SQL name a user types.
    pub fn key(db: &str, collection: &str) -> String {
        format!("{db}.{collection}")
    }

    pub fn get(&self, key: &str) -> Option<CachedTable> {
        self.tables.read().ok()?.get(key).cloned()
    }

    /// Returns the entry only if it was inferred within `max_age_secs`.
    /// `max_age_secs == 0` means entries never expire.
    pub fn get_fresh(&self, key: &str, max_age_secs: u64) -> Option<CachedTable> {
        let entry = self.get(key)?;
        if max_age_secs == 0 {
            return Some(entry);
        }
        let age = now_secs().saturating_sub(entry.inferred_at);
        (age <= max_age_secs).then_some(entry)
    }

    pub fn put(&self, key: String, schema: SchemaRef, stats: CollectionStats) -> CachedTable {
        let entry = CachedTable { schema, stats, inferred_at: now_secs() };
        if let Ok(mut guard) = self.tables.write() {
            guard.insert(key, entry.clone());
        }
        self.persist();
        entry
    }

    pub fn known_tables(&self, db: &str) -> Vec<String> {
        let prefix = format!("{db}.");
        self.tables
            .read()
            .map(|g| {
                g.keys()
                    .filter_map(|k| k.strip_prefix(&prefix).map(str::to_string))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn load_from(&self, path: &Path) -> anyhow::Result<()> {
        if !path.exists() {
            return Ok(());
        }
        let raw = std::fs::read_to_string(path)?;
        let stored: StoredCatalog = serde_json::from_str(&raw)?;
        if stored.version != CATALOG_VERSION {
            tracing::info!(
                found = stored.version,
                expected = CATALOG_VERSION,
                "schema cache version mismatch; re-inferring"
            );
            return Ok(());
        }
        let mut guard = self
            .tables
            .write()
            .map_err(|_| anyhow::anyhow!("catalog lock poisoned"))?;
        for (k, t) in stored.tables {
            let fields: Vec<Field> = t.fields.iter().map(|f| f.to_arrow()).collect();
            guard.insert(
                k,
                CachedTable {
                    schema: Arc::new(Schema::new(fields)),
                    stats: t.stats,
                    inferred_at: t.inferred_at,
                },
            );
        }
        Ok(())
    }

    fn persist(&self) {
        let Some(path) = self.path.as_deref() else { return };
        let Ok(guard) = self.tables.read() else { return };

        let mut tables = HashMap::with_capacity(guard.len());
        for (k, v) in guard.iter() {
            let fields: Option<Vec<StoredField>> =
                v.schema.fields().iter().map(|f| StoredField::from_arrow(f)).collect();
            let Some(fields) = fields else {
                tracing::debug!(table = %k, "schema not representable in the cache format; skipping");
                continue;
            };
            tables.insert(
                k.clone(),
                StoredTable { fields, stats: v.stats.clone(), inferred_at: v.inferred_at },
            );
        }
        drop(guard);

        let stored = StoredCatalog { version: CATALOG_VERSION, tables };
        let Ok(json) = serde_json::to_string_pretty(&stored) else { return };

        // Write to a sibling temp file and rename, so a crash mid-write cannot
        // leave a truncated catalog behind.
        let tmp = path.with_extension("json.tmp");
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Err(e) = std::fs::write(&tmp, json).and_then(|_| std::fs::rename(&tmp, path)) {
            tracing::warn!(path = %path.display(), error = %e, "could not persist schema cache");
        }
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::infer::{META_BSON, META_PATH};

    fn sample_schema() -> SchemaRef {
        let mut meta = HashMap::new();
        meta.insert(META_BSON.to_string(), "objectId".to_string());
        meta.insert(META_PATH.to_string(), "_id".to_string());
        Arc::new(Schema::new(vec![
            Field::new("_id", DataType::Utf8, false).with_metadata(meta),
            Field::new(
                "profile",
                DataType::Struct(
                    vec![Field::new("age", DataType::Int32, true)].into(),
                ),
                true,
            ),
        ]))
    }

    #[test]
    fn round_trips_through_disk_preserving_metadata() {
        let dir = std::env::temp_dir().join(format!("auger-test-{}", std::process::id()));
        let path = dir.join("catalog.json");
        let _ = std::fs::remove_file(&path);

        let store = CatalogStore::new(Some(path.clone()));
        store.put("db.users".into(), sample_schema(), CollectionStats { doc_count: 42, ..Default::default() });

        let reloaded = CatalogStore::new(Some(path.clone()));
        let entry = reloaded.get("db.users").expect("entry should survive a restart");
        assert_eq!(entry.stats.doc_count, 42);

        let id = entry.schema.field_with_name("_id").unwrap();
        assert_eq!(id.metadata().get(META_BSON).map(String::as_str), Some("objectId"));
        assert!(matches!(
            entry.schema.field_with_name("profile").unwrap().data_type(),
            DataType::Struct(_)
        ));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn expiry_respects_max_age() {
        let store = CatalogStore::new(None);
        store.put("db.c".into(), sample_schema(), Default::default());
        assert!(store.get_fresh("db.c", 0).is_some(), "0 means never expire");
        assert!(store.get_fresh("db.c", 3600).is_some());
    }

    #[test]
    fn known_tables_filters_by_database() {
        let store = CatalogStore::new(None);
        store.put("a.one".into(), sample_schema(), Default::default());
        store.put("b.two".into(), sample_schema(), Default::default());
        assert_eq!(store.known_tables("a"), vec!["one".to_string()]);
    }
}
