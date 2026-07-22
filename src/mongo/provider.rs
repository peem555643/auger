//! `TableProvider` for a single MongoDB collection.
//!
//! This is where DataFusion's planner meets Mongo's query language. Three
//! things get pushed to the server here — the predicate, the set of fields, and
//! the row limit — and the scan is split into `_id` ranges so a large
//! collection is read by several cursors at once.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Statistics, stats::Precision};
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;

use crate::catalog::infer::field_path;
use crate::catalog::store::CollectionStats;
use crate::config::Config;
use crate::mongo::client::MongoConnection;
use crate::mongo::convert::project_schema;
use crate::mongo::exec::MongoExec;
use crate::mongo::pipeline::{MongoPlan, partition_bounds};
use crate::mongo::pushdown;

#[derive(Debug)]
pub struct MongoTableProvider {
    conn: MongoConnection,
    database: String,
    collection: String,
    schema: SchemaRef,
    stats: CollectionStats,
    config: Arc<Config>,
}

impl MongoTableProvider {
    pub fn new(
        conn: MongoConnection,
        database: String,
        collection: String,
        schema: SchemaRef,
        stats: CollectionStats,
        config: Arc<Config>,
    ) -> Self {
        Self { conn, database, collection, schema, stats, config }
    }

    /// Fraction of rows a pushed-down filter is assumed to keep.
    ///
    /// Without per-field histograms any number here is a guess; what matters
    /// for plan quality is that a filtered scan costs visibly less than an
    /// unfiltered one, so joins order sensibly.
    fn estimate_rows(&self, filtered: bool, limit: Option<usize>) -> usize {
        let base = self.stats.doc_count as usize;
        let after_filter = if filtered { (base / 5).max(1) } else { base };
        match limit {
            Some(l) => after_filter.min(l),
            None => after_filter,
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for MongoTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| match pushdown::translate(f, &self.schema) {
                // `Exact` lets DataFusion drop its own copy of the predicate.
                // Only claimed when the `$match` accepts precisely the SQL rows.
                Some(t) if t.exact => TableProviderFilterPushDown::Exact,
                // Still sent to the server — the server does the bulk of the
                // work against its indexes — but re-checked locally.
                Some(_) => TableProviderFilterPushDown::Inexact,
                None => TableProviderFilterPushDown::Unsupported,
            })
            .collect())
    }

    fn statistics(&self) -> Option<Statistics> {
        let mut s = Statistics::new_unknown(&self.schema);
        s.num_rows = Precision::Inexact(self.stats.doc_count as usize);
        if self.stats.total_size > 0 {
            s.total_byte_size = Precision::Inexact(self.stats.total_size as usize);
        }
        Some(s)
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected = project_schema(&self.schema, projection)?;

        // --- predicate -----------------------------------------------------
        let translated: Vec<_> = if self.config.pushdown.filters {
            filters
                .iter()
                .filter_map(|f| pushdown::translate(f, &self.schema))
                .collect()
        } else {
            Vec::new()
        };
        let filter = pushdown::combine(&translated);
        if translated.len() < filters.len() {
            tracing::debug!(
                table = %format!("{}.{}", self.database, self.collection),
                pushed = translated.len(),
                total = filters.len(),
                "some predicates could not be expressed as $match"
            );
        }

        // --- projection ----------------------------------------------------
        // Columns read from a nested path project the whole path, so the server
        // sends only the requested subtree rather than the entire document.
        let paths: Vec<String> = if projection.is_some() {
            projected.fields().iter().map(|f| field_path(f)).collect()
        } else {
            Vec::new()
        };

        // --- parallelism ---------------------------------------------------
        // Splitting is only worthwhile on a plain scan: with a `LIMIT` a single
        // cursor usually finishes before extra ones have connected.
        let want_partitions = if limit.is_some() {
            1
        } else {
            self.config.effective_parallelism(self.stats.doc_count)
        };

        let partitions: Vec<Option<bson::Document>> = if want_partitions > 1 {
            match self.conn.sample_ids(&self.database, &self.collection, want_partitions).await {
                Ok(ids) => partition_bounds(&ids, want_partitions),
                Err(e) => {
                    // Falling back to one cursor is always correct; a failed
                    // sample must not fail the query.
                    tracing::debug!(error = %e, "could not sample _id bounds; scanning serially");
                    vec![None]
                }
            }
        } else {
            vec![None]
        };

        let limit_i64 = limit.and_then(|l| i64::try_from(l).ok());
        let plans: Vec<MongoPlan> = partitions
            .into_iter()
            .map(|partition| MongoPlan {
                filter: filter.clone(),
                partition,
                projection: paths.clone(),
                sort: Vec::new(),
                skip: None,
                // A per-partition limit is a bound, not the final answer: the
                // limit operator above still trims the merged stream.
                limit: self.config.pushdown.limits.then_some(limit_i64).flatten(),
            })
            .collect();

        let estimated = self.estimate_rows(filter.is_some(), limit);

        Ok(Arc::new(MongoExec::new(
            self.conn.clone(),
            self.database.clone(),
            self.collection.clone(),
            projected,
            plans,
            self.config.server.batch_size,
            self.stats.clone(),
            estimated,
            self.config.pushdown.explain_pipeline,
        )))
    }
}
