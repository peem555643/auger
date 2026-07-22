//! The physical operator that runs an aggregation pipeline and turns the
//! resulting cursor into a stream of Arrow batches.
//!
//! Documents are converted a batch at a time and the batch is yielded as soon
//! as it is full, so a `LIMIT` upstream stops the scan rather than waiting for
//! the collection to be drained. Each partition owns its own cursor, which is
//! where the parallel `_id` range split pays off.

use std::fmt;
use std::sync::Arc;

use bson::Document;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Statistics, stats::Precision};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, Partitioning,
    SendableRecordBatchStream,
};
use futures::{StreamExt, TryStreamExt};

use crate::catalog::store::CollectionStats;
use crate::mongo::client::MongoConnection;
use crate::mongo::convert::documents_to_batch;
use crate::mongo::pipeline::MongoPlan;

#[derive(Debug, Clone)]
pub struct MongoExec {
    conn: MongoConnection,
    database: String,
    collection: String,
    /// Output schema, already projected.
    schema: SchemaRef,
    /// One plan per output partition.
    plans: Vec<MongoPlan>,
    batch_size: usize,
    stats: CollectionStats,
    /// Row-count estimate after the pushed-down filter, for costing.
    estimated_rows: usize,
    explain_pipeline: bool,
    properties: Arc<PlanProperties>,
}

impl MongoExec {
    pub fn new(
        conn: MongoConnection,
        database: String,
        collection: String,
        schema: SchemaRef,
        plans: Vec<MongoPlan>,
        batch_size: usize,
        stats: CollectionStats,
        estimated_rows: usize,
        explain_pipeline: bool,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(plans.len().max(1)),
            // Batches are emitted as the cursor produces them.
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            conn,
            database,
            collection,
            schema,
            plans,
            batch_size,
            stats,
            estimated_rows,
            explain_pipeline,
            properties,
        }
    }

    fn statistics_of(&self) -> Statistics {
        let mut s = Statistics::new_unknown(&self.schema);
        s.num_rows = Precision::Inexact(self.estimated_rows);
        if self.stats.avg_doc_size > 0 {
            s.total_byte_size =
                Precision::Inexact(self.estimated_rows.saturating_mul(self.stats.avg_doc_size as usize));
        }
        s
    }
}

impl DisplayAs for MongoExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let plan = self.plans.first();
        match t {
            DisplayFormatType::Default => {
                // Name what was pushed to the server. Without this, an EXPLAIN
                // cannot distinguish "the filter ran in Mongo" from "the filter
                // ran here after reading the whole collection", which is the
                // single most important thing to know about one of these plans.
                write!(f, "MongoExec: {}.{}", self.database, self.collection)?;
                write!(f, ", partitions={}", self.plans.len())?;
                if let Some(plan) = plan {
                    let mut pushed: Vec<String> = Vec::new();
                    if plan.filter.is_some() {
                        pushed.push("$match".into());
                    }
                    if !plan.sort.is_empty() {
                        pushed.push("$sort".into());
                    }
                    if plan.limit.is_some() {
                        pushed.push("$limit".into());
                    }
                    if !plan.projection.is_empty() {
                        pushed.push(format!("$project({})", plan.projection.len()));
                    }
                    if !pushed.is_empty() {
                        write!(f, ", pushed=[{}]", pushed.join(", "))?;
                    }
                }
                Ok(())
            }
            DisplayFormatType::Verbose | DisplayFormatType::TreeRender => {
                write!(f, "MongoExec: {}.{}", self.database, self.collection)?;
                write!(f, ", partitions={}", self.plans.len())?;
                if self.explain_pipeline {
                    // Print the pipeline the server actually receives. Being able
                    // to paste this straight into `mongosh` is the difference
                    // between debugging a plan in minutes and in hours.
                    if let Some(plan) = plan {
                        let pipeline: Vec<String> =
                            plan.to_pipeline().iter().map(|s| s.to_string()).collect();
                        write!(f, ", pipeline=[{}]", pipeline.join(", "))?;
                    }
                }
                Ok(())
            }
        }
    }
}

impl ExecutionPlan for MongoExec {
    fn name(&self) -> &str {
        "MongoExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "MongoExec is a leaf and takes no children".into(),
            ))
        }
    }

    fn execute(&self, partition: usize, _ctx: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let plan = self.plans.get(partition).cloned().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "MongoExec has {} partitions but {partition} was requested",
                self.plans.len()
            ))
        })?;

        let conn = self.conn.clone();
        let database = self.database.clone();
        let collection = self.collection.clone();
        let schema = Arc::clone(&self.schema);
        let batch_size = self.batch_size.max(1);

        let output_schema = Arc::clone(&schema);
        let open = async move {
            let cfg = conn.config().clone();
            let coll = conn
                .client()
                .database(&database)
                .collection::<Document>(&collection);

            let mut action = coll
                .aggregate(plan.to_pipeline())
                .batch_size(cfg.cursor_batch_size)
                .allow_disk_use(cfg.allow_disk_use);
            if cfg.server_timeout_secs > 0 {
                action = action.max_time(std::time::Duration::from_secs(cfg.server_timeout_secs));
            }
            let cursor = action.await.map_err(mongo_err)?;

            // Accumulate whole batches so the Arrow builders are exercised once
            // per `batch_size` documents rather than once per document.
            let stream = futures::stream::try_unfold(
                (cursor, schema, batch_size),
                move |(mut cursor, schema, batch_size)| async move {
                    let mut docs: Vec<Document> = Vec::with_capacity(batch_size);
                    while docs.len() < batch_size {
                        match cursor.try_next().await {
                            Ok(Some(doc)) => docs.push(doc),
                            Ok(None) => break,
                            Err(e) => return Err(mongo_err(e)),
                        }
                    }
                    if docs.is_empty() {
                        return Ok(None);
                    }
                    let batch = documents_to_batch(&schema, &docs)?;
                    Ok(Some((batch, (cursor, schema, batch_size))))
                },
            );
            Ok::<_, DataFusionError>(stream)
        };

        // `once(..).try_flatten()` defers opening the cursor until the consumer
        // polls, so a plan that is built and discarded never touches the server.
        let stream = futures::stream::once(open).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(output_schema, stream.boxed())))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        let stats = match partition {
            None => self.statistics_of(),
            Some(_) => {
                // Partitions are `_id` ranges cut at sampled quantiles, so an
                // even split is the honest estimate.
                let mut s = self.statistics_of();
                let n = self.plans.len().max(1);
                s.num_rows = Precision::Inexact(self.estimated_rows / n);
                s
            }
        };
        Ok(Arc::new(stats))
    }
}

fn mongo_err(e: mongodb::error::Error) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}
