use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::datasource::{TableProviderFilterPushDown as FPD};
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::{
    datasource::{datasource::Statistics, TableProvider},
    error::DataFusionError,
    physical_plan::{ExecutionPlan, SendableRecordBatchStream},
};
use futures::Stream;
use mongodb_arrow_connector::reader::ReaderConfig;

pub struct MongoSource {
    config: ReaderConfig,
    schema: SchemaRef,
}

impl TableProvider for MongoSource {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        filters: &[datafusion::logical_plan::Expr],
        // limit can be used to reduce the amount scanned
        // from the datasource as a performance optimization.
        // If set, it contains the amount of rows needed by the `LogicalPlan`,
        // The datasource should return *at least* this number of rows if available.
        limit: Option<usize>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn ExecutionPlan>> {
        dbg!(filters);
        Ok(Arc::new(MongoExec {
            partitions: vec![MongoPartition {
                config: self.config.clone(),
                filter: None,
            }],
            schema: self.schema(),
            projection: projection.clone().unwrap(),
            batch_size,
            statistics: Statistics::default(),
            limit,
        }))
    }

    fn supports_filter_pushdown(&self, filter: &Expr) -> datafusion::error::Result<FPD> {
        dbg!(&filter);
        Ok(match filter {
            Expr::Alias(_, _) => FPD::Unsupported,
            Expr::Column(_) => FPD::Unsupported,
            Expr::ScalarVariable(_) => FPD::Unsupported,
            Expr::Literal(_) => FPD::Unsupported,
            Expr::BinaryExpr { left, op, right } => FPD::Unsupported,
            Expr::Not(_) => FPD::Unsupported,
            Expr::IsNotNull(_) => FPD::Unsupported,
            Expr::IsNull(_) => FPD::Unsupported,
            Expr::Negative(_) => FPD::Unsupported,
            Expr::Between { expr, negated, low, high } => FPD::Unsupported,
            Expr::Case { expr, when_then_expr, else_expr } => FPD::Unsupported,
            Expr::Cast { expr, data_type } => FPD::Unsupported,
            Expr::TryCast { expr, data_type } => FPD::Unsupported,
            Expr::Sort { expr, asc, nulls_first } => FPD::Unsupported,
            Expr::ScalarFunction { fun, args } => FPD::Unsupported,
            Expr::ScalarUDF { fun, args } => FPD::Unsupported,
            Expr::AggregateFunction { fun, args, distinct } => FPD::Unsupported,
            Expr::AggregateUDF { fun, args } => FPD::Unsupported,
            Expr::InList { expr, list, negated } => FPD::Unsupported,
            Expr::Wildcard => FPD::Unsupported,
        })
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

#[derive(Debug, Clone)]
pub struct MongoExec {
    /// Mongo partitions to read
    partitions: Vec<MongoPartition>,
    /// Schema after projection is applied
    schema: SchemaRef,
    /// Projection for which columns to load
    projection: Vec<usize>,
    /// Batch size
    batch_size: usize,
    /// Statistics for the data set (sum of statistics for all partitions)
    statistics: Statistics,
    /// Optional limit of the number of rows
    limit: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct MongoPartition {
    config: ReaderConfig,
    filter: Option<()>,
}

#[async_trait]
impl ExecutionPlan for MongoExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        todo!()
    }

    fn children(&self) -> Vec<std::sync::Arc<dyn ExecutionPlan>> {
        // No children as this is a root node
        vec![]
    }

    fn with_new_children(
        &self,
        children: Vec<std::sync::Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(Arc::new(self.clone()))
        } else {
            Err(DataFusionError::Internal(format!(
                "Children cannot be replaced in {:?}",
                self
            )))
        }
    }

    async fn execute(
        &self,
        partition: usize,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let partition = self.partitions.get(partition).unwrap();
        // is the reader schema being passed the projected one? Might be incorrect
        let reader = mongodb_arrow_connector::reader::Reader::try_new(
            &partition.config,
            self.schema().clone(),
        )
        .unwrap();
        Ok(Box::pin(MongoStream { reader }))
    }
}

struct MongoStream {
    reader: mongodb_arrow_connector::reader::Reader,
}

impl Stream for MongoStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.reader.next())
    }
}

impl RecordBatchStream for MongoStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }
}
