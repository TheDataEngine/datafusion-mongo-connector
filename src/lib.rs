use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::datasource::TableProviderFilterPushDown as FPD;
use datafusion::logical_plan::{Expr, Operator};
use datafusion::physical_plan::RecordBatchStream;
use datafusion::scalar::ScalarValue;
use datafusion::{
    datasource::{datasource::Statistics, TableProvider},
    error::{DataFusionError, Result as DFResult},
    physical_plan::{ExecutionPlan, SendableRecordBatchStream},
};
use futures::Stream;
use futures::StreamExt;
use mongodb::bson::*;
use mongodb_arrow_connector::reader::{Reader, ReaderConfig};
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    task,
};
use tokio_stream::wrappers::ReceiverStream;

const BATCH_SIZE: usize = 65536;

pub struct MongoSource {
    pub config: ReaderConfig,
    pub schema: SchemaRef,
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
        filters: &[Expr],
        // limit can be used to reduce the amount scanned
        // from the datasource as a performance optimization.
        // If set, it contains the amount of rows needed by the `LogicalPlan`,
        // The datasource should return *at least* this number of rows if available.
        limit: Option<usize>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn ExecutionPlan>> {
        let read_filters = write_filters(filters);
        let num_records = Reader::estimate_records(
            &self.config,
            read_filters.clone().unwrap_or_default(),
            Some(90000),
        )
        .unwrap();
        let partitions = if let Some(num_records) = num_records {
            let num_partitions = (num_records + BATCH_SIZE - 1) / BATCH_SIZE;
            println!(
                "There are {} partitions with {:?} batch per partition",
                num_partitions, batch_size
            );
            (0..=num_partitions)
                .map(|p| MongoPartition {
                    config: self.config.clone(),
                    filters: read_filters.clone(),
                    limit: Some(BATCH_SIZE),
                    skip: Some(p * BATCH_SIZE),
                })
                .collect()
        } else {
            vec![MongoPartition {
                config: self.config.clone(),
                filters: write_filters(filters),
                limit: None,
                skip: None,
            }]
        };
        Ok(Arc::new(MongoExec {
            partitions,
            schema: if let Some(p) = &projection {
                let fields = p
                    .iter()
                    .map(|index| self.schema.field(*index).clone())
                    .collect();
                Arc::new(Schema::new(fields))
            } else {
                self.schema()
            },
            projection: projection.clone().unwrap(),
            batch_size,
            statistics: Statistics::default(),
            limit,
        }))
    }

    fn supports_filter_pushdown(&self, filter: &Expr) -> datafusion::error::Result<FPD> {
        Ok(match filter {
            Expr::Alias(_, _) => FPD::Unsupported,
            Expr::Column(_) => FPD::Unsupported,
            Expr::ScalarVariable(_) => FPD::Unsupported,
            Expr::Literal(_) => FPD::Unsupported,
            Expr::BinaryExpr { op, left, right } => {
                if let Expr::Column(_) = &**left {
                    if let Expr::Literal(_) = &**right {
                        match op {
                            Operator::Eq => FPD::Exact,
                            Operator::NotEq => FPD::Exact,
                            Operator::Lt => FPD::Exact,
                            Operator::LtEq => FPD::Exact,
                            Operator::Gt => FPD::Exact,
                            Operator::GtEq => FPD::Exact,
                            Operator::Plus => FPD::Unsupported,
                            Operator::Minus => FPD::Unsupported,
                            Operator::Multiply => FPD::Unsupported,
                            Operator::Divide => FPD::Unsupported,
                            Operator::Modulus => FPD::Unsupported,
                            Operator::And => FPD::Inexact,
                            Operator::Or => FPD::Inexact,
                            Operator::Like => FPD::Unsupported,
                            Operator::NotLike => FPD::Unsupported,
                        }
                    } else {
                        FPD::Unsupported
                    }
                } else {
                    FPD::Unsupported
                }
            }
            Expr::Not(_) => FPD::Unsupported,
            Expr::IsNotNull(_) => FPD::Unsupported,
            Expr::IsNull(_) => FPD::Unsupported,
            Expr::Negative(_) => FPD::Unsupported,
            Expr::Between { .. } => FPD::Unsupported,
            Expr::Case { .. } => FPD::Unsupported,
            Expr::Cast { .. } => FPD::Exact,
            Expr::TryCast { .. } => FPD::Unsupported,
            Expr::Sort { .. } => FPD::Unsupported,
            Expr::ScalarFunction { .. } => FPD::Unsupported,
            Expr::ScalarUDF { .. } => FPD::Unsupported,
            Expr::AggregateFunction { .. } => FPD::Unsupported,
            Expr::AggregateUDF { .. } => FPD::Unsupported,
            Expr::InList { .. } => FPD::Unsupported,
            Expr::Wildcard => FPD::Unsupported,
            Expr::WindowFunction { .. } => FPD::Unsupported,
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
    filters: Option<Vec<Document>>,
    skip: Option<usize>,
    limit: Option<usize>,
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
        datafusion::physical_plan::Partitioning::UnknownPartitioning(self.partitions.len())
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
        let (response_tx, response_rx): (
            Sender<ArrowResult<RecordBatch>>,
            Receiver<ArrowResult<RecordBatch>>,
        ) = channel(2);

        let partition = self.partitions.get(partition).unwrap().clone();
        let schema = self.schema();

        task::spawn_blocking(move || {
            if let Err(e) = read_data(partition, schema, response_tx) {
                println!("MongoDB reader thread terminated due to error: {:?}", e);
            }
        });
        Ok(Box::pin(MongoStream {
            schema: self.schema(),
            inner: ReceiverStream::new(response_rx),
        }))
    }
}

fn read_data(
    partition: MongoPartition,
    schema: SchemaRef,
    response_tx: Sender<ArrowResult<RecordBatch>>,
) -> DFResult<()> {
    let mut reader = mongodb_arrow_connector::reader::Reader::try_new(
        &partition.config,
        schema,
        partition.filters.clone().unwrap_or_default(),
        partition.limit,
        partition.skip,
    )
    .unwrap(); // TODO: send error back

    loop {
        match reader.next() {
            Some(Ok(batch)) => {
                send_result(&response_tx, Ok(batch))?;
            }
            None => {
                break;
            }
            Some(Err(e)) => {
                send_result(&response_tx, Err(e))?;

                // Terminate thread with error
                return Err(DataFusionError::Execution(
                    "Unable to read data from MongoDB".to_string(),
                ));
            }
        }
    }

    Ok(())
}

fn send_result(
    response_tx: &Sender<ArrowResult<RecordBatch>>,
    result: ArrowResult<RecordBatch>,
) -> DFResult<()> {
    // Note this function is running on its own blockng tokio thread so blocking here is ok.
    response_tx
        .blocking_send(result)
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    Ok(())
}

struct MongoStream {
    schema: SchemaRef,
    inner: ReceiverStream<ArrowResult<RecordBatch>>,
}

impl Stream for MongoStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for MongoStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn write_filters(exprs: &[Expr]) -> Option<Vec<Document>> {
    if exprs.is_empty() {
        return None;
    }

    let mut filters = Vec::with_capacity(exprs.len());
    exprs.iter().for_each(|expr| match expr {
        Expr::Alias(_, _) => todo!(),
        Expr::Column(_) => todo!(),
        Expr::ScalarVariable(_) => todo!(),
        Expr::Literal(_) => todo!(),
        Expr::BinaryExpr { left, op, right } => {
            if let Expr::Column(col_name) = &**left {
                if let Expr::Literal(scalar) = &**right {
                    filters.push(match op {
                        Operator::Eq => {
                            doc! { "$match": {
                                col_name: scalar_to_bson(scalar)
                            }}
                        }
                        Operator::NotEq => doc! { "$match": {col_name: {
                            "$ne": scalar_to_bson(scalar)
                        }}},
                        Operator::Lt => doc! {"$match": {
                            col_name: {
                                "$lt": scalar_to_bson(scalar)
                            }
                        }},
                        Operator::LtEq => doc! {"$match": {
                            col_name: {
                                "$lte": scalar_to_bson(scalar)
                            }
                        }},
                        Operator::Gt => doc! {"$match": {
                            col_name: {
                                "$gt": scalar_to_bson(scalar)
                            }
                        }},
                        Operator::GtEq => doc! {"$match": {
                            col_name: {
                                "$gte": scalar_to_bson(scalar)
                            }
                        }},
                        Operator::Plus => todo!(),
                        Operator::Minus => todo!(),
                        Operator::Multiply => todo!(),
                        Operator::Divide => todo!(),
                        Operator::Modulus => todo!(),
                        Operator::And => todo!(),
                        Operator::Or => todo!(),
                        Operator::Like => todo!(),
                        Operator::NotLike => todo!(),
                    });
                } else {
                    todo!("right side should be literal")
                }
            } else {
                todo!("left expression has to be a column for now")
            }
        }
        Expr::Not(_) => todo!(),
        Expr::IsNotNull(_) => todo!(),
        Expr::IsNull(_) => todo!(),
        Expr::Negative(_) => todo!(),
        Expr::Between { .. } => todo!(),
        Expr::Case { .. } => todo!(),
        Expr::Cast { .. } => {
            todo!()
            // dbg!(&expr);
            // filters.push(doc! { "$addFields": {"$convert": {
            //     "input": data_type.to_string()
            // } } });
        }
        Expr::TryCast { .. } => todo!(),
        Expr::Sort { .. } => todo!(),
        Expr::ScalarFunction { .. } => todo!(),
        Expr::ScalarUDF { .. } => todo!(),
        Expr::AggregateFunction { .. } => todo!(),
        Expr::AggregateUDF { .. } => todo!(),
        Expr::InList { .. } => todo!(),
        Expr::Wildcard => todo!(),
        Expr::WindowFunction { .. } => todo!(),
    });

    Some(filters)
}

fn scalar_to_bson(scalar: &ScalarValue) -> Bson {
    match scalar {
        ScalarValue::Boolean(Some(bool)) => Bson::Boolean(*bool),
        ScalarValue::Float32(Some(value)) => Bson::Double(*value as f64),
        ScalarValue::Float64(Some(value)) => Bson::Double(*value),
        ScalarValue::Int8(Some(value)) => Bson::Int32(*value as i32),
        ScalarValue::Int16(Some(value)) => Bson::Int32(*value as i32),
        ScalarValue::Int32(Some(value)) => Bson::Int32(*value as i32),
        ScalarValue::Int64(Some(value)) => Bson::Int64(*value),
        ScalarValue::UInt8(Some(value)) => Bson::Int32(*value as i32),
        ScalarValue::UInt16(Some(value)) => Bson::Int32(*value as i32),
        ScalarValue::UInt32(Some(value)) => Bson::Int32(*value as i32),
        ScalarValue::UInt64(Some(value)) => Bson::Int64(*value as i64),
        ScalarValue::Utf8(Some(string)) => Bson::String(string.clone()),
        ScalarValue::LargeUtf8(Some(string)) => Bson::String(string.clone()),
        ScalarValue::Binary(_) => todo!(),
        ScalarValue::LargeBinary(_) => todo!(),
        ScalarValue::List(_, _) => todo!(),
        ScalarValue::Date32(_) => todo!(),
        ScalarValue::Date64(_) => todo!(),
        ScalarValue::TimestampSecond(_) => todo!(),
        ScalarValue::TimestampMillisecond(_) => todo!(),
        ScalarValue::TimestampMicrosecond(_) => todo!(),
        ScalarValue::TimestampNanosecond(_) => todo!(),
        ScalarValue::IntervalYearMonth(_) => todo!(),
        ScalarValue::IntervalDayTime(_) => todo!(),
        _ => Bson::Null,
    }
}
