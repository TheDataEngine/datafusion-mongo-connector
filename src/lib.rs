use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::{DataType, Schema, SchemaRef, TimeUnit};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::datasource::TableProviderFilterPushDown as FPD;
use datafusion::logical_plan::{Expr, Operator};
use datafusion::physical_plan::{RecordBatchStream, Statistics};
use datafusion::scalar::ScalarValue;
use datafusion::{
    datasource::TableProvider,
    error::DataFusionError,
    physical_plan::{ExecutionPlan, SendableRecordBatchStream},
};
use futures::Stream;
use futures::StreamExt;
use mongodb::bson::*;
use mongodb_arrow_connector::reader::{Reader, ReaderConfig};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;

pub struct MongoSource {
    pub config: ReaderConfig,
    pub schema: SchemaRef,
}

#[async_trait]
impl TableProvider for MongoSource {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn scan(
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
            Some(1000),
        )
        .await?;
        let partitions = if let Some(num_records) = num_records {
            let partition_limit = limit.unwrap_or(num_records);
            let num_records = num_records.min(partition_limit);
            let num_partitions = (num_records + batch_size - 1) / batch_size;
            // println!(
            //     "There are {} partitions with {:?} batch per partition",
            //     num_partitions, batch_size
            // );
            (0..=num_partitions)
                .map(|p| MongoPartition {
                    config: self.config.clone(),
                    filters: read_filters.clone(),
                    limit: Some(batch_size),
                    skip: Some(p * batch_size),
                })
                .collect()
        } else {
            vec![MongoPartition {
                config: self.config.clone(),
                filters: write_filters(filters),
                limit,
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
        }))
    }

    fn supports_filter_pushdown(&self, filter: &Expr) -> datafusion::error::Result<FPD> {
        let supported = match filter {
            Expr::Alias(_, _) => FPD::Unsupported,
            Expr::Column(_) => FPD::Unsupported,
            Expr::ScalarVariable(_) => FPD::Unsupported,
            Expr::Literal(_) => FPD::Unsupported,
            Expr::BinaryExpr { op, left, right } => {
                match (&**left, &**right, op) {
                    (Expr::Column(_), Expr::Literal(_), op) => check_comparison_op(op),
                    (Expr::Column(_), Expr::BinaryExpr { .. }, op) => {
                        // This could be a / (b + 1)
                        let supports = self.supports_filter_pushdown(right)?;
                        if supports == FPD::Unsupported {
                            return Ok(supports);
                        }
                        check_comparison_op(op)
                    }
                    // Left is cast to a type, then compared with a literal
                    (Expr::Cast { data_type, .. }, Expr::Literal(_), op) => {
                        if to_aggregation_datatype(data_type).is_none() {
                            return Ok(FPD::Unsupported);
                        }
                        // check if the datatype being cast to is supported?
                        check_comparison_op(op)
                    }
                    (Expr::BinaryExpr { .. }, Expr::Literal(_), op) => {
                        let supports = self.supports_filter_pushdown(left)?;
                        if supports == FPD::Unsupported {
                            return Ok(supports);
                        }
                        // An exact or inexact filter is supported, continue evaluating
                        check_comparison_op(op)
                    }
                    _ => {
                        // dbg!((left, right, op));
                        FPD::Unsupported
                    }
                }
            }
            Expr::Not(expr) => self.supports_filter_pushdown(expr)?,
            Expr::IsNotNull(expr) => match **expr {
                Expr::Column(_) => FPD::Exact,
                _ => self.supports_filter_pushdown(expr)?,
            },
            Expr::IsNull(expr) => match **expr {
                Expr::Column(_) => FPD::Exact,
                _ => self.supports_filter_pushdown(expr)?,
            },
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
            Expr::InList { negated, .. } => {
                if *negated {
                    FPD::Unsupported
                } else {
                    // TODO: regex match not supported
                    FPD::Exact
                }
            }
            Expr::Wildcard => FPD::Unsupported,
            Expr::WindowFunction { .. } => FPD::Unsupported,
            Expr::GetIndexedField { .. } => FPD::Unsupported,
        };

        if supported == FPD::Unsupported {
            dbg!(filter);
        }

        Ok(supported)
    }
}

#[derive(Debug, Clone)]
pub struct MongoExec {
    /// Mongo partitions to read
    partitions: Vec<MongoPartition>,
    /// Schema after projection is applied
    schema: SchemaRef,
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

        tokio::task::spawn(async move {
            let mut reader = mongodb_arrow_connector::reader::Reader::try_new(
                &partition.config,
                schema,
                partition.filters.clone().unwrap_or_default(),
                partition.limit,
                partition.skip,
            )
            .unwrap();

            while let Ok(Some(batch)) = reader.next_batch().await {
                println!(
                    "Record batch has {} records and {} columns",
                    batch.num_rows(),
                    batch.num_columns()
                );
                response_tx.send(Ok(batch)).await.expect("Unable to send"); // TODO: handle this error
            }
        })
        .await
        .expect("Unable to spawn task"); // TODO: handle this error

        Ok(Box::pin(MongoStream {
            schema: self.schema(),
            inner: ReceiverStream::new(response_rx),
        }))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

struct MongoStream {
    schema: SchemaRef,
    inner: ReceiverStream<ArrowResult<RecordBatch>>,
}

impl Stream for MongoStream {
    type Item = datafusion::arrow::error::Result<datafusion::arrow::record_batch::RecordBatch>;

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

    let mut filters: Vec<Document> = Vec::with_capacity(exprs.len());
    exprs.iter().for_each(|expr| match expr {
        Expr::Alias(_, _) => todo!(),
        Expr::Column(_) => todo!(),
        Expr::ScalarVariable(_) => todo!(),
        Expr::Literal(_) => todo!(),
        Expr::BinaryExpr { left, op, right } => {
            match (&**left, &**right, op) {
                (Expr::Column(col), Expr::Literal(scalar), op) => {
                    let col_name = col.flat_name();
                    filters.push(match_op_to_scalar(&col_name, op, scalar));
                }
                (Expr::Column(col), Expr::BinaryExpr { .. }, op) => {
                    let col_name = col.flat_name();
                    let left_bson = df_expr_to_bson(left);
                    let right_bson = df_expr_to_bson(right);
                    dbg!((col_name, left_bson, right_bson, op));
                    todo!()
                }
                (Expr::BinaryExpr { .. }, Expr::Literal(scalar), op) => {
                    let left_bson = df_expr_to_bson(left);
                    filters.push(doc! {
                        "$match": {
                            "$expr": {
                                op_to_str(op): [left_bson, scalar_to_bson(scalar)]
                            }
                        }
                    });
                }
                (Expr::Cast { expr, data_type }, Expr::Literal(scalar), op) => {
                    // Handle the cast and literal separately
                    filters.push(doc! {
                        "$addFields": {
                            "convfield": {
                                "$convert": {
                                    "input": df_expr_to_bson(expr),
                                    "to": to_aggregation_datatype(data_type).unwrap(),
                                    "onError": Bson::Null
                                }
                            }
                        }
                    });
                    // Add literal
                    filters.push(match_op_to_scalar("convfield", op, scalar));
                    // Remove the convfield
                    filters.push(doc! { "$unset": "convfield" });
                }
                _ => {
                    panic!(
                        "Incorrectly passed a filter that is not implemented for {:?}, {}, {:?}",
                        left, op, right
                    )
                }
            };
        }
        Expr::Not(expr) => filters.push(doc! {
            "$match": {
                "$expr": {
                    "$not": [df_expr_to_bson(expr)]
                }
            }
        }),
        Expr::IsNotNull(expr) => filters.push(doc! {
            "$match": {
                "$expr": {
                    "$ne": [df_expr_to_bson(expr), Bson::Null]
                }
            }
        }),
        Expr::IsNull(expr) => filters.push(doc! {
            "$match": {
                "$expr": {
                    "$eq": [df_expr_to_bson(expr), Bson::Null]
                }
            }
        }),
        Expr::Negative(_) => todo!(),
        Expr::Between { .. } => todo!(),
        Expr::Case { .. } => todo!(),
        Expr::Cast { .. } => {
            todo!()
        }
        Expr::TryCast { .. } => todo!(),
        Expr::Sort { .. } => todo!(),
        Expr::ScalarFunction { .. } => todo!(),
        Expr::ScalarUDF { .. } => todo!(),
        Expr::AggregateFunction { .. } => todo!(),
        Expr::AggregateUDF { .. } => todo!(),
        Expr::InList { expr, list, .. } => {
            let list = list.iter().map(df_expr_to_bson).collect::<Vec<_>>();
            filters.push(doc! {
                "$match": {
                    "$expr": {
                        "$in": [df_expr_to_bson(expr), list]
                    }
                }
            });
        }
        Expr::Wildcard => todo!(),
        Expr::WindowFunction { .. } => todo!(),
        Expr::GetIndexedField { .. } => todo!(),
    });

    Some(filters)
}

fn to_aggregation_datatype(data_type: &DataType) -> Option<&str> {
    match data_type {
        DataType::Null => None,
        DataType::Boolean => Some("bool"),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32 => Some("int"),
        DataType::Int64 | DataType::UInt64 => Some("long"),
        DataType::Float16 => None,
        DataType::Float32 | DataType::Float64 => Some("double"),
        DataType::Timestamp(time_unit, _) if time_unit == &TimeUnit::Millisecond => Some("date"),
        DataType::Date32 => None, // can't convert number of days, need millis
        DataType::Date64 => Some("date"),
        DataType::Time32(_) => None,
        DataType::Time64(_) => None,
        DataType::Duration(_) => None,
        DataType::Interval(_) => None,
        DataType::Binary => None,
        DataType::FixedSizeBinary(_) => None,
        DataType::LargeBinary => None,
        DataType::Utf8 | DataType::LargeUtf8 => Some("string"),
        DataType::List(_) => None,
        DataType::FixedSizeList(_, _) => None,
        DataType::LargeList(_) => None,
        DataType::Struct(_) => None,
        DataType::Union(_) => None,
        DataType::Dictionary(_, _) => None,
        DataType::Decimal(_, _) => Some("decimal"),
        _ => None,
    }
}

fn df_expr_to_bson(expr: &Expr) -> Bson {
    match expr {
        Expr::Alias(_, _) => todo!(),
        Expr::Column(col) => Bson::String(format!("${}", col.flat_name())),
        Expr::ScalarVariable(_) => todo!(),
        Expr::Literal(scalar) => scalar_to_bson(scalar),
        Expr::BinaryExpr { left, op, right } => {
            let op_fn = op_to_str(op);
            Bson::Document(doc! {
                op_fn: vec![df_expr_to_bson(left), df_expr_to_bson(right)]
            })
        }
        Expr::Not(expr) => Bson::Document(doc! {
            "$not": df_expr_to_bson(expr)
        }),
        Expr::IsNotNull(_) => Bson::Document(doc! {
            "$ne": [df_expr_to_bson(expr), Bson::Null]
        }),
        Expr::IsNull(expr) => Bson::Document(doc! {
            "$eq": [df_expr_to_bson(expr), Bson::Null]
        }),
        Expr::Negative(_) => todo!(),
        Expr::Between { .. } => todo!(),
        Expr::Case { .. } => todo!(),
        Expr::Cast { .. } => todo!(),
        Expr::TryCast { .. } => todo!(),
        Expr::Sort { .. } => todo!(),
        Expr::ScalarFunction { .. } => todo!(),
        Expr::ScalarUDF { .. } => todo!(),
        Expr::AggregateFunction { .. } => todo!(),
        Expr::WindowFunction { .. } => todo!(),
        Expr::AggregateUDF { .. } => todo!(),
        Expr::InList { .. } => todo!(),
        Expr::Wildcard => todo!(),
        Expr::GetIndexedField { .. } => todo!(),
    }
}

#[inline]
fn op_to_str(op: &Operator) -> &str {
    match op {
        Operator::Eq => "$eq",
        Operator::NotEq => "$ne",
        Operator::Lt => "$lt",
        Operator::LtEq => "$lte",
        Operator::Gt => "$gt",
        Operator::GtEq => "$gte",
        Operator::Plus => "$add",
        Operator::Minus => "$subtract",
        Operator::Multiply => "$multiply",
        Operator::Divide => "$divide",
        Operator::Modulo => "$mod",
        Operator::And => "$and",
        Operator::Or => "$or",
        Operator::Like => todo!(),
        Operator::NotLike => todo!(),
        Operator::RegexMatch => todo!(),
        Operator::RegexIMatch => todo!(),
        Operator::RegexNotMatch => todo!(),
        Operator::RegexNotIMatch => todo!(),
        Operator::IsDistinctFrom => todo!(),
        Operator::IsNotDistinctFrom => todo!(),
    }
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
        ScalarValue::TimestampSecond(_, _) => todo!(),
        ScalarValue::TimestampMillisecond(Some(value), _) => {
            Bson::DateTime(mongodb::bson::DateTime::from_millis(*value))
        }
        ScalarValue::TimestampMicrosecond(_, _) => todo!(),
        ScalarValue::TimestampNanosecond(_, _) => todo!(),
        ScalarValue::IntervalYearMonth(_) => todo!(),
        ScalarValue::IntervalDayTime(_) => todo!(),
        _ => Bson::Null,
    }
}

fn match_op_to_scalar(col_name: &str, op: &Operator, scalar: &ScalarValue) -> Document {
    match op {
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
        Operator::Modulo => todo!(),
        Operator::And => todo!(),
        Operator::Or => todo!(),
        Operator::Like => todo!(),
        Operator::NotLike => todo!(),
        Operator::RegexMatch => todo!(),
        Operator::RegexIMatch => todo!(),
        Operator::RegexNotMatch => todo!(),
        Operator::RegexNotIMatch => todo!(),
        Operator::IsDistinctFrom => todo!(),
        Operator::IsNotDistinctFrom => todo!(),
    }
}

#[inline]
fn check_comparison_op(op: &Operator) -> FPD {
    match op {
        Operator::Eq => FPD::Exact,
        Operator::NotEq => FPD::Exact,
        Operator::Lt => FPD::Exact,
        Operator::LtEq => FPD::Exact,
        Operator::Gt => FPD::Exact,
        Operator::GtEq => FPD::Exact,
        Operator::Plus => FPD::Exact,
        Operator::Minus => FPD::Exact,
        Operator::Multiply => FPD::Exact,
        Operator::Divide => FPD::Exact,
        Operator::Modulo => FPD::Unsupported,
        Operator::And => FPD::Inexact,
        Operator::Or => FPD::Inexact,
        Operator::Like => FPD::Unsupported,
        Operator::NotLike => FPD::Unsupported,
        Operator::RegexMatch => FPD::Unsupported,
        Operator::RegexIMatch => FPD::Unsupported,
        Operator::RegexNotMatch => FPD::Unsupported,
        Operator::RegexNotIMatch => FPD::Unsupported,
        Operator::IsDistinctFrom => FPD::Unsupported,
        Operator::IsNotDistinctFrom => FPD::Unsupported,
    }
}
