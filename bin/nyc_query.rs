use std::{sync::Arc, time::Instant};

use arrow::{datatypes::*, record_batch::RecordBatch, util::pretty::print_batches};
use datafusion::prelude::*;
use datafusion_mongo_connector::*;
use mongodb_arrow_connector::reader::*;

#[tokio::main]
async fn main() {
    let now = Instant::now();
    let mut context = ExecutionContext::new();

    let nyc_schema = Arc::new(Schema::new(vec![
        Field::new("VendorID", DataType::Utf8, true),
        Field::new("tpep_pickup_datetime", DataType::Utf8, true),
        Field::new("tpep_dropoff_datetime", DataType::Utf8, true),
        Field::new("passenger_count", DataType::Int32, true),
        Field::new("trip_distance", DataType::Utf8, true),
        Field::new("RatecodeID", DataType::Utf8, true),
        Field::new("store_and_fwd_flag", DataType::Utf8, true),
        Field::new("PULocationID", DataType::Utf8, true),
        Field::new("DOLocationID", DataType::Utf8, true),
        Field::new("payment_type", DataType::Utf8, true),
        Field::new("fare_amount", DataType::Float64, true),
        Field::new("extra", DataType::Float64, true),
        Field::new("mta_tax", DataType::Float64, true),
        Field::new("tip_amount", DataType::Float64, true),
        Field::new("tolls_amount", DataType::Float64, true),
        Field::new("improvement_surcharge", DataType::Float64, true),
        Field::new("total_amount", DataType::Float64, true),
    ]));

    let mongo_nyc = MongoSource {
        config: ReaderConfig {
            hostname: "192.168.0.106".to_string(),
            port: Some(27018),
            database: "datafusion".to_string(),
            collection: "nyc_taxi".to_string(),
            credential: None,
            auth_db: None,
        },
        schema: nyc_schema.clone(),
    };
    context
        .register_table("mongo_nyc", Arc::new(mongo_nyc))
        .unwrap();

    // context
    //     .register_csv(
    //         "csv_nyc",
    //         "./data",
    //         CsvReadOptions {
    //             has_header: true,
    //             delimiter: b',',
    //             schema: Some(nyc_schema.as_ref()),
    //             schema_infer_max_records: (1000),
    //             file_extension: "csv",
    //         },
    //     )
    //     .unwrap();

    let df = context
        .sql(
            "select 
            count(*) as total_records,
            VendorID as vid,
            sum(cast(trip_distance as float)) as total_distance
            from mongo_nyc
            where 
                passenger_count > 3 and 
                cast(trip_distance as float) < 5.00 and
                fare_amount / (total_amount + 0.001) > 0.70 and
                total_amount < 20.0
                and passenger_count is not null
                and -passenger_count < -2
                and VendorID in ('2', '4')
            group by VendorID
            order by vid
            limit 100",
        )
        .await
        .unwrap();
    let logical_plan = df.to_logical_plan();
    dbg!(&logical_plan);
    let batches = df
        .collect_partitioned()
        .await
        .unwrap()
        .into_iter()
        .flatten()
        .collect::<Vec<RecordBatch>>();
    print_batches(&batches).unwrap();

    let elapsed = now.elapsed();
    println!("Execution took {:?}s", elapsed.as_secs());
}
