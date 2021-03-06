use std::{fs::File, path::PathBuf, process::Command, sync::Arc};

use arrow::datatypes::*;
use mongodb_arrow_connector::writer::*;

#[tokio::main]
async fn main() {
    Command::new("wget")
        .args(&[
            "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-01.csv",
            "-P",
            "./data",
        ])
        .output()
        .expect("Unable to download");

    // load data to mongodb
    let csv_schema = Arc::new(Schema::new(vec![
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

    let paths: Vec<PathBuf> = std::fs::read_dir("./data")
        .unwrap()
        .filter_map(|p| {
            let p = p.unwrap();
            if p.file_name().to_str().unwrap().ends_with(".csv") {
                Some(p.path())
            } else {
                None
            }
        })
        .collect();

    for path in paths {
        println!("Writing data for {:?}", path);
        let file = File::open(path).unwrap();
        let mut csv_reader =
            arrow::csv::Reader::new(file, csv_schema.clone(), true, Some(b','), 8196, None, None);

        let writer_config = WriterConfig {
            hostname: "localhost".to_string(),
            port: Some(27018),
            database: "datafusion".to_string(),
            collection: "nyc_taxi".to_string(),
            credential: None,
            auth_db: None,
            write_mode: WriteMode::Append,
            coerce_types: true,
        };
        let mongo_writer = Writer::try_new(writer_config, csv_schema.as_ref().clone())
            .await
            .unwrap();
        while let Some(Ok(batch)) = csv_reader.next() {
            println!("-");
            mongo_writer.write(&batch).await.unwrap();
        }
    }
}
