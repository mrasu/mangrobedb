use arrow::array::{Int32Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{ArrowNativeType, DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow_flight::error::FlightError;
use arrow_flight::sql::CommandStatementIngest;
use arrow_flight::sql::client::FlightSqlServiceClient;
use clap::Parser;
use futures::stream;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::transport::Channel;

const DEFAULT_ADDR: &str = "127.0.0.1:50051";
const DEFAULT_TABLE: &str = "hello_table";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::parse();
    let endpoint = format!("http://{}", config.addr);
    let channel = Channel::from_shared(endpoint)?.connect().await?;
    let mut client = FlightSqlServiceClient::new(channel);

    let batch = sample_batch()?;
    let command = CommandStatementIngest {
        table: config.table_name.to_string(),
        ..Default::default()
    };
    client
        .execute_ingest(
            command,
            stream::iter(vec![Ok::<RecordBatch, FlightError>(batch)]),
        )
        .await?;

    println!(
        "sent sample import to table={} at {}",
        config.table_name, config.addr
    );

    Ok(())
}

#[derive(Debug, Parser)]
struct Config {
    #[arg(long, default_value = DEFAULT_ADDR)]
    addr: String,

    #[arg(long = "table", default_value = DEFAULT_TABLE)]
    table_name: String,
}

fn sample_batch() -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("stream", DataType::Int64, true),
        Field::new("message", DataType::Utf8, true),
        Field::new("user", DataType::Utf8, true),
        Field::new("new_user", DataType::Utf8, true),
        Field::new(
            "posted_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "posted_at_hour",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]));

    let now = SystemTime::now();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(Int64Array::from(vec![0, 0, 0, 0])),
            Arc::new(StringArray::from(vec![
                "hello", "flight", "mangrobe", "client",
            ])),
            Arc::new(StringArray::from(vec!["foo", "bar", "foo", "bar"])),
            Arc::new(StringArray::from(vec!["foo1", "bar1", "foo1", "bar1"])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                1_777_523_200_000_000,
                1_777_526_800_000_000,
                now.duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_micros()
                    .to_i64()
                    .unwrap(),
                1_777_527_800_000_000,
            ])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                truncate_micros_to_hour(1_777_523_200_000_000),
                truncate_micros_to_hour(1_777_526_800_000_000),
                truncate_micros_to_hour(
                    now.duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_micros()
                        .to_i64()
                        .unwrap(),
                ),
                truncate_micros_to_hour(1_777_527_800_000_000),
            ])),
        ],
    )?;

    Ok(batch)
}

const SECONDS_PER_HOUR: i64 = 60 * 60;
const MILLIS_PER_HOUR: i64 = SECONDS_PER_HOUR * 1_000;
const MICROS_PER_HOUR: i64 = MILLIS_PER_HOUR * 1_000;

fn truncate_micros_to_hour(value: i64) -> i64 {
    value.div_euclid(MICROS_PER_HOUR) * MICROS_PER_HOUR
}
