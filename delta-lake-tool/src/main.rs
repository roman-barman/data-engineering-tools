use datafusion::arrow;
use deltalake::protocol::SaveMode;
use deltalake::writer::{DeltaWriter, JsonWriter};
use deltalake::{DataType, DeltaOps, PrimitiveType};
use deltalake::datafusion::prelude::SessionContext;
use std::env;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;
use url::Url;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut dir = env::current_dir()?;
    dir.push("data");
    dir.push("http_requests");
    let url = Url::parse(format!("file://{}", dir.display()).as_str())?;

    create_table(url.clone()).await?;
    write_data(url.clone()).await?;
    read_data(url.clone()).await?;

    Ok(())
}

async fn read_data(url: Url) -> Result<(), Box<dyn std::error::Error>> {
    let table = deltalake::open_table(url).await?;

    let ctx = SessionContext::new();
    ctx.register_table("http_requests", Arc::new(table))?;
    let results = ctx
        .sql("SELECT * FROM http_requests")
        .await?
        .collect()
        .await?;
    let pretty_results = arrow::util::pretty::pretty_format_batches(&results)?.to_string();

    println!("{}", pretty_results);

    Ok(())
}

async fn write_data(url: Url) -> Result<(), Box<dyn std::error::Error>> {
    let mut table = deltalake::open_table(url).await?;
    let mut writer = JsonWriter::for_table(&table)?;

    let mut buffer = String::new();
    let _ = File::open("./import.json")?.read_to_string(&mut buffer)?;

    writer
        .write(
            buffer
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect(),
        )
        .await?;
    writer.flush_and_commit(&mut table).await?;

    Ok(())
}

async fn create_table(url: Url) -> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaOps::try_from_uri(url)
        .await?
        .create()
        .with_table_name("http")
        .with_comment("HTTP request logs")
        .with_column(
            "timestamp".to_string(),
            DataType::Primitive(PrimitiveType::Timestamp),
            true,
            None,
        )
        .with_column(
            "host".to_string(),
            DataType::Primitive(PrimitiveType::String),
            true,
            None,
        )
        .with_column(
            "method".to_string(),
            DataType::Primitive(PrimitiveType::String),
            true,
            None,
        )
        .with_column(
            "path".to_string(),
            DataType::Primitive(PrimitiveType::String),
            true,
            None,
        )
        .with_column(
            "status_code".to_string(),
            DataType::Primitive(PrimitiveType::Integer),
            true,
            None,
        )
        .with_column(
            "size".to_string(),
            DataType::Primitive(PrimitiveType::Long),
            true,
            None,
        )
        .with_save_mode(SaveMode::Append)
        .await?;

    println!("{}", table);
    Ok(())
}
