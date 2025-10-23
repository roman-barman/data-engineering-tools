use deltalake::protocol::SaveMode;
use deltalake::{DataType, DeltaOps, PrimitiveType};
use std::env;
use url::Url;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    create_table().await?;
    Ok(())
}

async fn create_table() -> Result<(), Box<dyn std::error::Error>> {
    let mut dir = env::current_dir()?;
    dir.push("data");
    dir.push("http_requests");

    let table = DeltaOps::try_from_uri(Url::parse(format!("file://{}", dir.display()).as_str())?)
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
