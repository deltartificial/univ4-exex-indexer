use std::env;
use clickhouse::Client;

pub async fn connect_to_clickhouse() -> eyre::Result<Client> {
    let database_url = env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());
    let database_name = env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "default".to_string());
    
    let client = Client::default()
        .with_url(database_url)
        .with_database(database_name)
        .with_option("compression", "lz4");
    
    Ok(client)
}
