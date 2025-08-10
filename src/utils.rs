use crate::table_definitions::TABLES;
use reth_tracing::tracing::info;
use std::env;
use clickhouse::Client;



pub async fn connect_to_clickhouse() -> eyre::Result<Client> {
    let database_url = env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());
    let database_name = env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "default".to_string());
    
    let client = Client::default()
        .with_url(database_url)
        .with_database(database_name);
    
    Ok(client)
}

pub async fn create_tables(client: &Client) -> eyre::Result<()> {
    for table in TABLES.iter() {
        let create_table_sql = table.create_table_sql();
        client.query(&create_table_sql).execute().await?;

        for index_sql in table.create_index_statements() {
            client.query(&index_sql).execute().await?;
        }
    }

    info!("Initialized database tables");
    Ok(())
}