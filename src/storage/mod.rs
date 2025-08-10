pub mod writer;
use clickhouse::Client;
use crate::schema::TABLES;
use reth_tracing::tracing::info;

pub async fn connect() -> eyre::Result<Client> {
    crate::utils::connect_to_clickhouse().await
}

pub async fn init_tables(client: &Client) -> eyre::Result<()> {
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
