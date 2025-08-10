use crate::table_definitions::TABLES;
use reth_tracing::tracing::info;
use std::env;
use tokio_postgres::{Client, NoTls};



pub async fn connect_to_postgres() -> eyre::Result<Client> {
    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let (client, connection) = tokio_postgres::connect(&db_url, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    Ok(client)
}

pub async fn create_tables(client: &Client) -> eyre::Result<()> {
    for table in TABLES.iter() {
        let create_table_sql = table.create_table_sql();
        client.execute(&create_table_sql, &[]).await?;

        for index_sql in table.create_index_statements() {
            client.execute(&index_sql, &[]).await?;
        }
    }

    info!("Initialized database tables");
    Ok(())
}