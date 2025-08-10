use lazy_static::lazy_static;
use tokio_postgres::types::Type;

#[derive(Debug, Clone)]
pub struct TableDefinition {
    pub name: &'static str,
    pub columns: Vec<ColumnDefinition>,
    pub indexes: Vec<&'static str>,
}

#[derive(Debug, Clone)]
pub struct ColumnDefinition {
    pub name: &'static str,
    pub sql_type: &'static str,
    pub nullable: bool,
    pub primary_key: bool,
}

impl ColumnDefinition {
    pub fn get_postgres_type(&self) -> Type {
        match self.sql_type {
            "BIGINT" => Type::INT8,
            "INTEGER" => Type::INT4,
            "SMALLINT" => Type::INT2,
            "TEXT" | "VARCHAR" => Type::TEXT,
            "BOOLEAN" => Type::BOOL,
            "DOUBLE PRECISION" => Type::FLOAT8,
            "REAL" => Type::FLOAT4,
            "TIMESTAMP WITH TIME ZONE" => Type::TIMESTAMPTZ,
            "TIMESTAMP" => Type::TIMESTAMP,
            "DATE" => Type::DATE,
            "BYTEA" => Type::BYTEA,
            "JSON" => Type::JSON,
            "JSONB" => Type::JSONB,
            _ => Type::TEXT,
        }
    }
}

impl TableDefinition {
    pub fn create_table_sql(&self) -> String {
        let columns: Vec<String> = self.columns
            .iter()
            .map(|col| {
                let nullable = if col.nullable { "" } else { " NOT NULL" };
                let pk = if col.primary_key { " PRIMARY KEY" } else { "" };
                format!("{} {}{}{}", col.name, col.sql_type, nullable, pk)
            })
            .collect();

        format!(
            "CREATE TABLE IF NOT EXISTS {} (\n    {}\n)",
            self.name,
            columns.join(",\n    ")
        )
    }

    pub fn create_index_statements(&self) -> Vec<String> {
        self.indexes.iter().map(|idx| idx.to_string()).collect()
    }

    pub fn revert_statement(&self) -> String {
        format!("DELETE FROM {} WHERE block_number = ANY($1::bigint[])", self.name)
    }
}

pub fn get_table_definitions() -> Vec<TableDefinition> {
    vec![
        TableDefinition {
            name: "uni_v4_pools",
            columns: vec![
                ColumnDefinition {
                    name: "block_number",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "transaction_hash",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "transaction_index",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "log_index",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "log_address",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "pool_id",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: true,
                },
                ColumnDefinition {
                    name: "currency0",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "currency1",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "fee",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "tick_spacing",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "hooks",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "sqrt_price_x96",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "tick",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "updated_at",
                    sql_type: "TIMESTAMP WITH TIME ZONE",
                    nullable: false,
                    primary_key: false,
                },
            ],
            indexes: vec![
                "CREATE INDEX IF NOT EXISTS idx_uni_v4_pools_pool_id ON uni_v4_pools (pool_id)",
            ],
        },
    ]
}

lazy_static! {
    pub static ref TABLES: Vec<TableDefinition> = get_table_definitions();
}

pub fn get_table_definition(name: &str) -> Option<TableDefinition> {
    TABLES.iter().find(|t| t.name == name).cloned()
}

pub fn get_table(name: &str) -> Option<TableDefinition> {
    get_table_definition(name)
}