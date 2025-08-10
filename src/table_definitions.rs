use lazy_static::lazy_static;

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



impl TableDefinition {
    pub fn create_table_sql(&self) -> String {
        let columns: Vec<String> = self.columns
            .iter()
            .map(|col| {
                let clickhouse_type = match col.sql_type {
                    "BIGINT" => "Int64",
                    "INTEGER" => "Int32", 
                    "SMALLINT" => "Int16",
                    "TEXT" | "VARCHAR" => "String",
                    "BOOLEAN" => "Bool",
                    "DOUBLE PRECISION" => "Float64",
                    "REAL" => "Float32",
                    "TIMESTAMP WITH TIME ZONE" | "TIMESTAMP" => "DateTime",
                    "DATE" => "Date",
                    _ => "String",
                };
                
                let nullable = if col.nullable { 
                    format!("Nullable({})", clickhouse_type)
                } else { 
                    clickhouse_type.to_string() 
                };
                
                format!("{} {}", col.name, nullable)
            })
            .collect();

        let primary_key_cols: Vec<String> = self.columns
            .iter()
            .filter(|col| col.primary_key)
            .map(|col| col.name.to_string())
            .collect();

        let engine = if primary_key_cols.is_empty() {
            "ENGINE = MergeTree() ORDER BY block_number".to_string()
        } else {
            format!("ENGINE = MergeTree() ORDER BY ({})", primary_key_cols.join(", "))
        };

        format!(
            "CREATE TABLE IF NOT EXISTS {} (\n    {}\n) {}",
            self.name,
            columns.join(",\n    "),
            engine
        )
    }

    pub fn create_index_statements(&self) -> Vec<String> {
        vec![]
    }

    pub fn revert_statement(&self) -> String {
        format!("ALTER TABLE {} DELETE WHERE block_number IN ({{}})", self.name)
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
            indexes: vec![],
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