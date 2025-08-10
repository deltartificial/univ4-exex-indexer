#[derive(Debug, Clone)]
pub struct Table {
    pub name: &'static str,
    pub columns: Vec<Column>,
    pub indexes: Vec<&'static str>,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: &'static str,
    pub sql_type: &'static str,
    pub nullable: bool,
    pub primary_key: bool,
}

impl Table {
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
                let nullable = if col.nullable { format!("Nullable({})", clickhouse_type) } else { clickhouse_type.to_string() };
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

    pub fn create_index_statements(&self) -> Vec<String> { vec![] }

    pub fn revert_statement(&self) -> String {
        format!("ALTER TABLE {} DELETE WHERE block_number IN ({{}})", self.name)
    }
}

pub fn definitions() -> Vec<Table> {
    vec![
        Table {
            name: "uni_v4_pools",
            columns: vec![
                Column { name: "block_number", sql_type: "BIGINT", nullable: false, primary_key: false },
                Column { name: "transaction_hash", sql_type: "VARCHAR", nullable: false, primary_key: false },
                Column { name: "transaction_index", sql_type: "BIGINT", nullable: false, primary_key: false },
                Column { name: "log_index", sql_type: "BIGINT", nullable: false, primary_key: false },
                Column { name: "log_address", sql_type: "VARCHAR", nullable: false, primary_key: false },
                Column { name: "pool_id", sql_type: "VARCHAR", nullable: false, primary_key: true },
                Column { name: "currency0", sql_type: "VARCHAR", nullable: false, primary_key: false },
                Column { name: "currency1", sql_type: "VARCHAR", nullable: false, primary_key: false },
                Column { name: "fee", sql_type: "VARCHAR", nullable: false, primary_key: false },
                Column { name: "tick_spacing", sql_type: "VARCHAR", nullable: false, primary_key: false },
                Column { name: "hooks", sql_type: "VARCHAR", nullable: false, primary_key: false },
                Column { name: "sqrt_price_x96", sql_type: "VARCHAR", nullable: false, primary_key: false },
                Column { name: "tick", sql_type: "VARCHAR", nullable: false, primary_key: false },
                Column { name: "updated_at", sql_type: "TIMESTAMP WITH TIME ZONE", nullable: false, primary_key: false },
            ],
            indexes: vec![],
        },
    ]
}

lazy_static::lazy_static! {
    pub static ref TABLES: Vec<Table> = definitions();
}

pub fn get(name: &str) -> Option<Table> { TABLES.iter().find(|t| t.name == name).cloned() }
