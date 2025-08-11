#[derive(Debug, Clone)]
pub struct Table {
    pub name: &'static str,
    pub columns: Vec<Column>,
    pub indexes: Vec<&'static str>,
    pub partition_by: Option<&'static str>,
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
        let mut columns: Vec<String> = Vec::with_capacity(self.columns.len());
        for col in &self.columns {
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
                // Pass-through for common ClickHouse-native types
                "UInt32" => "UInt32",
                "UInt64" => "UInt64",
                "Int32" => "Int32",
                "Int64" => "Int64",
                "FixedString(66)" => "FixedString(66)",
                "FixedString(40)" => "FixedString(40)",
                "DateTime64(3, 'UTC')" => "DateTime64(3, 'UTC')",
                "Decimal(38,0)" => "Decimal(38, 0)",
                _ => "String",
            };
            let nullable = if col.nullable { format!("Nullable({})", clickhouse_type) } else { clickhouse_type.to_string() };
            columns.push(format!("{} {}", col.name, nullable));
        }

        let mut primary_key_cols: Vec<String> = Vec::with_capacity(self.columns.len());
        for col in &self.columns {
            if col.primary_key { primary_key_cols.push(col.name.to_string()); }
        }

        let order_by = if primary_key_cols.is_empty() {
            "block_number".to_string()
        } else {
            primary_key_cols.join(", ")
        };

        let mut engine = String::new();
        engine.push_str("ENGINE = MergeTree() ");
        if let Some(partition) = self.partition_by {
            engine.push_str(&format!("PARTITION BY {} ", partition));
        }
        engine.push_str(&format!(
            "ORDER BY ({}) SETTINGS index_granularity = 8192, compress = 'LZ4'",
            order_by
        ));

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

