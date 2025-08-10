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

        let engine = format!(
            "ENGINE = MergeTree() ORDER BY ({}) SETTINGS index_granularity = 8192, compress = 'LZ4'",
            order_by
        );

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

