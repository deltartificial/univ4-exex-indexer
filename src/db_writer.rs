use crate::table_definitions::TableDefinition;
use eyre::Result;
use std::sync::Arc;
use clickhouse::Client;

pub struct DbWriter {
    client: Arc<Client>,
    table: TableDefinition,
    records: Vec<Vec<String>>,
}

impl DbWriter {
    pub async fn new(client: &Arc<Client>, table: TableDefinition) -> Result<Self> {
        Ok(Self {
            client: Arc::clone(client),
            table,
            records: Vec::new(),
        })
    }

    pub async fn write_record(&mut self, record: Vec<String>) -> Result<()> {
        self.records.push(record);
        Ok(())
    }

    pub async fn finish(self) -> Result<usize> {
        if self.records.is_empty() {
            return Ok(0);
        }



        let total_records = self.records.len();
        
        let mut insert = self.client.insert(&self.table.name)?;
        for record in self.records {
            insert.write(&record).await?;
        }
        insert.end().await?;

        Ok(total_records)
    }

    pub async fn revert(&self, block_numbers: &[i64]) -> Result<()> {
        let block_list = block_numbers.iter()
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(", ");
            
        let delete_stmt = self.table.revert_statement().replace("{}", &block_list);
        
        self.client
            .query(&delete_stmt)
            .execute()
            .await?;
        Ok(())
    }
}

pub trait EthereumValue {
    fn to_clickhouse_value(&self) -> String;
}

impl EthereumValue for primitive_types::H256 {
    fn to_clickhouse_value(&self) -> String {
        self.to_string()
    }
}

impl EthereumValue for primitive_types::U256 {
    fn to_clickhouse_value(&self) -> String {
        self.to_string()
    }
}

impl EthereumValue for alloy::primitives::Address {
    fn to_clickhouse_value(&self) -> String {
        self.to_checksum(Some(1))
    }
}

impl EthereumValue for Option<String> {
    fn to_clickhouse_value(&self) -> String {
        self.clone().unwrap_or_else(|| "".to_string())
    }
}

impl<'a> EthereumValue for Option<&'a str> {
    fn to_clickhouse_value(&self) -> String {
        self.map(|s| s.to_string()).unwrap_or_else(|| "".to_string())
    }
}

impl EthereumValue for Option<i64> {
    fn to_clickhouse_value(&self) -> String {
        self.map(|v| v.to_string()).unwrap_or_else(|| "0".to_string())
    }
}

impl EthereumValue for Option<i32> {
    fn to_clickhouse_value(&self) -> String {
        self.map(|v| v.to_string()).unwrap_or_else(|| "0".to_string())
    }
}

impl EthereumValue for Option<bool> {
    fn to_clickhouse_value(&self) -> String {
        self.map(|v| if v { "1" } else { "0" }.to_string()).unwrap_or_else(|| "0".to_string())
    }
}

impl EthereumValue for Option<chrono::DateTime<chrono::Utc>> {
    fn to_clickhouse_value(&self) -> String {
        self.map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string()).unwrap_or_else(|| "1970-01-01 00:00:00".to_string())
    }
}

impl EthereumValue for Option<alloy::primitives::Address> {
    fn to_clickhouse_value(&self) -> String {
        self.as_ref().map(|addr| addr.to_checksum(Some(1))).unwrap_or_else(|| "".to_string())
    }
}

impl EthereumValue for Option<primitive_types::H256> {
    fn to_clickhouse_value(&self) -> String {
        self.as_ref().map(|h| h.to_string()).unwrap_or_else(|| "".to_string())
    }
}

impl EthereumValue for Option<primitive_types::U256> {
    fn to_clickhouse_value(&self) -> String {
        self.as_ref().map(|u| u.to_string()).unwrap_or_else(|| "0".to_string())
    }
}

impl EthereumValue for Option<alloy::primitives::FixedBytes<32>> {
    fn to_clickhouse_value(&self) -> String {
        self.as_ref().map(|bytes| bytes.to_string()).unwrap_or_else(|| "".to_string())
    }
}

impl<'a> EthereumValue for Option<&'a alloy::primitives::FixedBytes<32>> {
    fn to_clickhouse_value(&self) -> String {
        self.map(|bytes| bytes.to_string()).unwrap_or_else(|| "".to_string())
    }
}

impl EthereumValue for Option<alloy::primitives::Uint<256, 4>> {
    fn to_clickhouse_value(&self) -> String {
        self.as_ref().map(|u| u.to_string()).unwrap_or_else(|| "0".to_string())
    }
}

impl EthereumValue for Vec<u8> {
    fn to_clickhouse_value(&self) -> String {
        alloy::primitives::hex::encode_prefixed(self)
    }
}

impl EthereumValue for [u8] {
    fn to_clickhouse_value(&self) -> String {
        alloy::primitives::hex::encode_prefixed(self)
    }
}

impl EthereumValue for String {
    fn to_clickhouse_value(&self) -> String {
        self.clone()
    }
}

impl EthereumValue for str {
    fn to_clickhouse_value(&self) -> String {
        self.to_string()
    }
}

impl EthereumValue for i64 {
    fn to_clickhouse_value(&self) -> String {
        self.to_string()
    }
}

impl EthereumValue for i32 {
    fn to_clickhouse_value(&self) -> String {
        self.to_string()
    }
}

impl EthereumValue for bool {
    fn to_clickhouse_value(&self) -> String {
        if *self { "1" } else { "0" }.to_string()
    }
}

impl EthereumValue for chrono::DateTime<chrono::Utc> {
    fn to_clickhouse_value(&self) -> String {
        self.format("%Y-%m-%d %H:%M:%S").to_string()
    }
}

impl EthereumValue for alloy::primitives::FixedBytes<32> {
    fn to_clickhouse_value(&self) -> String {
        self.to_string()
    }
}

impl<const BITS: usize, const LIMBS: usize> EthereumValue for alloy::primitives::Uint<BITS, LIMBS> {
    fn to_clickhouse_value(&self) -> String {
        self.to_string()
    }
}

impl<const BITS: usize, const LIMBS: usize> EthereumValue for alloy::primitives::Signed<BITS, LIMBS> {
    fn to_clickhouse_value(&self) -> String {
        self.to_string()
    }
}

impl<T: EthereumValue + ?Sized> EthereumValue for &T {
    fn to_clickhouse_value(&self) -> String {
        (*self).to_clickhouse_value()
    }
}

impl EthereumValue for f64 {
    fn to_clickhouse_value(&self) -> String {
        self.to_string()
    }
}

#[macro_export]
macro_rules! record_values {
    ($($value:expr),* $(,)?) => {{
        use $crate::db_writer::EthereumValue;
        let values: Vec<String> = vec![$($value.to_clickhouse_value()),*];
        values
    }};
}