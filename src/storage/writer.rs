use std::sync::Arc;
use clickhouse::Client;
use eyre::Result;
use crate::schema::Table;

pub struct ClickhouseWriter {
    client: Arc<Client>,
    table: Table,
    records: Vec<Vec<String>>,
}

impl ClickhouseWriter {
    pub fn new(client: &Arc<Client>, table: Table) -> Result<Self> {
        Ok(Self { client: Arc::clone(client), table, records: Vec::with_capacity(1024) })
    }

    #[inline]
    pub fn write_record(&mut self, record: Vec<String>) {
        self.records.push(record);
    }

    pub async fn finish(self) -> Result<usize> {
        if self.records.is_empty() { return Ok(0); }
        let total_records = self.records.len();
        let mut insert = self.client.insert(&self.table.name)?;
        for record in self.records { insert.write(&record).await?; }
        insert.end().await?;
        Ok(total_records)
    }

    pub async fn revert(&self, block_numbers: &[i64]) -> Result<()> {
        let mut block_list = String::with_capacity(block_numbers.len().saturating_mul(12).max(32));
        for (i, n) in block_numbers.iter().enumerate() {
            if i > 0 { block_list.push_str(", "); }
            block_list.push_str(&n.to_string());
        }
        let delete_stmt = self.table.revert_statement().replace("{}", &block_list);
        self.client.query(&delete_stmt).execute().await?;
        Ok(())
    }
}

pub trait IntoClickhouseValue { fn into_ch_value(&self) -> String; }

impl IntoClickhouseValue for String { #[inline] fn into_ch_value(&self) -> String { self.clone() } }
impl IntoClickhouseValue for &str { #[inline] fn into_ch_value(&self) -> String { self.to_string() } }
impl IntoClickhouseValue for i64 { #[inline] fn into_ch_value(&self) -> String { self.to_string() } }
impl IntoClickhouseValue for i32 { #[inline] fn into_ch_value(&self) -> String { self.to_string() } }
impl IntoClickhouseValue for i128 { #[inline] fn into_ch_value(&self) -> String { self.to_string() } }
impl IntoClickhouseValue for u128 { #[inline] fn into_ch_value(&self) -> String { self.to_string() } }
impl IntoClickhouseValue for bool { #[inline] fn into_ch_value(&self) -> String { if *self { "1" } else { "0" }.to_string() } }
impl IntoClickhouseValue for chrono::DateTime<chrono::Utc> { #[inline] fn into_ch_value(&self) -> String { self.format("%Y-%m-%d %H:%M:%S").to_string() } }
impl IntoClickhouseValue for primitive_types::H256 { #[inline] fn into_ch_value(&self) -> String { self.to_string() } }
impl IntoClickhouseValue for primitive_types::U256 { #[inline] fn into_ch_value(&self) -> String { self.to_string() } }
impl IntoClickhouseValue for alloy::primitives::Address { #[inline] fn into_ch_value(&self) -> String { self.to_checksum(Some(1)) } }
impl IntoClickhouseValue for alloy::primitives::FixedBytes<32> { #[inline] fn into_ch_value(&self) -> String { self.to_string() } }
impl<const BITS: usize, const LIMBS: usize> IntoClickhouseValue for alloy::primitives::Uint<BITS, LIMBS> { #[inline] fn into_ch_value(&self) -> String { self.to_string() } }
impl<const BITS: usize, const LIMBS: usize> IntoClickhouseValue for alloy::primitives::Signed<BITS, LIMBS> { #[inline] fn into_ch_value(&self) -> String { self.to_string() } }
impl<T: IntoClickhouseValue + ?Sized> IntoClickhouseValue for &T { #[inline] fn into_ch_value(&self) -> String { (*self).into_ch_value() } }
impl IntoClickhouseValue for Vec<u8> { #[inline] fn into_ch_value(&self) -> String { alloy::primitives::hex::encode_prefixed(self) } }
impl IntoClickhouseValue for [u8] { #[inline] fn into_ch_value(&self) -> String { alloy::primitives::hex::encode_prefixed(self) } }

#[macro_export]
macro_rules! values {
    ($($value:expr),* $(,)?) => {{
        use $crate::storage::writer::IntoClickhouseValue;
        vec![$($value.into_ch_value()),*]
    }};
}
