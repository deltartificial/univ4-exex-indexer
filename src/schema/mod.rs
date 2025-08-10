mod types;
mod tables;

pub use types::{Table, Column};
pub use tables::definitions;

lazy_static::lazy_static! {
    pub static ref TABLES: Vec<Table> = definitions();
}

pub fn get(name: &str) -> Option<Table> { TABLES.iter().find(|t| t.name == name).cloned() }
