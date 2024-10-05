use std::{fmt::Display, fs::File};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};

use crate::catalog::Catalog;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CreateTableCommand {
    pub name: String,
    pub columns: Vec<ColumnDefinition>,
    pub primary_key: String,
}

impl CreateTableCommand {
    pub fn get_column(&self, name: &str) -> Option<&ColumnDefinition> {
        self.columns.iter().find(|col| col.name == name)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ColumnDefinition {
    pub name: String,
    pub r#type: ColumnType,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ColumnType {
    Integer,
    Float,
    Text,
    Boolean,
}

impl Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Integer => write!(f, "Integer"),
            Self::Float => write!(f, "Float"),
            Self::Boolean => write!(f, "Boolean"),
            Self::Text => write!(f, "Text"),
        }
    }
}

/// creates a table in the catalog and also on the disk
pub fn create_table(table: CreateTableCommand, catalog: &mut Catalog) -> anyhow::Result<()> {
    if catalog.get_table(&table.name).is_some() {
        bail!("Table name '{}' already exists", table.name);
    }
    create_table_on_disk(&table, catalog)?;
    catalog.add_table(table)?;
    Ok(())
}

fn create_table_on_disk(table: &CreateTableCommand, catalog: &Catalog) -> anyhow::Result<()> {
    let table_path = catalog.get_table_path(&table.name);
    if table_path.exists() {
        bail!(
            "FATAL: Internal Error: Table filepath {} already exists.",
            table_path.display()
        );
    }

    File::create(&table_path).with_context(|| {
        format!(
            "FATAL: Internal Error: Failed to create table file path: {}",
            table_path.clone().display()
        )
    })?;

    Ok(())
}
