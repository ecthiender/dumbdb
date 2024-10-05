use std::{fmt::Display, fs::File, path::PathBuf};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};

use crate::{catalog::Catalog, helper::write_to_file};

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub r#type: ColumnType,
}

#[derive(Debug, Serialize, Deserialize)]
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
    catalog.tables.push(table);
    catalog.flush()?;
    Ok(())
}

fn create_table_on_disk(table: &CreateTableCommand, catalog: &Catalog) -> anyhow::Result<()> {
    let table_rel_path = PathBuf::from(format!("{}.tbl", table.name.clone()));
    let table_path = catalog.directory_path.join(table_rel_path);
    if table_path.exists() {
        bail!(
            "FATAL: Internal Error: Table filepath {} already exists.",
            table_path.display()
        );
    }

    // prepare the column names in CSV format
    let column_names = table
        .columns
        .iter()
        .map(|col| col.name.clone())
        .collect::<Vec<_>>()
        .join(",");

    let mut file = File::create(&table_path).with_context(|| {
        format!(
            "FATAL: Internal Error: Failed to create table file path: {}",
            table_path.clone().display()
        )
    })?;
    write_to_file(&mut file, column_names).with_context(|| {
        format!(
            "FATAL: Internal Error: Failed to write columns to table file path: {}",
            table_path.clone().display()
        )
    })?;

    Ok(())
}
