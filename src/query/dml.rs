/// foo
use std::{
    collections::HashMap,
    fmt::Display,
    fs::{self},
    path::PathBuf,
};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};

use crate::{catalog::Catalog, helper::write_to_file};

use super::ddl::{ColumnDefinition, ColumnType};

#[derive(Debug, Serialize, Deserialize)]
pub struct PutItemCommand {
    pub table_name: String,
    pub item: Record,
}

pub type Record = HashMap<String, PrimitiveValue>;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum PrimitiveValue {
    Integer(u64),
    Float(f64),
    Boolean(bool),
    Text(String),
}

impl Display for PrimitiveValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Integer(val) => write!(f, "Integer({})", val),
            Self::Float(val) => write!(f, "Float({})", val),
            Self::Boolean(val) => write!(f, "Boolean({})", val),
            Self::Text(val) => write!(f, "Text({})", val),
        }
    }
}

impl PrimitiveValue {
    fn to_value(&self) -> String {
        match self {
            Self::Integer(val) => val.to_string(),
            Self::Float(val) => val.to_string(),
            Self::Boolean(val) => val.to_string(),
            Self::Text(val) => val.to_string(),
        }
    }
    fn from_string(value: String) -> Self {
        match value.parse::<u64>() {
            Ok(int) => PrimitiveValue::Integer(int),
            Err(_) => match value.parse::<f64>() {
                Ok(float) => PrimitiveValue::Float(float),
                Err(_) => match value.parse::<bool>() {
                    Ok(boolean) => PrimitiveValue::Boolean(boolean),
                    Err(_) => PrimitiveValue::Text(value),
                },
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetItemCommand {
    pub table_name: String,
    pub key: String,
}

pub fn get_item(command: GetItemCommand, catalog: &Catalog) -> anyhow::Result<Record> {
    match catalog.get_table(&command.table_name) {
        None => bail!("Table name '{}' doesn't exist.", command.table_name),
        Some(table) => {
            let key_position = table
                .columns
                .iter()
                .position(|col_def| col_def.name == table.primary_key)
                .with_context(|| "Internal Error: primary key must exist.")?;
            let table_rel_path = PathBuf::from(format!("{}.tbl", command.table_name));
            let table_path = catalog.directory_path.join(table_rel_path);
            if !table_path.exists() {
                bail!(
                    "FATAL: Internal Error: Table filepath does not exist: {}",
                    table_path.display()
                );
            }
            for line in fs::read_to_string(table_path)?.lines() {
                let parts: Vec<_> = line.split(",").collect();
                if parts[key_position] == command.key {
                    let record = parse_record(&table.columns, line)?;
                    return Ok(record);
                }
            }
            bail!("ERROR: Internal Error: Could not find item with primary key.");
        }
    }
}

fn parse_record(columns: &[ColumnDefinition], item: &str) -> anyhow::Result<Record> {
    let mut result = HashMap::new();
    for (idx, part) in item.split(",").enumerate() {
        let col_name = columns[idx].name.clone();
        let val = PrimitiveValue::from_string(part.to_string());
        result.insert(col_name, val);
    }
    Ok(result)
}

pub fn put_item(command: PutItemCommand, catalog: &Catalog) -> anyhow::Result<()> {
    // check if table name is valid
    match catalog.get_table(&command.table_name) {
        None => bail!("Table name '{}' doesn't exist.", command.table_name),
        Some(table) => {
            // check if item data is valid
            for (column_name, value) in &command.item {
                match table.get_column(column_name) {
                    None => bail!("Unknown column in item object: {}.", column_name),
                    Some(column) => {
                        match (&column.r#type, value) {
                            (ColumnType::Boolean, PrimitiveValue::Boolean(_)) => (),
                            (ColumnType::Integer, PrimitiveValue::Integer(_)) => (),
                            (ColumnType::Float, PrimitiveValue::Float(_)) => (),
                            (ColumnType::Text, PrimitiveValue::Text(_)) => (),
                            (col_type,val_type) => bail!("Column type mismatch. Column defined as type: {}, but provided value has type: {}.", col_type, val_type),
                        }
                    }
                }
            }
            insert_into_table(command.item, &command.table_name, catalog)?;
        }
    }
    Ok(())
}

fn insert_into_table(mut item: Record, table_name: &str, catalog: &Catalog) -> anyhow::Result<()> {
    let table_rel_path = PathBuf::from(format!("{}.tbl", table_name));
    let table_path = catalog.directory_path.join(table_rel_path);
    if !table_path.exists() {
        bail!(
            "FATAL: Internal Error: Table filepath does not exist: {}",
            table_path.display()
        );
    }

    match catalog.get_table(table_name) {
        None => bail!(
            "FATAL: Internal Error: Expected table {} to be present",
            table_name
        ),
        Some(table) => {
            let mut values = vec![];
            for column in &table.columns {
                let value = item.remove(&column.name);
                match value {
                    None => values.push("NULL".to_string()),
                    Some(val) => values.push(val.to_value()),
                }
            }
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(table_path)
                .with_context(|| "Could not open file for writing.")?;

            write_to_file(&mut file, values.join(","))?;
        }
    }
    Ok(())
}
