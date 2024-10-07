use std::{collections::HashMap, fmt::Display};

use anyhow::bail;
use serde::{Deserialize, Serialize};

use crate::storage::Tuple;
use crate::{
    catalog::{Catalog, TableName},
    query::ddl::{ColumnDefinition, ColumnName, ColumnType},
    GetItemCommand,
};

use super::get_item;

#[derive(Debug, Serialize, Deserialize)]
pub struct PutItemCommand {
    pub table_name: TableName,
    pub item: Item,
}

pub type Item = HashMap<ColumnName, PrimitiveValue>;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
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
    pub fn to_storage_format(&self) -> String {
        match self {
            Self::Integer(val) => val.to_string(),
            Self::Float(val) => val.to_string(),
            Self::Boolean(val) => val.to_string(),
            Self::Text(val) => val.to_string(),
        }
    }
    pub fn from_string(value: String) -> Option<Self> {
        if value == "NULL" || value.is_empty() {
            None
        } else {
            Some(match value.parse::<u64>() {
                Ok(int) => PrimitiveValue::Integer(int),
                Err(_) => match value.parse::<f64>() {
                    Ok(float) => PrimitiveValue::Float(float),
                    Err(_) => match value.parse::<bool>() {
                        Ok(boolean) => PrimitiveValue::Boolean(boolean),
                        Err(_) => PrimitiveValue::Text(value),
                    },
                },
            })
        }
    }
}

pub fn put_item(command: PutItemCommand, catalog: &mut Catalog) -> anyhow::Result<()> {
    // check if table name is valid
    match catalog.get_table(&command.table_name) {
        None => bail!("Table name '{}' doesn't exist.", command.table_name),
        Some(table) => {
            // check if primary key is present in payload
            let key = match command.item.get(&table.primary_key) {
                None => bail!(
                    "Item object must contain primary key: {}.",
                    table.primary_key
                ),
                Some(primary_key_value) => primary_key_value.to_storage_format(),
            };
            // check to see if this primary key already exists
            let cmd = GetItemCommand {
                table_name: command.table_name.clone(),
                key: key.clone(),
            };
            if get_item(cmd, catalog, false)?.is_some() {
                bail!("ERROR: Item with primary key '{}' already exists.", key);
            }

            // check if item data is valid
            for (column_name, value) in &command.item {
                match table.get_column(column_name) {
                    None => bail!("Unknown column in item object: {}.", column_name),
                    Some(column) => typecheck_column(column, value)?,
                }
            }
            insert_into_table(key, command.item, &command.table_name, catalog)?;
        }
    }
    dbg!(&catalog.get_table(&command.table_name).unwrap().index);
    Ok(())
}

fn typecheck_column(column: &ColumnDefinition, value: &PrimitiveValue) -> anyhow::Result<()> {
    match (&column.r#type, value) {
        (ColumnType::Boolean, PrimitiveValue::Boolean(_)) => (),
        (ColumnType::Integer, PrimitiveValue::Integer(_)) => (),
        (ColumnType::Float, PrimitiveValue::Float(_)) => (),
        (ColumnType::Text, PrimitiveValue::Text(_)) => (),
        (col_type, val_type) => bail!(
            "Column type mismatch. Column defined as type: {}, but provided value has type: {}.",
            col_type,
            val_type
        ),
    }
    Ok(())
}

fn insert_into_table(
    key: String,
    item: Item,
    table_name: &TableName,
    catalog: &mut Catalog,
) -> anyhow::Result<()> {
    let table_path = catalog.get_table_path(table_name);
    if !table_path.exists() {
        bail!(
            "FATAL: Internal Error: Table filepath does not exist: {}",
            table_path.display()
        );
    }
    match catalog.get_table_mut(table_name) {
        None => bail!(
            "FATAL: Internal Error: Expected table {} to be present",
            table_name
        ),
        Some(table) => {
            let tuple = item_to_tuple(item, &table.columns);
            table.block.write(tuple)?;
            table.index.insert(key, table.cursor);
            table.cursor += 1;
        }
    }
    Ok(())
}

/// Convert the values given in an 'Item' to the storage format; consults the
/// `ColumnDefinition`s to serialize appropriately.
fn item_to_tuple(mut item: Item, columns: &[ColumnDefinition]) -> Tuple {
    let mut values = vec![];
    for column in columns {
        let value = item.remove(&column.name);
        values.push(value);
    }
    values
}
