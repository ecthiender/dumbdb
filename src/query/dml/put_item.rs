use std::collections::HashMap;

use anyhow::bail;
use serde::{Deserialize, Serialize};

use crate::query::types::{ColumnDefinition, ColumnName, ColumnType, ColumnValue, TableName};
use crate::storage::Tuple;
use crate::{catalog::Catalog, GetItemCommand};

use super::get_item;

#[derive(Debug, Serialize, Deserialize)]
pub struct PutItemCommand {
    pub table_name: TableName,
    pub item: Item,
}

pub type Item = HashMap<ColumnName, ColumnValue>;

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
                Some(primary_key_value) => primary_key_value.to_string(),
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
    Ok(())
}

fn typecheck_column(column: &ColumnDefinition, value: &ColumnValue) -> anyhow::Result<()> {
    match (&column.r#type, value) {
        (ColumnType::Boolean, ColumnValue::Boolean(_)) => (),
        (ColumnType::Integer, ColumnValue::Integer(_)) => (),
        (ColumnType::Float, ColumnValue::Float(_)) => (),
        (ColumnType::Text, ColumnValue::Text(_)) => (),
        (col_type, val_type) => bail!(
            "Column type mismatch. Column defined as type: {}, but provided value has type: {}.",
            col_type,
            val_type.to_string(),
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
