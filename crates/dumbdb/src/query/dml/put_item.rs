use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::catalog::Catalog;
use crate::query::error::QueryError;
use crate::query::types::{ColumnDefinition, ColumnName, ColumnType, ColumnValue, TableName};
use crate::storage::Tuple;

#[derive(Debug, Serialize, Deserialize)]
pub struct PutItemCommand {
    pub table_name: TableName,
    pub item: Item,
}

pub type Item = HashMap<ColumnName, ColumnValue>;

pub async fn put_item(command: PutItemCommand, catalog: &mut Catalog) -> Result<(), QueryError> {
    // check if table name is valid
    match catalog.get_table_mut(&command.table_name) {
        None => return Err(QueryError::TableNotFound(command.table_name)),
        Some(table) => {
            // check if primary key is present in payload
            let key = match command.item.get(&table.primary_key) {
                None => {
                    return Err(QueryError::ItemMustContainPrimaryKey(
                        table.primary_key.clone(),
                    ))
                }
                // we need a copy of the key to store in the index, along with
                // the tuple being stored on disk. hence, the clone.
                Some(primary_key_value) => primary_key_value.clone(),
            };
            // check to see if this primary key already exists
            if table.table_buffer.contains_key(&key) {
                return Err(QueryError::PrimaryKeyAlreadyExists(key));
            }
            // check if item data is valid
            for (column_name, value) in &command.item {
                match table.get_column(column_name) {
                    None => return Err(QueryError::UnknownColumnInItem(column_name.clone())),
                    Some(column) => typecheck_column(column, value)?,
                }
            }
            // finally write the data
            let tuple = item_to_tuple(command.item, &table.columns);
            table.table_buffer.write(key, tuple).await?;
        }
    }
    Ok(())
}

fn typecheck_column(column: &ColumnDefinition, value: &ColumnValue) -> Result<(), QueryError> {
    match (&column.r#type, value) {
        (ColumnType::Boolean, ColumnValue::Boolean(_)) => (),
        (ColumnType::Integer, ColumnValue::Integer(_)) => (),
        // (ColumnType::Float, ColumnValue::Float(_)) => (),
        (ColumnType::Text, ColumnValue::Text(_)) => (),
        (col_type, col_val) => {
            return Err(QueryError::ColumnTypeMismatch {
                expected: col_type.clone(),
                given: col_val.to_type(),
            })
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
