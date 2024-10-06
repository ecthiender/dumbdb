use std::{
    collections::HashMap,
    fs::{self},
    path::Path,
};

use anyhow::bail;
use serde::{Deserialize, Serialize};

use super::put_item::PrimitiveValue;
use crate::{
    catalog::{Catalog, Table, TableName},
    query::ddl::{ColumnDefinition, ColumnName},
};

#[derive(Debug, Serialize, Deserialize)]
pub struct GetItemCommand {
    pub table_name: TableName,
    pub key: String,
}

pub type Record = HashMap<ColumnName, Option<PrimitiveValue>>;

pub fn get_item(
    command: GetItemCommand,
    catalog: &Catalog,
    scan_file: bool,
) -> anyhow::Result<Option<Record>> {
    match catalog.get_table(&command.table_name) {
        None => bail!("Table name '{}' doesn't exist.", command.table_name),
        Some(table) => {
            let key_position = table.pk_position();
            let table_path = catalog.get_table_path(&command.table_name);
            if !table_path.exists() {
                bail!(
                    "FATAL: Internal Error: Table filepath does not exist: {}",
                    table_path.display()
                );
            }

            // read from the index; get the cursor
            if let Some(cursor) = table.index.get(&command.key) {
                // read from the file
                let contents = fs::read_to_string(table_path)?;
                match contents.lines().nth(*cursor) {
                    None => bail!("ERROR: Internal Error: Could not find item with primary key."),
                    Some(line) => {
                        let record = parse_record(&table.columns, line)?;
                        Ok(Some(record))
                    }
                }
            // if not found in the index
            } else {
                // The index is our main lookup structure; one invariant is all
                // primary keys are available in the index. Hence if it's not
                // found in the index; the key doesn't exist. return None
                //
                // But.. software bugs are pesky and sometimes hard to predict.
                // Maybe there's an edge case where the index doesn't have the
                // key, but the file has it. For those cases, if scan_file flag
                // is passed then we rescan the entire file.
                if scan_file {
                    Ok(scan_entire_file_get_item(
                        &table_path,
                        command,
                        key_position,
                        table,
                    )?)
                } else {
                    Ok(None)
                }
            }
        }
    }
}

fn scan_entire_file_get_item(
    table_path: &Path,
    command: GetItemCommand,
    key_position: usize,
    table: &Table,
) -> anyhow::Result<Option<Record>> {
    for line in fs::read_to_string(table_path)?.lines() {
        let parts: Vec<_> = line.split(",").collect();
        if parts[key_position] == command.key {
            let record = parse_record(&table.columns, line)?;
            return Ok(Some(record));
        }
    }
    Ok(None)
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
