use std::collections::HashMap;

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};

use crate::{
    catalog::{Catalog, Table},
    query::types::{ColumnDefinition, ColumnName, ColumnValue, TableName},
    storage::Tuple,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct GetItemCommand {
    pub table_name: TableName,
    pub key: String,
}

pub type Record = HashMap<ColumnName, Option<ColumnValue>>;

pub fn get_item(
    command: GetItemCommand,
    catalog: &Catalog,
    scan_file: bool,
) -> anyhow::Result<Option<Record>> {
    match catalog.get_table(&command.table_name) {
        None => bail!("Table name '{}' doesn't exist.", command.table_name),
        Some(table) => {
            let table_path = catalog.get_table_path(&command.table_name);
            if !table_path.exists() {
                bail!(
                    "FATAL: Internal Error: Table filepath does not exist: {}",
                    table_path.display()
                );
            }
            // read from the index; get the cursor
            if let Some(offset) = table.index.get(&command.key) {
                let tuple = table.block.seek_to_offset(*offset)?;
                let record = parse_record(&table.columns, tuple)?;
                Ok(Some(record))
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
                    Ok(scan_entire_file_get_item(command, table)?)
                } else {
                    Ok(None)
                }
            }
        }
    }
}

fn scan_entire_file_get_item(
    command: GetItemCommand,
    table: &Table,
) -> anyhow::Result<Option<Record>> {
    let key_position = table.pk_position();
    for tuple in table.block.get_reader()? {
        let tuple = tuple?;
        let key = tuple[key_position]
            .clone()
            .with_context(|| "invariant violation: primary key value not found in tuple.")?;
        if key.to_string() == command.key {
            let record = parse_record(&table.columns, tuple)?;
            return Ok(Some(record));
        }
    }
    Ok(None)
}

fn parse_record(columns: &[ColumnDefinition], item: Tuple) -> anyhow::Result<Record> {
    let mut record = HashMap::new();
    for (idx, value) in item.into_iter().enumerate() {
        let col_name = columns[idx].name.clone();
        record.insert(col_name, value);
    }
    Ok(record)
}
