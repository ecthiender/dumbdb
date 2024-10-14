use std::collections::HashMap;

use anyhow::bail;
use serde::{Deserialize, Serialize};

use crate::{
    catalog::Catalog,
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
            let record = table
                .table_buffer
                .get_item(command.key, scan_file)?
                .map(|item| parse_record(&table.columns, item))
                .transpose()?;

            Ok(record)
        }
    }
}

fn parse_record(columns: &[ColumnDefinition], item: Tuple) -> anyhow::Result<Record> {
    let mut record = HashMap::new();
    for (idx, value) in item.into_iter().enumerate() {
        let col_name = columns[idx].name.clone();
        record.insert(col_name, value);
    }
    Ok(record)
}
