use std::{
    collections::HashMap,
    fs::{self},
};

use anyhow::bail;
use serde::{Deserialize, Serialize};

use super::put_item::PrimitiveValue;
use crate::{catalog::Catalog, query::ddl::ColumnDefinition};

#[derive(Debug, Serialize, Deserialize)]
pub struct GetItemCommand {
    pub table_name: String,
    pub key: String,
}

pub type Record = HashMap<String, Option<PrimitiveValue>>;

pub fn get_item(command: GetItemCommand, catalog: &Catalog) -> anyhow::Result<Record> {
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
                        return Ok(record);
                    }
                }
            // scan the entire file
            } else {
                for line in fs::read_to_string(table_path)?.lines() {
                    let parts: Vec<_> = line.split(",").collect();
                    if parts[key_position] == command.key {
                        let record = parse_record(&table.columns, line)?;
                        return Ok(record);
                    }
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
