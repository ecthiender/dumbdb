use std::{collections::HashMap, fs::File, io::BufReader, path::Path};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};

use super::put_item::PrimitiveValue;
use crate::{
    byte_lines::ByteLines,
    catalog::{Catalog, Table, TableName},
    query::ddl::{ColumnDefinition, ColumnName},
    storage::{deserialize_binary, Tuple},
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

            dbg!(&table.index);
            // read from the index; get the cursor
            dbg!(&command.key);
            if let Some(cursor) = table.index.get(&command.key) {
                // read from the file
                let mut reader = read_from_file(&table_path)?;
                match reader.nth(*cursor) {
                    None => bail!("ERROR: Internal Error: Could not find item with primary key."),
                    Some(line) => {
                        let line = line?;
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
    // read from the file
    let reader = read_from_file(table_path)?;
    for line in reader.into_iter() {
        let line = line?;
        let tuple: Tuple = deserialize_binary(line)?;
        let key = tuple[key_position]
            .clone()
            .with_context(|| "invariant violation: primary key value not found in tuple.")?;
        if key.to_storage_format() == command.key {
            let record = parse_record_from_tuple(&table.columns, tuple)?;
            return Ok(Some(record));
        }
    }
    Ok(None)
}

pub fn read_from_file(table_path: &Path) -> anyhow::Result<ByteLines<BufReader<File>>> {
    // read from the file
    let file_handle = File::open(table_path)?;
    let reader = BufReader::new(file_handle);
    let bytelines = ByteLines::new(reader);
    Ok(bytelines)
}

fn parse_record(columns: &[ColumnDefinition], item: Vec<u8>) -> anyhow::Result<Record> {
    let result: Tuple = deserialize_binary(item)?;
    parse_record_from_tuple(columns, result)
}

fn parse_record_from_tuple(columns: &[ColumnDefinition], item: Tuple) -> anyhow::Result<Record> {
    let mut record = HashMap::new();
    for (idx, value) in item.into_iter().enumerate() {
        let col_name = columns[idx].name.clone();
        record.insert(col_name, value);
    }
    Ok(record)
}
