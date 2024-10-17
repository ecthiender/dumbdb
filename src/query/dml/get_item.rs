use anyhow::bail;
use serde::{Deserialize, Serialize};

use crate::{
    catalog::Catalog,
    query::types::{ColumnValue, TableName},
};

use super::common::{parse_record, Record};

#[derive(Debug, Serialize, Deserialize)]
pub struct GetItemCommand {
    pub table_name: TableName,
    pub key: ColumnValue,
}

pub async fn get_item(
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
                .get(command.key, scan_file)
                .await?
                .map(|item| parse_record(&table.columns, item))
                .transpose()?;

            Ok(record)
        }
    }
}
