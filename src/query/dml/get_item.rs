use serde::{Deserialize, Serialize};

use crate::{
    catalog::Catalog,
    query::{
        error::QueryError,
        types::{ColumnValue, TableName},
    },
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
) -> Result<Option<Record>, QueryError> {
    match catalog.get_table(&command.table_name) {
        None => Err(QueryError::TableNotFound(command.table_name)),
        Some(table) => {
            let record = table
                .table_buffer
                .get(command.key, scan_file)
                .await?
                .map(|item| parse_record(&table.columns, item));

            Ok(record)
        }
    }
}
