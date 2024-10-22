use serde::{Deserialize, Serialize};
use tokio::fs::{remove_file, File};

use crate::{catalog::Catalog, TableName};

use super::{
    error::{InternalError, QueryError},
    types::TableDefinition,
};

/// creates a table in the catalog and also on the disk
pub async fn create_table(table: TableDefinition, catalog: &mut Catalog) -> Result<(), QueryError> {
    if catalog.get_table(&table.name).is_some() {
        return Err(QueryError::TableAlreadyExists(table.name));
    }
    create_table_on_disk(&table, catalog).await?;
    catalog.add_table(table).await?;
    Ok(())
}

async fn create_table_on_disk(
    table: &TableDefinition,
    catalog: &Catalog,
) -> Result<(), QueryError> {
    let table_path = catalog.get_table_path(&table.name);
    if table_path.exists() {
        return Err(QueryError::InternalError(
            InternalError::FilepathAlreadyExists(table_path),
        ));
    }

    File::create(&table_path).await.map_err(|e| {
        QueryError::InternalError(InternalError::FailedToCreateFile {
            filepath: table_path,
            error: e,
        })
    })?;

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DropTableCommand {
    pub table_name: TableName,
}

/// drops a table from the catalog and also on the disk
pub async fn drop_table(
    DropTableCommand { table_name }: DropTableCommand,
    catalog: &mut Catalog,
) -> Result<(), QueryError> {
    if catalog.get_table(&table_name).is_none() {
        return Err(QueryError::TableNotFound(table_name));
    }
    drop_table_from_disk(&table_name, catalog).await?;
    catalog.drop_table(table_name).await?;
    Ok(())
}

async fn drop_table_from_disk(table_name: &TableName, catalog: &Catalog) -> Result<(), QueryError> {
    let table_path = catalog.get_table_path(table_name);
    if table_path.exists() {
        remove_file(&table_path).await.map_err(|e| {
            QueryError::InternalError(InternalError::FailedToDeleteFile {
                filepath: table_path,
                error: e,
            })
        })?;
    }
    Ok(())
}
