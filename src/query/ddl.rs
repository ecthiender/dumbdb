use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use tokio::fs::{remove_file, File};

use crate::{catalog::Catalog, TableName};

use super::types::TableDefinition;

/// creates a table in the catalog and also on the disk
pub async fn create_table(table: TableDefinition, catalog: &mut Catalog) -> anyhow::Result<()> {
    if catalog.get_table(&table.name).is_some() {
        bail!("Table name '{}' already exists", table.name);
    }
    create_table_on_disk(&table, catalog).await?;
    catalog.add_table(table).await?;
    Ok(())
}

async fn create_table_on_disk(table: &TableDefinition, catalog: &Catalog) -> anyhow::Result<()> {
    let table_path = catalog.get_table_path(&table.name);
    if table_path.exists() {
        bail!(
            "FATAL: Internal Error: Table filepath {} already exists.",
            table_path.display()
        );
    }

    File::create(&table_path).await.with_context(|| {
        format!(
            "FATAL: Internal Error: Failed to create table file path: {}",
            table_path.clone().display()
        )
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
) -> anyhow::Result<()> {
    if catalog.get_table(&table_name).is_none() {
        bail!("No table '{}' exists", table_name);
    }
    drop_table_from_disk(&table_name, catalog).await?;
    catalog.drop_table(table_name).await?;
    Ok(())
}

async fn drop_table_from_disk(table_name: &TableName, catalog: &Catalog) -> anyhow::Result<()> {
    let table_path = catalog.get_table_path(table_name);
    if table_path.exists() {
        remove_file(&table_path).await.with_context(|| {
            format!(
                "FATAL: Internal Error: Error removing file {}",
                table_path.display()
            )
        })?;
    }
    Ok(())
}
