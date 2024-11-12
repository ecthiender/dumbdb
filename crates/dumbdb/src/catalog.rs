use std::{
    fs::File,
    io::{BufReader, BufWriter},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{
    query::types::{ColumnDefinition, ColumnName, TableDefinition, TableName},
    table::{self, TableBuffer, TableBufferError},
};

const CATALOG_FILE_NAME: &str = "catalog.json";

/// Internal metadata of what tables are there, their schema etc. that we can
/// serialize to disk.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct SerializableCatalog {
    tables: Vec<TableDefinition>,
}

/// Internal metadata of what tables are there, their schema etc., that we keep
/// in memory. Contains computed in-memory state like indexes.
#[derive(Debug, Clone)]
pub(crate) struct Catalog {
    directory_path: PathBuf,
    catalog_path: PathBuf,
    tables: Vec<Table>,
}

#[derive(thiserror::Error, Debug)]
pub enum CatalogError {
    #[error("Database directory '{0}' does not exist.")]
    DbDirNotExist(PathBuf),
    #[error("Internal Error: {0}")]
    TableStorageError(#[from] TableBufferError),
    #[error("Internal Error: {0}")]
    FileOperationError(#[from] std::io::Error),
    #[error("Internal Error: {0}")]
    DeserError(#[from] serde_json::Error),
}

impl Catalog {
    pub(crate) async fn new(dir_path: PathBuf) -> Result<Self, CatalogError> {
        if !dir_path.exists() {
            return Err(CatalogError::DbDirNotExist(dir_path));
        }
        let catalog_path = dir_path.join(CATALOG_FILE_NAME);
        let stored_catalog: SerializableCatalog = if catalog_path.exists() {
            read_json_file(&catalog_path)?
        } else {
            SerializableCatalog { tables: vec![] }
        };
        let mut tables = vec![];
        for table in stored_catalog.tables {
            tables.push(Table::new(table, &dir_path).await?);
        }
        Ok(Self {
            catalog_path,
            directory_path: dir_path,
            tables,
        })
    }

    pub(crate) fn list_tables(&self) -> Vec<TableName> {
        self.tables.iter().map(|t| t.name.clone()).collect()
    }

    pub(crate) fn get_table(&self, name: &TableName) -> Option<&Table> {
        self.tables.iter().find(|&table| table.name == *name)
    }

    pub(crate) fn get_table_mut(&mut self, name: &TableName) -> Option<&mut Table> {
        self.tables.iter_mut().find(|table| table.name == *name)
    }

    pub(crate) fn get_table_path(&self, table_name: &TableName) -> PathBuf {
        table::get_table_path_(&self.directory_path, table_name)
    }

    pub(crate) async fn add_table(
        &mut self,
        table_def: TableDefinition,
    ) -> Result<(), CatalogError> {
        let table = Table::new(table_def, &self.directory_path).await?;
        self.tables.push(table);
        self.flush()?;
        Ok(())
    }

    pub(crate) async fn drop_table(&mut self, table_name: TableName) -> Result<(), CatalogError> {
        self.tables.retain(|t| t.name != table_name);
        self.flush()?;
        Ok(())
    }

    pub(crate) fn get_table_size(&self, name: &TableName) -> Option<usize> {
        self.get_table(name).map(|table| table.table_buffer.size())
    }

    fn flush(&self) -> Result<(), CatalogError> {
        let stored_catalog = SerializableCatalog {
            tables: self.tables.iter().map(Into::into).collect(),
        };
        write_json_file(&self.catalog_path, &stored_catalog)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Table {
    pub(crate) name: TableName,
    pub(crate) columns: Vec<ColumnDefinition>,
    pub(crate) primary_key: ColumnName,
    pub(crate) table_buffer: TableBuffer,
}

impl Table {
    pub async fn new(
        table_definition: TableDefinition,
        directory_path: &Path,
    ) -> Result<Self, CatalogError> {
        let table_buffer = TableBuffer::new(&table_definition, directory_path).await?;

        let table = Self {
            name: table_definition.name,
            columns: table_definition.columns,
            primary_key: table_definition.primary_key,
            table_buffer,
        };
        Ok(table)
    }

    pub fn get_column(&self, name: &ColumnName) -> Option<&ColumnDefinition> {
        self.columns.iter().find(|col| col.name == *name)
    }
}

impl<'a> From<&'a Table> for TableDefinition {
    fn from(table: &'a Table) -> Self {
        Self {
            name: table.name.clone(),
            columns: table.columns.clone(),
            primary_key: table.primary_key.clone(),
        }
    }
}

// helpers
fn read_json_file<T: for<'a> Deserialize<'a>>(file_path: &PathBuf) -> Result<T, CatalogError> {
    // Open the file in read-only mode.
    let file = File::open(file_path)?;
    // Create a buffered reader for more efficient file reading.
    let reader = BufReader::new(file);
    Ok(serde_json::from_reader(reader)?)
}

fn write_json_file<T: Serialize>(file_path: &PathBuf, item: &T) -> Result<(), CatalogError> {
    // Open the file in write-only mode, create it if it doesn't exist.
    let file = File::create(file_path)?;
    // Create a buffered writer for efficient file writing.
    let writer = BufWriter::new(file);
    // Serialize the item struct to JSON and write it to the file.
    serde_json::to_writer(writer, &item)?;
    Ok(())
}
