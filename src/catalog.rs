use std::{
    fs::File,
    io::{BufReader, BufWriter},
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};

use crate::{
    query::types::{ColumnDefinition, ColumnName, TableDefinition, TableName},
    table::TableBuffer,
};

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

impl Catalog {
    pub(crate) fn new(dir_path: PathBuf) -> anyhow::Result<Self> {
        if !dir_path.exists() {
            bail!("Database directory '{}' doesn't exist.", dir_path.display())
        }

        let catalog_path = dir_path.join("catalog");
        let stored_catalog: SerializableCatalog = if catalog_path.exists() {
            read_json_file(&catalog_path)?
        } else {
            SerializableCatalog { tables: vec![] }
        };
        let mut tables = vec![];
        for table in stored_catalog.tables {
            tables.push(Table::new(table, &dir_path)?);
        }
        Ok(Self {
            catalog_path,
            directory_path: dir_path,
            tables,
        })
    }

    pub(crate) fn get_table(&self, name: &TableName) -> Option<&Table> {
        self.tables.iter().find(|&table| table.name == *name)
    }

    pub(crate) fn get_table_mut(&mut self, name: &TableName) -> Option<&mut Table> {
        self.tables.iter_mut().find(|table| table.name == *name)
    }

    pub(crate) fn get_table_path(&self, table_name: &TableName) -> PathBuf {
        get_table_path_(&self.directory_path, table_name)
    }

    pub(crate) fn add_table(&mut self, table_def: TableDefinition) -> anyhow::Result<()> {
        let table = Table::new(table_def, &self.directory_path)?;
        self.tables.push(table);
        self.flush()?;
        Ok(())
    }

    fn flush(&self) -> anyhow::Result<()> {
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
    pub fn new(table_definition: TableDefinition, directory_path: &Path) -> anyhow::Result<Self> {
        let table_buffer = TableBuffer::new(&table_definition, directory_path)?;

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
fn get_table_path_(directory_path: &Path, table_name: &TableName) -> PathBuf {
    let table_rel_path = PathBuf::from(format!("{}.tbl", table_name.0.as_str()));
    directory_path.join(table_rel_path)
}

fn read_json_file<T: for<'a> Deserialize<'a>>(file_path: &PathBuf) -> anyhow::Result<T> {
    // Open the file in read-only mode.
    let file = File::open(file_path).with_context(|| {
        format!(
            "read_json_file: Unable to open file: {}",
            file_path.display()
        )
    })?;
    // Create a buffered reader for more efficient file reading.
    let reader = BufReader::new(file);
    serde_json::from_reader(reader).with_context(|| "Unable to parse JSON")
}

fn write_json_file<T: Serialize>(file_path: &PathBuf, item: &T) -> anyhow::Result<()> {
    // Open the file in write-only mode, create it if it doesn't exist.
    let file = File::create(file_path).with_context(|| {
        format!(
            "write_json_file: Unable to create file: {}",
            file_path.display()
        )
    })?;
    // Create a buffered writer for efficient file writing.
    let writer = BufWriter::new(file);
    // Serialize the item struct to JSON and write it to the file.
    serde_json::to_writer(writer, &item)?;
    Ok(())
}
