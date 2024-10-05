use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{BufReader, BufWriter},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};

use crate::{query::ddl::CreateTableCommand, ColumnDefinition};

/// Internal metadata of what tables are there, their schema etc.
#[derive(Debug, Clone)]
pub(crate) struct Catalog {
    directory_path: PathBuf,
    catalog_path: PathBuf,
    tables: Vec<Table>,
}

#[derive(Debug, Clone)]
pub(crate) struct Table {
    pub(crate) name: String,
    pub(crate) columns: Vec<ColumnDefinition>,
    pub(crate) primary_key: String,
    pub(crate) file_write_handle: Arc<File>,
    pub(crate) index: BTreeMap<String, usize>,
    pub(crate) cursor: usize,
}

impl Table {
    pub fn new(
        table_definition: CreateTableCommand,
        directory_path: &Path,
    ) -> anyhow::Result<Self> {
        let table_path = get_table_path_(directory_path, &table_definition.name);

        let (index, cursor) = Self::build_index(&table_path, &table_definition)?;

        let write_file = std::fs::OpenOptions::new()
            .append(true)
            .open(table_path)
            .with_context(|| "Could not open file for writing.")?;

        Ok(Self {
            name: table_definition.name,
            columns: table_definition.columns,
            primary_key: table_definition.primary_key,
            file_write_handle: Arc::new(write_file),
            index,
            cursor,
        })
    }

    fn build_index(
        table_path: &Path,
        table: &CreateTableCommand,
    ) -> anyhow::Result<(BTreeMap<String, usize>, usize)> {
        let key_position = table
            .columns
            .iter()
            .position(|col_def| col_def.name == table.primary_key)
            .with_context(|| "Internal Error: primary key must exist.")?;

        let contents = fs::read_to_string(table_path)?;
        let mut index = BTreeMap::new();
        for (row_pos, line) in contents.lines().enumerate() {
            let parts: Vec<_> = line.split(",").collect();
            let index_key = parts[key_position];
            index.insert(index_key.to_string(), row_pos);
        }
        Ok((index, contents.len()))
    }

    pub fn get_column(&self, name: &str) -> Option<&ColumnDefinition> {
        self.columns.iter().find(|col| col.name == name)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct SerializableCatalog {
    tables: Vec<CreateTableCommand>,
}

impl<'a> From<&'a Table> for CreateTableCommand {
    fn from(table: &'a Table) -> Self {
        Self {
            name: table.name.clone(),
            columns: table.columns.clone(),
            primary_key: table.primary_key.clone(),
        }
    }
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

    pub(crate) fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.iter().find(|&table| table.name == name)
    }

    pub(crate) fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        self.tables.iter_mut().find(|table| table.name == name)
    }

    pub(crate) fn get_table_path(&self, table_name: &str) -> PathBuf {
        get_table_path_(&self.directory_path, table_name)
    }

    pub(crate) fn add_table(&mut self, table_def: CreateTableCommand) -> anyhow::Result<()> {
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

// helpers
fn get_table_path_(directory_path: &Path, table_name: &str) -> PathBuf {
    let table_rel_path = PathBuf::from(format!("{}.tbl", table_name));
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
