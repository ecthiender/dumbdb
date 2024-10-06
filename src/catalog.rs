use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufReader, BufWriter},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use crate::query::{
    ddl::{ColumnDefinition, ColumnName, CreateTableCommand},
    dml::get_item::read_from_file,
};
use crate::storage::{deserialize_binary, Tuple};

/// Internal metadata of what tables are there, their schema etc. that we can
/// serialize to disk.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct SerializableCatalog {
    tables: Vec<CreateTableCommand>,
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

#[derive(Debug, Clone)]
pub(crate) struct Table {
    pub(crate) name: TableName,
    pub(crate) columns: Vec<ColumnDefinition>,
    pub(crate) primary_key: ColumnName,
    pub(crate) file_handle: Arc<RwLock<File>>,
    pub(crate) index: BTreeMap<String, usize>,
    pub(crate) cursor: usize,
    primary_key_position: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, derive_more::Display)]
#[serde(into = "String")]
#[serde(from = "String")]
pub struct TableName(pub SmolStr);

impl From<&str> for TableName {
    fn from(value: &str) -> Self {
        TableName::new(value)
    }
}

impl From<String> for TableName {
    fn from(value: String) -> Self {
        TableName::new(&value)
    }
}

impl From<TableName> for String {
    fn from(val: TableName) -> Self {
        val.0.to_string()
    }
}

impl TableName {
    pub fn new(value: &str) -> Self {
        Self(SmolStr::new(value))
    }
}

impl Table {
    pub fn new(
        table_definition: CreateTableCommand,
        directory_path: &Path,
    ) -> anyhow::Result<Self> {
        let table_path = get_table_path_(directory_path, &table_definition.name);

        let write_file = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .open(&table_path)
            .with_context(|| "Could not open file for writing.")?;

        let key_position = table_definition
            .columns
            .iter()
            .position(|col_def| col_def.name == table_definition.primary_key)
            .with_context(|| "Internal Error: primary key must exist.")?;

        let mut table = Self {
            name: table_definition.name,
            columns: table_definition.columns,
            primary_key: table_definition.primary_key,
            primary_key_position: key_position,
            file_handle: Arc::new(RwLock::new(write_file)),
            index: BTreeMap::new(),
            cursor: 0,
        };

        table.build_index(&table_path)?;
        Ok(table)
    }

    fn build_index(&mut self, table_path: &Path) -> anyhow::Result<()> {
        let key_position = self.pk_position();
        let mut index = BTreeMap::new();
        // read from the file
        let reader = read_from_file(table_path)?;
        for (row_pos, line) in reader.enumerate() {
            let line = line?;
            let tuple: Tuple = deserialize_binary(line)?;
            let index_key = tuple[key_position]
                .clone()
                .with_context(|| "invariant violation: primary key value not found in tuple.")?;
            index.insert(index_key.to_storage_format(), row_pos);
            self.cursor = row_pos;
        }
        self.index = index;
        dbg!(&self.index);
        Ok(())
    }

    pub fn get_column(&self, name: &ColumnName) -> Option<&ColumnDefinition> {
        self.columns.iter().find(|col| col.name == *name)
    }

    pub fn pk_position(&self) -> usize {
        self.primary_key_position
    }
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
