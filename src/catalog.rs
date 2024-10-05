use std::{
    fs::File,
    io::{BufReader, BufWriter},
    path::PathBuf,
};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};

use crate::query::ddl::CreateTableCommand;

/// Internal metadata of what tables are there, their schema etc.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Catalog {
    pub(crate) directory_path: PathBuf,
    pub(crate) catalog_path: PathBuf,
    pub(crate) tables: Vec<CreateTableCommand>,
}

impl Catalog {
    pub(crate) fn new(dir_path: PathBuf) -> anyhow::Result<Self> {
        if !dir_path.exists() {
            bail!("Database directory '{}' doesn't exist.", dir_path.display())
        }

        let catalog_path = dir_path.join("catalog");
        if catalog_path.exists() {
            let contents: Catalog = read_json_file(&catalog_path)?;
            Ok(contents)
        } else {
            Ok(Self {
                directory_path: dir_path,
                catalog_path,
                tables: vec![],
            })
        }
    }

    pub(crate) fn flush(&self) -> anyhow::Result<()> {
        write_json_file(&self.catalog_path, self)
    }

    pub(crate) fn get_table(&self, name: &str) -> Option<&CreateTableCommand> {
        self.tables.iter().find(|&table| table.name == name)
    }
}

// helpers
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
