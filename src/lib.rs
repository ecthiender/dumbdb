use std::{
    collections::HashMap,
    fmt::Display,
    fs::{self, File},
    io::{BufReader, BufWriter, Write},
    path::PathBuf,
};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTableCommand {
    pub name: String,
    pub columns: Vec<ColumnDefinition>,
    pub primary_key: String,
}

impl CreateTableCommand {
    pub fn get_column(&self, name: &str) -> Option<&ColumnDefinition> {
        self.columns.iter().find(|col| col.name == name)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub r#type: ColumnType,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ColumnType {
    Integer,
    Float,
    Text,
    Boolean,
}

impl Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Integer => write!(f, "Integer"),
            Self::Float => write!(f, "Float"),
            Self::Boolean => write!(f, "Boolean"),
            Self::Text => write!(f, "Text"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PutItemCommand {
    pub table_name: String,
    pub item: Record,
}

type Record = HashMap<String, PrimitiveValue>;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum PrimitiveValue {
    Integer(u64),
    Float(f64),
    Boolean(bool),
    Text(String),
}

impl Display for PrimitiveValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Integer(val) => write!(f, "Integer({})", val),
            Self::Float(val) => write!(f, "Float({})", val),
            Self::Boolean(val) => write!(f, "Boolean({})", val),
            Self::Text(val) => write!(f, "Text({})", val),
        }
    }
}

impl PrimitiveValue {
    fn to_value(self) -> String {
        match self {
            Self::Integer(val) => val.to_string(),
            Self::Float(val) => val.to_string(),
            Self::Boolean(val) => val.to_string(),
            Self::Text(val) => val.to_string(),
        }
    }
    fn from_string(value: String) -> Self {
        match value.parse::<u64>() {
            Ok(int) => PrimitiveValue::Integer(int),
            Err(_) => match value.parse::<f64>() {
                Ok(float) => PrimitiveValue::Float(float),
                Err(_) => match value.parse::<bool>() {
                    Ok(boolean) => PrimitiveValue::Boolean(boolean),
                    Err(_) => PrimitiveValue::Text(value),
                },
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetItemCommand {
    pub table_name: String,
    pub key: String,
}

/// Internal metadata of what tables are there, their schema etc.
#[derive(Debug, Serialize, Deserialize)]
pub struct Catalog {
    pub directory_path: PathBuf,
    pub catalog_path: PathBuf,
    pub tables: Vec<CreateTableCommand>,
}

impl Catalog {
    pub fn new(dir_path: PathBuf) -> anyhow::Result<Self> {
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

    pub fn flush(&self) -> anyhow::Result<()> {
        write_json_file(&self.catalog_path, self)
    }

    pub fn get_table(&self, name: &str) -> Option<&CreateTableCommand> {
        for table in &self.tables {
            if table.name == name {
                return Some(table);
            }
        }
        None
    }
}

pub fn get_item(command: GetItemCommand, catalog: &Catalog) -> anyhow::Result<Record> {
    match catalog.get_table(&command.table_name) {
        None => bail!("Table name '{}' doesn't exist.", command.table_name),
        Some(table) => {
            let key_position = table
                .columns
                .iter()
                .position(|col_def| col_def.name == table.primary_key)
                .with_context(|| "Internal Error: primary key must exist.")?;
            let table_rel_path = PathBuf::from(format!("{}.tbl", command.table_name));
            let table_path = catalog.directory_path.join(table_rel_path);
            if !table_path.exists() {
                bail!(
                    "FATAL: Internal Error: Table filepath does not exist: {}",
                    table_path.display()
                );
            }
            for line in fs::read_to_string(table_path)?.lines() {
                let parts: Vec<_> = line.split(",").collect();
                if parts[key_position] == command.key {
                    let record = parse_record(&table.columns, line)?;
                    return Ok(record);
                }
            }
            bail!("ERROR: Internal Error: Could not find item with primary key.");
        }
    }
}

fn parse_record(columns: &Vec<ColumnDefinition>, item: &str) -> anyhow::Result<Record> {
    let mut result = HashMap::new();
    for (idx, part) in item.split(",").enumerate() {
        let col_name = columns[idx].name.clone();
        let val = PrimitiveValue::from_string(part.to_string());
        result.insert(col_name, val);
    }
    Ok(result)
}

pub fn put_item(command: PutItemCommand, catalog: &Catalog) -> anyhow::Result<()> {
    // check if table name is valid
    match catalog.get_table(&command.table_name) {
        None => bail!("Table name '{}' doesn't exist.", command.table_name),
        Some(table) => {
            // check if item data is valid
            for (column_name, value) in &command.item {
                match table.get_column(&column_name) {
                    None => bail!("Unknown column in item object: {}.", column_name),
                    Some(column) => {
                        match (&column.r#type, value) {
                            (ColumnType::Boolean, PrimitiveValue::Boolean(_)) => (),
                            (ColumnType::Integer, PrimitiveValue::Integer(_)) => (),
                            (ColumnType::Float, PrimitiveValue::Float(_)) => (),
                            (ColumnType::Text, PrimitiveValue::Text(_)) => (),
                            (col_type,val_type) => bail!("Column type mismatch. Column defined as type: {}, but provided value has type: {}.", col_type, val_type),
                        }
                    }
                }
            }
            insert_into_table(command.item, &command.table_name, catalog)?;
        }
    }
    Ok(())
}

fn insert_into_table(mut item: Record, table_name: &str, catalog: &Catalog) -> anyhow::Result<()> {
    let table_rel_path = PathBuf::from(format!("{}.tbl", table_name));
    let table_path = catalog.directory_path.join(table_rel_path);
    if !table_path.exists() {
        bail!(
            "FATAL: Internal Error: Table filepath does not exist: {}",
            table_path.display()
        );
    }

    match catalog.get_table(&table_name) {
        None => bail!(
            "FATAL: Internal Error: Expected table {} to be present",
            table_name
        ),
        Some(table) => {
            let mut values = vec![];
            for column in &table.columns {
                let value = item.remove(&column.name);
                match value {
                    None => values.push("NULL".to_string()),
                    Some(val) => values.push(val.to_value()),
                }
            }
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .append(true)
                .open(table_path)
                .with_context(|| "Could not open file for writing.")?;

            write_to_file(&mut file, values.join(","))?;
        }
    }
    Ok(())
}

/// creates a table in the catalog and also on the disk
pub fn create_table(table: CreateTableCommand, catalog: &mut Catalog) -> anyhow::Result<()> {
    if catalog.get_table(&table.name).is_some() {
        bail!("Table name '{}' already exists", table.name);
    }
    create_table_on_disk(&table, &catalog)?;
    catalog.tables.push(table);
    catalog.flush()?;
    Ok(())
}

fn create_table_on_disk(table: &CreateTableCommand, catalog: &Catalog) -> anyhow::Result<()> {
    let table_rel_path = PathBuf::from(format!("{}.tbl", table.name.clone()));
    let table_path = catalog.directory_path.join(table_rel_path);
    if table_path.exists() {
        bail!(
            "FATAL: Internal Error: Table filepath {} already exists.",
            table_path.display()
        );
    }

    // prepare the column names in CSV format
    let column_names = table
        .columns
        .iter()
        .map(|col| col.name.clone())
        .collect::<Vec<_>>()
        .join(",");

    let mut file = File::create(&table_path).with_context(|| {
        format!(
            "FATAL: Internal Error: Failed to create table file path: {}",
            table_path.clone().display()
        )
    })?;
    write_to_file(&mut file, column_names).with_context(|| {
        format!(
            "FATAL: Internal Error: Failed to write columns to table file path: {}",
            table_path.clone().display()
        )
    })?;

    Ok(())
}

fn write_to_file(file: &mut File, data: String) -> anyhow::Result<()> {
    writeln!(file, "{}", data)
        .with_context(|| "FATAL: Internal Error: Failed writing data to file")?;
    let _ = file.flush();
    Ok(())
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
    Ok(serde_json::from_reader(reader).with_context(|| "Unable to parse JSON")?)
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

#[cfg(test)]
mod tests {
    use rand::Rng;
    use serde_json::json;

    use super::*;

    const DB_PATH: &str = "./data/test/dumbdb";

    #[test]
    fn test_create_table() -> anyhow::Result<()> {
        let catalog = setup("create_table")?;
        assert!(catalog.get_table("authors").is_some());
        Ok(())
    }

    #[test]
    fn test_write_data() -> anyhow::Result<()> {
        let mut catalog = setup("write_data")?;
        for i in 0..10 {
            let author_item = create_put_item(i)?;
            put_item(author_item, &mut catalog)?;
        }
        let table_rel_path = PathBuf::from(format!("authors.tbl"));
        let table_path = catalog.directory_path.join(table_rel_path);
        let contents = fs::read_to_string(table_path)?;
        let last_line = contents
            .lines()
            .filter(|x| !x.is_empty())
            .last()
            .with_context(|| "There should be 10 rows written")?;
        assert!(last_line.to_string().starts_with("9,"));
        Ok(())
    }

    #[test]
    fn test_read_data() -> anyhow::Result<()> {
        let mut catalog = setup("read_data")?;
        for i in 0..10 {
            let author_item = create_put_item(i)?;
            put_item(author_item, &mut catalog)?;
        }
        for i in 5..8 {
            let cmd = create_get_item(i)?;
            let record = get_item(cmd, &catalog)?;
            assert_eq!(
                &PrimitiveValue::Integer(i as u64),
                record.get("id").unwrap()
            );
        }
        Ok(())
    }

    fn setup(test_name: &str) -> anyhow::Result<Catalog> {
        let authors_table = json!({
            "name": "authors",
            "columns": [
                {
                    "name": "id",
                    "type": "Integer",
                },
                {
                    "name": "name",
                    "type": "Text",
                }
            ],
            "primary_key": "id"
        });

        let root_test_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        // if test dir already exists; remove it.
        let dir_path = root_test_dir.join(format!("{}_{}", DB_PATH, test_name));
        if dir_path.exists() {
            let _ = fs::remove_dir_all(&dir_path);
        }
        fs::create_dir_all(&dir_path)?;
        let mut catalog = Catalog::new(dir_path)?;
        create_table(serde_json::from_value(authors_table)?, &mut catalog)?;
        Ok(catalog)
    }

    fn create_get_item(id: u64) -> anyhow::Result<GetItemCommand> {
        Ok(serde_json::from_value(json!({
            "table_name": "authors",
            "key": id.to_string(),
        }))?)
    }

    fn create_put_item(id: u64) -> anyhow::Result<PutItemCommand> {
        const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        const STRING_LEN: usize = 10;
        let mut rng = rand::thread_rng();
        let rand_string: String = (0..STRING_LEN)
            .map(|_| {
                let idx = rng.gen_range(0..CHARSET.len());
                CHARSET[idx] as char
            })
            .collect();

        Ok(serde_json::from_value(json!({
            "table_name": "authors",
            "item": {
                "id": id,
                "name": rand_string,
            }
        }))?)
    }
}
