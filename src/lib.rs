pub use query::ddl::CreateTableCommand;
pub use query::dml::{GetItemCommand, PutItemCommand};
use std::path::PathBuf;

use catalog::Catalog;
use query::ddl;
use query::dml;

mod catalog;
mod helper;
mod query;

#[derive(Debug)]
pub struct Database {
    catalog: Catalog,
}

impl Database {
    pub fn new(path: &str) -> anyhow::Result<Self> {
        let catalog = Catalog::new(PathBuf::from(path))?;
        Ok(Self { catalog })
    }

    pub fn create_table(&mut self, table: ddl::CreateTableCommand) -> anyhow::Result<()> {
        ddl::create_table(table, &mut self.catalog)
    }

    pub fn put_item(&self, command: dml::PutItemCommand) -> anyhow::Result<()> {
        dml::put_item(command, &self.catalog)
    }

    pub fn get_item(&self, command: dml::GetItemCommand) -> anyhow::Result<dml::Record> {
        dml::get_item(command, &self.catalog)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use anyhow::Context;
    use rand::Rng;
    use serde_json::json;

    use super::*;

    const DB_PATH: &str = "./data/test/dumbdb";

    #[test]
    fn test_create_table() -> anyhow::Result<()> {
        let db = setup("create_table")?;
        assert!(db.catalog.get_table("authors").is_some());
        Ok(())
    }

    #[test]
    fn test_write_data() -> anyhow::Result<()> {
        let db = setup("write_data")?;
        for i in 0..10 {
            let author_item = create_put_item(i)?;
            db.put_item(author_item)?;
        }
        let table_path = db.catalog.get_table_path("authors");
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
        let db = setup("read_data")?;
        for i in 0..10 {
            let author_item = create_put_item(i)?;
            db.put_item(author_item)?;
        }
        for i in 5..8 {
            let cmd = create_get_item(i)?;
            let record = db.get_item(cmd)?;
            assert_eq!(&dml::PrimitiveValue::Integer(i), record.get("id").unwrap());
        }
        Ok(())
    }

    fn setup(test_name: &str) -> anyhow::Result<Database> {
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
        let mut db = Database::new(dir_path.to_str().unwrap())?;
        db.create_table(serde_json::from_value(authors_table)?)?;
        Ok(db)
    }

    fn create_get_item(id: u64) -> anyhow::Result<dml::GetItemCommand> {
        Ok(serde_json::from_value(json!({
            "table_name": "authors",
            "key": id.to_string(),
        }))?)
    }

    fn create_put_item(id: u64) -> anyhow::Result<dml::PutItemCommand> {
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
