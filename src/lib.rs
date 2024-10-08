use std::path::PathBuf;

use catalog::Catalog;
pub use dml::{GetItemCommand, PutItemCommand, Record};
use query::ddl;
use query::dml;
pub use query::types::TableDefinition;

pub mod byte_lines;
mod catalog;
mod query;
pub mod storage;

#[derive(Debug, Clone)]
pub struct Database {
    catalog: Catalog,
}

impl Database {
    pub fn new(path: &str) -> anyhow::Result<Self> {
        let catalog = Catalog::new(PathBuf::from(path))?;
        Ok(Self { catalog })
    }

    pub fn create_table(&mut self, table: TableDefinition) -> anyhow::Result<()> {
        ddl::create_table(table, &mut self.catalog)
    }

    pub fn put_item(&mut self, command: dml::PutItemCommand) -> anyhow::Result<()> {
        dml::put_item(command, &mut self.catalog)
    }

    pub fn get_item(&self, command: dml::GetItemCommand) -> anyhow::Result<Option<dml::Record>> {
        dml::get_item(command, &self.catalog, false)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, File},
        io::BufReader,
    };

    use anyhow::Context;
    use byte_lines::ByteLines;
    use query::types::ColumnValue;
    use rand::Rng;
    use serde_json::json;
    use storage::{deserialize_binary, Tuple};

    use super::*;

    const DB_PATH: &str = "data/test/dumbdb";

    #[test]
    fn test_create_table() -> anyhow::Result<()> {
        let db = setup("create_table")?;
        assert!(db.catalog.get_table(&"authors".into()).is_some());
        Ok(())
    }

    #[test]
    fn test_write_data() -> anyhow::Result<()> {
        let mut db = setup("write_data")?;
        for i in 0..10 {
            let author_item = create_put_item(i)?;
            db.put_item(author_item)?;
        }
        let table_path = db.catalog.get_table_path(&"authors".into());
        let file_handle = File::open(table_path)?;
        let reader = BufReader::new(file_handle);
        let bytelines = ByteLines::new(reader);

        let last_line = bytelines
            .last()
            .with_context(|| "There should be 10 rows written")?;
        let last_line = last_line?;
        let tuple: Tuple = deserialize_binary(last_line)?;
        let values: Vec<_> = tuple.into_iter().flatten().collect();
        assert_eq!(values[0], ColumnValue::Integer(9));
        Ok(())
    }

    #[test]
    fn test_read_data() -> anyhow::Result<()> {
        let mut db = setup("read_data")?;
        for i in 0..10 {
            let author_item = create_put_item(i)?;
            db.put_item(author_item)?;
        }
        for i in 5..8 {
            let cmd = create_get_item(i)?;
            let record = db.get_item(cmd)?.unwrap();
            assert_eq!(
                record.get(&"id".into()).unwrap(),
                &Some(ColumnValue::Integer(i))
            );
        }
        Ok(())
    }

    #[test]
    fn test_writes_with_same_id() -> anyhow::Result<()> {
        let mut db = setup("write_data_same_id")?;

        // insert one record; and read it
        let id = 42;
        let put_item_1 = create_put_item(id)?;
        let generated_name = put_item_1.item.get(&"name".into()).cloned();
        db.put_item(put_item_1)?;
        let get_item = create_get_item(id)?;
        let record = db.get_item(get_item)?.unwrap();
        assert_eq!(
            record.get(&"id".into()).unwrap(),
            &Some(ColumnValue::Integer(id))
        );
        assert_eq!(record.get(&"name".into()).unwrap(), &generated_name);

        // insert another record with same id; it should overwrite the old data
        let put_item_2 = create_put_item(id)?;
        let res = db.put_item(put_item_2).map_err(|e| e.to_string());
        assert_eq!(
            res,
            Err("ERROR: Item with primary key '42' already exists.".to_string())
        );

        Ok(())
    }

    #[test]
    fn test_writing_data_updates_index() -> anyhow::Result<()> {
        let mut db = setup("index_write")?;
        for i in 0..10 {
            let author_item = create_put_item(i)?;
            db.put_item(author_item)?;
        }

        let table = db.catalog.get_table(&"authors".into()).unwrap();

        let row_pos = table.index.get("1");
        assert!(row_pos.is_some());
        let row_pos = row_pos.unwrap();
        assert_eq!(row_pos, &1);

        let row_pos = table.index.get("6");
        assert!(row_pos.is_some());
        let row_pos = row_pos.unwrap();
        assert_eq!(row_pos, &6);

        let row_pos = table.index.get("9");
        assert!(row_pos.is_some());
        let row_pos = row_pos.unwrap();
        assert_eq!(row_pos, &9);

        assert_eq!(table.cursor, 10);
        Ok(())
    }

    // looks like even when writing 11(!) rows of data, there is some issue, that
    // it can't read all of them back?!
    #[test]
    fn test_write_lots_of_data() -> anyhow::Result<()> {
        let mut db = setup("write_data_lots")?;
        for i in 0..11 {
            let author_item = create_put_item(i)?;
            db.put_item(author_item)?;
        }
        let table = db.catalog.get_table(&"authors".into()).unwrap();
        for tuple in table.block.get_reader()? {
            let tuple = tuple?;
            assert_eq!(tuple.len(), 2);
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
        const CHARSET: &[u8] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 )(*&^%$#@!~\"',;";
        const STRING_LEN: usize = 20;
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
