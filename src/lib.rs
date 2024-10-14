use std::path::PathBuf;

use catalog::Catalog;
pub use dml::{FilterItemCommand, GetItemCommand, PutItemCommand, Record};
use query::ddl;
use query::dml;
pub use query::types::TableDefinition;

mod catalog;
mod query;
mod storage;
mod table;

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

    pub fn filter_item(&self, command: dml::FilterItemCommand) -> anyhow::Result<Vec<dml::Record>> {
        dml::filter_item(command, &self.catalog)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{self};

    use anyhow::Context;
    use query::types::ColumnValue;
    use rand::Rng;
    use serde_json::json;

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
        let table = db.catalog.get_table(&"authors".into()).unwrap();

        let last_line = table
            .table_buffer
            .block
            .get_reader()?
            .last()
            .with_context(|| "There should be 10 rows written")?;
        let last_line = last_line?;
        let values: Vec<_> = last_line.into_iter().flatten().collect();
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
        for i in 0..20 {
            let author_item = create_put_item(i)?;
            db.put_item(author_item)?;
        }

        let table = db.catalog.get_table(&"authors".into()).unwrap();

        let byte_offset = table.table_buffer.index.get(&ColumnValue::Integer(0));
        assert!(byte_offset.is_some());
        let byte_offset = byte_offset.unwrap();
        assert_eq!(byte_offset, &0);

        let byte_offset = table.table_buffer.index.get(&ColumnValue::Integer(6));
        assert!(byte_offset.is_some());
        let byte_offset = byte_offset.unwrap();
        let tuple = table.table_buffer.block.seek_to_offset(*byte_offset)?;
        let primary_key = tuple[table.table_buffer.pk_position].clone().unwrap();
        assert_eq!(primary_key, ColumnValue::Integer(6));

        let byte_offset = table.table_buffer.index.get(&ColumnValue::Integer(9));
        assert!(byte_offset.is_some());
        let byte_offset = byte_offset.unwrap();
        let tuple = table.table_buffer.block.seek_to_offset(*byte_offset)?;
        let primary_key = tuple[table.table_buffer.pk_position].clone().unwrap();
        assert_eq!(primary_key, ColumnValue::Integer(9));
        Ok(())
    }

    #[test]
    fn test_write_lots_of_data() -> anyhow::Result<()> {
        let mut db = setup("write_data_lots")?;
        for i in 0..100 {
            let author_item = create_put_item(i)?;
            db.put_item(author_item)?;
        }
        let table = db.catalog.get_table(&"authors".into()).unwrap();
        for tuple in table.table_buffer.block.get_reader()? {
            let tuple = tuple?;
            assert_eq!(tuple.len(), 2);
        }
        Ok(())
    }

    #[test]
    fn test_filtering() -> anyhow::Result<()> {
        let mut db = setup("filtering")?;
        for i in 0..100 {
            let author_item = create_put_item(i)?;
            db.put_item(author_item)?;
        }

        // test expression 1
        let cmd = create_filter_item_1()?;
        let res = db.filter_item(cmd)?;
        assert_eq!(res.len(), 80);

        // test expression 2
        let cmd = create_filter_item_2()?;
        let res = db.filter_item(cmd)?;
        assert_eq!(res.len(), 10);

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
            "key": ColumnValue::Integer(id),
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

    fn create_filter_item_1() -> anyhow::Result<dml::FilterItemCommand> {
        Ok(serde_json::from_value(json!({
              "table_name": "authors",
              "filter": {
                "$and": [
                  {
                    "column": "id",
                    "op": "$gt",
                    "value": 9
                  },
                  {
                    "column": "id",
                    "op": "$lt",
                    "value": 90
                  },
                ]
              }
        }))?)
    }

    fn create_filter_item_2() -> anyhow::Result<dml::FilterItemCommand> {
        Ok(serde_json::from_value(json!({
              "table_name": "authors",
              "filter": {
                "$or": [
                  {
                    "column": "id",
                    "op": "$eq",
                    "value": 42
                  },
                  {
                    "$and": [
                      {
                        "column": "id",
                        "op": "$gt",
                        "value": 1
                      },
                      {
                        "column": "id",
                        "op": "$lte",
                        "value": 10
                      }
                    ]
                  }
                ]
              }
        }))?)
    }
}
