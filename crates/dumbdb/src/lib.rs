use std::path::PathBuf;

use catalog::Catalog;
pub use dml::{FilterItemCommand, GetItemCommand, PutItemCommand, Record};
use query::ddl;
pub use query::ddl::{CreateTableCommand, DropTableCommand};
use query::dml;
pub use query::error;
use query::error::QueryError;
pub use query::types::{ColumnValue, TableDefinition, TableName};

mod catalog;
mod query;
mod storage;
mod table;

#[derive(Debug, Clone)]
pub struct Database {
    catalog: Catalog,
}

impl Database {
    pub async fn new(path: &str) -> Result<Self, QueryError> {
        let catalog = Catalog::new(PathBuf::from(path)).await?;
        Ok(Self { catalog })
    }

    pub async fn create_table(&mut self, table: CreateTableCommand) -> Result<(), QueryError> {
        ddl::create_table(table, &mut self.catalog).await
    }

    pub async fn drop_table(&mut self, command: DropTableCommand) -> Result<(), QueryError> {
        ddl::drop_table(command, &mut self.catalog).await
    }

    pub async fn put_item(&mut self, command: dml::PutItemCommand) -> Result<(), QueryError> {
        dml::put_item(command, &mut self.catalog).await
    }

    pub async fn get_item(
        &self,
        command: dml::GetItemCommand,
    ) -> Result<Option<dml::Record>, QueryError> {
        dml::get_item(command, &self.catalog, false).await
    }

    pub async fn filter_item(
        &self,
        command: dml::FilterItemCommand,
    ) -> Result<Vec<dml::Record>, QueryError> {
        dml::filter_item(command, &self.catalog).await
    }

    pub fn get_size(&self, table: &TableName) -> Option<usize> {
        self.catalog.get_table_size(table)
    }

    pub fn list_tables(&self) -> Vec<TableName> {
        self.catalog.list_tables()
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{self};

    use futures::StreamExt;
    use query::types::ColumnValue;
    use rand::Rng;
    use serde_json::json;

    use super::*;

    const DB_PATH: &str = "data/test/dumbdb";

    #[tokio::test]
    async fn test_create_table() -> anyhow::Result<()> {
        let db = setup("create_table").await?;
        assert!(db.catalog.get_table(&"authors".into()).is_some());
        Ok(())
    }

    #[tokio::test]
    async fn test_write_data() -> anyhow::Result<()> {
        let mut db = setup("write_data").await?;
        for i in 0..10 {
            let author_item = create_put_item(i)?;
            db.put_item(author_item).await?;
        }
        let table = db.catalog.get_table(&"authors".into()).unwrap();

        let lines = table
            .table_buffer
            .block
            .get_reader()
            .await?
            .collect::<Vec<_>>()
            .await;

        let last_line = lines.into_iter().last().unwrap();
        // .with_context(|| "There should be 10 rows written")?;
        let last_line = last_line?;
        let values: Vec<_> = last_line.into_iter().flatten().collect();
        assert_eq!(values[0], ColumnValue::Integer(9));
        Ok(())
    }

    #[tokio::test]
    async fn test_read_data() -> anyhow::Result<()> {
        let mut db = setup("read_data").await?;
        for i in 0..10 {
            let author_item = create_put_item(i)?;
            db.put_item(author_item).await?;
        }
        for i in 5..8 {
            let cmd = create_get_item(i)?;
            let record = db.get_item(cmd).await?.unwrap();
            assert_eq!(
                record.get(&"id".into()).unwrap(),
                &Some(ColumnValue::Integer(i))
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_writes_with_same_id() -> anyhow::Result<()> {
        let mut db = setup("write_data_same_id").await?;

        // insert one record; and read it
        let id = 42;
        let put_item_1 = create_put_item(id)?;
        let generated_name = put_item_1.item.get(&"name".into()).cloned();
        db.put_item(put_item_1).await?;
        let get_item = create_get_item(id)?;
        let record = db.get_item(get_item).await?.unwrap();
        assert_eq!(
            record.get(&"id".into()).unwrap(),
            &Some(ColumnValue::Integer(id))
        );
        assert_eq!(record.get(&"name".into()).unwrap(), &generated_name);

        // insert another record with same id; it should overwrite the old data
        let put_item_2 = create_put_item(id)?;
        let res = db.put_item(put_item_2).await.map_err(|e| e.to_string());
        assert_eq!(
            res,
            Err("Record with primary key '42' already exists.".to_string())
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_writing_data_updates_index() -> anyhow::Result<()> {
        let mut db = setup("index_write").await?;
        for i in 0..20 {
            let author_item = create_put_item(i)?;
            db.put_item(author_item).await?;
        }

        let table = db.catalog.get_table(&"authors".into()).unwrap();

        let byte_offset = table.table_buffer.index.get(&ColumnValue::Integer(0));
        assert!(byte_offset.is_some());
        let byte_offset = byte_offset.unwrap();
        assert_eq!(byte_offset, &0);

        let byte_offset = table.table_buffer.index.get(&ColumnValue::Integer(6));
        assert!(byte_offset.is_some());
        let byte_offset = byte_offset.unwrap();
        let tuple = table
            .table_buffer
            .block
            .seek_to_offset(*byte_offset)
            .await?;
        let primary_key = tuple[table.table_buffer.pk_position].clone().unwrap();
        assert_eq!(primary_key, ColumnValue::Integer(6));

        let byte_offset = table.table_buffer.index.get(&ColumnValue::Integer(9));
        assert!(byte_offset.is_some());
        let byte_offset = byte_offset.unwrap();
        let tuple = table
            .table_buffer
            .block
            .seek_to_offset(*byte_offset)
            .await?;
        let primary_key = tuple[table.table_buffer.pk_position].clone().unwrap();
        assert_eq!(primary_key, ColumnValue::Integer(9));
        Ok(())
    }

    #[tokio::test]
    async fn test_write_lots_of_data() -> anyhow::Result<()> {
        let mut db = setup("write_data_lots").await?;
        for i in 0..1001 {
            let author_item = create_put_item(i)?;
            db.put_item(author_item).await?;
        }
        let table = db.catalog.get_table(&"authors".into()).unwrap();
        let mut stream = table.table_buffer.block.get_reader().await?;
        while let Some(tuple) = stream.next().await {
            let tuple = tuple?;
            assert_eq!(tuple.len(), 2);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_filtering() -> anyhow::Result<()> {
        let mut db = setup("filtering").await?;
        for i in 0..100 {
            let author_item = create_put_item(i)?;
            db.put_item(author_item).await?;
        }

        // test expression 1
        let cmd = create_filter_item_1()?;
        let res = db.filter_item(cmd).await?;
        assert_eq!(res.len(), 80);

        // test expression 2
        let cmd = create_filter_item_2()?;
        let res = db.filter_item(cmd).await?;
        assert_eq!(res.len(), 10);

        Ok(())
    }

    async fn setup(test_name: &str) -> anyhow::Result<Database> {
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
        let mut db = Database::new(dir_path.to_str().unwrap()).await?;
        db.create_table(serde_json::from_value(authors_table)?)
            .await?;
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
