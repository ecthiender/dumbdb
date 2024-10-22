use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Context;
use futures::StreamExt;
use tokio::sync::Mutex;

use crate::{
    query::types::{ColumnValue, TableName},
    storage::{calculate_new_offset, Block, Tuple},
    TableDefinition,
};

/// An abstraction that the query engine can query the data against. That is the
/// query engine doesn't deal with the storage layer. This layer provides an
/// abstraction over the storage layer. This is where we implement indexing.
#[derive(Debug, Clone)]
pub(crate) struct TableBuffer {
    pub(crate) block: Block,
    /// Byte-offset based index.
    pub(crate) index: HashMap<ColumnValue, u64>,
    // The current byte offset we are pointing to. This is used for indexing. To
    // know where in the file a particular tuple is located.
    pub(crate) byte_offset: Arc<Mutex<u64>>,
    pub(crate) pk_position: usize,
}

impl TableBuffer {
    pub async fn new(
        table_definition: &TableDefinition,
        directory_path: &Path,
    ) -> anyhow::Result<Self> {
        let table_path = get_table_path_(directory_path, &table_definition.name);

        let key_position = table_definition
            .columns
            .iter()
            .position(|col_def| col_def.name == table_definition.primary_key)
            .with_context(|| "Internal Error: primary key must exist.")?;

        let block = Block::new(&table_path)?;

        let mut table = Self {
            block,
            pk_position: key_position,
            index: HashMap::new(),
            byte_offset: Arc::new(Mutex::new(0)),
        };
        table.build_index().await?;
        Ok(table)
    }

    pub async fn get(&self, key: ColumnValue, scan_file: bool) -> anyhow::Result<Option<Tuple>> {
        // read from the index; get the cursor
        if let Some(offset) = self.index.get(&key) {
            let tuple = self.block.seek_to_offset(*offset).await?;
            Ok(Some(tuple))
        // if not found in the index
        } else {
            // The index is our main lookup structure; one invariant is all
            // primary keys are available in the index. Hence if it's not
            // found in the index; the key doesn't exist. return None
            //
            // But.. software bugs are pesky and sometimes hard to predict.
            // Maybe there's an edge case where the index doesn't have the
            // key, but the file has it. For those cases, if scan_file flag
            // is passed then we rescan the entire file.
            if scan_file {
                Ok(self.scan_block_get_item(key).await?)
            } else {
                Ok(None)
            }
        }
    }

    pub async fn write(&mut self, key: ColumnValue, tuple: Tuple) -> anyhow::Result<()> {
        // write the tuple
        let length_bytes = self.block.write(tuple).await?;
        // update the index
        let mut curr_offset = self.byte_offset.lock().await;
        self.index.insert(key, *curr_offset);
        *curr_offset = calculate_new_offset(length_bytes, *curr_offset);
        Ok(())
    }

    /// Does this table's index contains the given key
    pub fn contains_key(&self, key: &ColumnValue) -> bool {
        self.index.contains_key(key)
    }

    pub fn size(&self) -> usize {
        self.index.len()
    }

    // scan the entire block to get an item
    async fn scan_block_get_item(&self, user_key: ColumnValue) -> anyhow::Result<Option<Tuple>> {
        let mut stream = self.block.get_reader().await?;
        while let Some(tuple) = stream.next().await {
            let tuple = tuple?;
            let key = tuple[self.pk_position]
                .clone()
                .with_context(|| "invariant violation: primary key value not found in tuple.")?;
            if key == user_key {
                return Ok(Some(tuple));
            }
        }
        Ok(None)
    }

    // build the index during initialization by reading through the entire block
    async fn build_index(&mut self) -> anyhow::Result<()> {
        let mut stream = self.block.get_reader_with_length().await?;
        let mut curr_offset = self.byte_offset.lock().await;
        while let Some(result) = stream.next().await {
            let (tuple, length) = result?;
            let index_key = tuple[self.pk_position]
                .clone()
                .with_context(|| "invariant violation: primary key value not found in tuple.")?;
            self.index.insert(index_key, *curr_offset);
            *curr_offset = calculate_new_offset(length, *curr_offset);
        }
        Ok(())
    }
}

// helpers
pub fn get_table_path_(directory_path: &Path, table_name: &TableName) -> PathBuf {
    let table_rel_path = PathBuf::from(format!("{}.dat", table_name.0.as_str()));
    directory_path.join(table_rel_path)
}
