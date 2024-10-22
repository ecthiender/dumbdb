use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use futures::StreamExt;
use tokio::sync::Mutex;

use crate::{
    query::types::{ColumnValue, TableName},
    storage::{calculate_new_offset, Block, StorageError, Tuple},
    TableDefinition,
};

/// An abstraction that the query engine can query the data against. That is the
/// query engine doesn't deal with the storage layer. This layer provides an
/// abstraction over the storage layer. This is where we implement indexing.
#[derive(Debug, Clone)]
pub(crate) struct TableBuffer {
    /// The block backing this table
    pub(crate) block: Block,
    /// The table index for O(1) lookups
    pub(crate) index: Index,
    /// Column index of the primary key
    pub(crate) pk_position: usize,
}

/// The index structure. It is a map of primary key to byte-offset in the block.
#[derive(Debug, Clone)]
pub struct Index {
    /// Byte-offset based index.
    pub(crate) index: HashMap<ColumnValue, u64>,
    /// The next byte offset we are pointing to. When a write comes, this value
    /// will be used for that tuple. The byte offset is kept behind a lock, so
    /// as to perform thread-safe updates.
    pub(crate) byte_offset: Arc<Mutex<u64>>,
}

#[derive(thiserror::Error, Debug)]
pub enum TableBufferError {
    #[error("Unexpected invariant violation: primary key not found in column definitions.")]
    PrimaryKeyNotInDefn,
    #[error("Unexpected invariant violation: primary key not found in data tuple.")]
    PrimaryKeyNotInTuple,
    #[error("Internal Storage Engine Error: {0}")]
    StorageError(#[from] StorageError),
}

impl Index {
    fn new() -> Self {
        Self {
            index: HashMap::new(),
            byte_offset: Arc::new(Mutex::new(0)),
        }
    }
    pub(crate) fn get(&self, key: &ColumnValue) -> Option<&u64> {
        self.index.get(key)
    }
    async fn update(&mut self, key: ColumnValue, tuple_length: u64) {
        let mut curr_offset = self.byte_offset.lock().await;
        self.index.insert(key, *curr_offset);
        *curr_offset = calculate_new_offset(tuple_length, *curr_offset);
    }
}

impl TableBuffer {
    pub async fn new(
        table_definition: &TableDefinition,
        directory_path: &Path,
    ) -> Result<Self, TableBufferError> {
        let table_path = get_table_path_(directory_path, &table_definition.name);

        let key_position = table_definition
            .columns
            .iter()
            .position(|col_def| col_def.name == table_definition.primary_key)
            .ok_or(TableBufferError::PrimaryKeyNotInDefn)?;

        let block = Block::new(&table_path)?;

        let mut table = Self {
            block,
            pk_position: key_position,
            index: Index::new(),
        };
        table.build_index().await?;
        Ok(table)
    }

    pub async fn get(
        &self,
        key: ColumnValue,
        scan_file: bool,
    ) -> Result<Option<Tuple>, TableBufferError> {
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

    pub async fn write(&mut self, key: ColumnValue, tuple: Tuple) -> Result<(), TableBufferError> {
        // write the tuple
        let length_bytes = self.block.write(tuple).await?;
        // update the index
        self.index.update(key, length_bytes).await;
        Ok(())
    }

    /// Does this table's index contains the given key
    pub fn contains_key(&self, key: &ColumnValue) -> bool {
        self.index.index.contains_key(key)
    }

    pub fn size(&self) -> usize {
        self.index.index.len()
    }

    // scan the entire block to get an item
    async fn scan_block_get_item(
        &self,
        user_key: ColumnValue,
    ) -> Result<Option<Tuple>, TableBufferError> {
        let mut stream = self.block.get_reader().await?;
        while let Some(tuple) = stream.next().await {
            let tuple = tuple?;
            let key = tuple[self.pk_position]
                .clone()
                .ok_or(TableBufferError::PrimaryKeyNotInTuple)?;
            if key == user_key {
                return Ok(Some(tuple));
            }
        }
        Ok(None)
    }

    // build the index during initialization by reading through the entire block
    async fn build_index(&mut self) -> Result<(), TableBufferError> {
        let mut stream = self.block.get_reader_with_length().await?;
        while let Some(result) = stream.next().await {
            let (tuple, length) = result?;
            let index_key = tuple[self.pk_position]
                .clone()
                .ok_or(TableBufferError::PrimaryKeyNotInTuple)?;
            // Calling the index.update function in this tight loop might be
            // slow; as we obtain the lock, update the data and release the lock
            // inside this tight loop. But it's fine until this practically
            // becomes a problem. Then we can optimize it.
            self.index.update(index_key, length).await;
        }
        Ok(())
    }
}

// helpers
pub fn get_table_path_(directory_path: &Path, table_name: &TableName) -> PathBuf {
    let table_rel_path = PathBuf::from(format!("{}.dat", table_name.0.as_str()));
    directory_path.join(table_rel_path)
}
