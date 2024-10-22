use std::pin::Pin;
/// A dumb, barebones storage engine for dumbdb. This is the unit that only
/// deals with storing and retrieving data from disk.
use std::{
    io::{Cursor, SeekFrom},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Context;
use futures::{Stream, StreamExt};
use rmp_serde::{Deserializer, Serializer};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::fs::File;
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader},
    sync::RwLock,
};

use crate::query::types::ColumnValue;

/// A tuple is a list of values (well, possible values, hence `Option<..>`). In
/// other words, this is a row of data.
pub type Tuple = Vec<Option<ColumnValue>>;

/// A block stores a table (i.e. a list of tuples) on disk, backed by a single
/// file.
//
/// It provides APIs to write new data, seek to a specific byte-offset, and read
/// all of the contents of the block as an iterator fashion.
///
/// Note: it does not provide any API to delete or update data.
///
/// Internally, this stores data in a length-prefixed binary format. So it can
/// have a O(1) retrieval of a specific tuple. Otherwise, you can read all
/// tuples in an iterator pattern.
#[derive(Debug, Clone)]
pub struct Block {
    // file path of the file on disk
    file_path: PathBuf,
    write_handle: Arc<RwLock<File>>,
}

// Size of the length prefix. We use these many bits to store the length of each
// tuple.
const LENGTH_PREFIX_SIZE: usize = 8;

impl Block {
    /// Create a new block. Takes a file path, where the data of the block is
    /// stored on disk.
    pub fn new(table_path: &Path) -> anyhow::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .append(true)
            .open(table_path)
            .with_context(|| "Internal Error: Could not open block file for writing.")?;

        Ok(Self {
            file_path: table_path.to_path_buf(),
            write_handle: Arc::new(RwLock::new(file.into())),
        })
    }

    /// Read a specific tuple. This offers a O(1) seek to the tuple on the disk.
    /// Given a byte-offset, seek to that specific offset in the block, and
    /// return a `Tuple`
    pub async fn seek_to_offset(&self, offset: u64) -> anyhow::Result<Tuple> {
        // Seek to the correct byte offset
        let mut file = File::open(&self.file_path)
            .await
            .with_context(|| "Internal Error: Could not open block file for seeking.")?;
        file.seek(SeekFrom::Start(offset))
            .await
            .with_context(|| "Internal Error: Could not seek in file.")?;

        // Read the length prefix (8 bytes)
        let mut length_buf = [0u8; LENGTH_PREFIX_SIZE];
        file.read_exact(&mut length_buf).await?;
        // .with_context(|| "Internal Error: Could not read length-prefix from file.")?;
        let tuple_length = u64::from_le_bytes(length_buf);

        // Now read the tuple data based on its length
        let mut data_buf = vec![0u8; tuple_length as usize];
        file.read_exact(&mut data_buf)
            .await
            .with_context(|| "Internal Error: Could not read data frame from file.")?;

        deserialize_binary(&data_buf)
    }

    /// Get an iterator over the block to read tuples in an iterator pattern.
    /// This uses Rust iterators, so it is memory efficient.
    pub async fn get_reader(&self) -> anyhow::Result<impl Stream<Item = anyhow::Result<Tuple>>> {
        // this is basically: getStream >>= traverse fst
        let stream = self.get_reader_with_length().await?;
        Ok(stream.map(|x| x.map(|(tuple, _length_prefix)| tuple)))
    }

    /// Get an iterator over the block to read tuples along with its byte
    /// offset, in an iterator pattern. This uses Rust iterators, so it is
    /// memory efficient.
    pub async fn get_reader_with_length(
        &self,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<(Tuple, u64)>>> {
        // this is basically: getStream >>= traverse deserialize_binary
        let stream = self.get_stream_with_length().await?;
        Ok(stream
            .map(|(data, length)| deserialize_binary(&data).map(|tuple: Tuple| (tuple, length))))
    }

    async fn get_stream_with_length(
        &self,
    ) -> anyhow::Result<Pin<Box<impl Stream<Item = (Vec<u8>, u64)>>>> {
        let file = File::open(&self.file_path).await?;
        let reader = BufReader::new(file);
        // let offset: u64 = 0;
        // Create a stream that reads the file and yields tuples with their offsets
        let stream = futures::stream::unfold(reader, |mut reader| async move {
            // Read the length prefix
            let mut length_bytes = [0u8; LENGTH_PREFIX_SIZE];
            match reader.read_exact(&mut length_bytes).await {
                Err(_e) => {
                    None // EOF or read error
                }
                Ok(_x) => {
                    // Read the data frame
                    let length = u64::from_le_bytes(length_bytes); // length of the data from the prefix
                    let mut buffer = vec![0; length as usize];
                    if reader.read_exact(&mut buffer).await.is_err() {
                        return None; // Read error
                    }
                    Some(((buffer, length), (reader)))
                }
            }
        });
        Ok(Box::pin(stream))
    }

    /// Write a `Tuple` and return the length of data written.
    pub async fn write(&mut self, tuple: Tuple) -> anyhow::Result<u64> {
        let serialized = serialize_binary(&tuple)?;
        let length = serialized.len() as u64;
        self.write_to_file(length.to_le_bytes(), serialized).await?;
        Ok(length)
    }

    // write binary data to file
    async fn write_to_file(&mut self, length_bytes: [u8; 8], data: Vec<u8>) -> anyhow::Result<()> {
        let mut file = self.write_handle.write().await;
        // Write the length prefix and then the actual data
        file.write_all(&length_bytes).await.with_context(|| {
            "ERROR: Internal Error: Failed to write length prefix to block file."
        })?;
        file.write_all(&data)
            .await
            .with_context(|| "ERROR: Internal Error: Failed to write data to block file.")?;
        file.flush().await?;
        file.sync_all().await?;
        Ok(())
    }
}

pub fn calculate_new_offset(tuple_length: u64, current_offset: u64) -> u64 {
    // new offset = current offset + length prefix + length of the tuple
    current_offset + LENGTH_PREFIX_SIZE as u64 + tuple_length
}

fn serialize_binary<T>(value: &T) -> anyhow::Result<Vec<u8>>
where
    T: Serialize,
{
    let mut data = Vec::new();
    value.serialize(&mut Serializer::new(&mut data))?;
    Ok(data)
}

fn deserialize_binary<T>(value: &[u8]) -> anyhow::Result<T>
where
    T: DeserializeOwned,
{
    let mut de = Deserializer::new(Cursor::new(&value));
    let data = Deserialize::deserialize(&mut de)?;
    Ok(data)
}
