/// A dumb, barebones storage engine for dumbdb. This is the unit that only
/// deals with storing and retrieving data from disk.
use std::{
    fs::File,
    io::{BufReader, Cursor, Read, Seek, SeekFrom, Write},
    iter,
    path::{Path, PathBuf},
};

use anyhow::Context;
use rmp_serde::{Deserializer, Serializer};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

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
}

impl Block {
    /// Create a new block. Takes a file path, where the data of the block is
    /// stored on disk.
    pub fn new(table_path: &Path) -> anyhow::Result<Self> {
        Ok(Self {
            file_path: table_path.to_path_buf(),
        })
    }

    /// Read a specific tuple. This offers a O(1) seek to the tuple on the disk.
    /// Given a byte-offset, seek to that specific offset in the block, and
    /// return a `Tuple`
    pub fn seek_to_offset(&self, offset: u64) -> anyhow::Result<Tuple> {
        // Seek to the correct byte offset
        let mut file = File::open(&self.file_path)?;
        file.seek(SeekFrom::Start(offset))?;

        // Read the length prefix (8 bytes)
        let mut length_buf = [0u8; 8];
        file.read_exact(&mut length_buf)?;
        let tuple_length = u64::from_le_bytes(length_buf);

        // Now read the tuple data based on its length
        let mut data_buf = vec![0u8; tuple_length as usize];
        file.read_exact(&mut data_buf)?;

        deserialize_binary(&data_buf)
    }

    /// Get an iterator over the block to read tuples in an iterator pattern.
    /// This uses Rust iterators, so it is memory efficient.
    pub fn get_reader(&self) -> anyhow::Result<impl Iterator<Item = anyhow::Result<Tuple>>> {
        Ok(self
            .get_reader_with_byte_offset()?
            .map(|x| x.map(|(tuple, _length_prefix)| tuple)))
    }

    /// Get an iterator over the block to read tuples along with its byte
    /// offset, in an iterator pattern. This uses Rust iterators, so it is
    /// memory efficient.
    pub fn get_reader_with_byte_offset(
        &self,
    ) -> anyhow::Result<impl Iterator<Item = anyhow::Result<(Tuple, u64)>>> {
        let file = File::open(&self.file_path)?;
        let mut reader = BufReader::new(file);
        let mut offset: u64 = 0;

        Ok(iter::from_fn(move || {
            let mut length_bytes = [0u8; 8];
            // Read the length prefix
            if reader.read_exact(&mut length_bytes).is_err() {
                return None; // EOF or read error
            }

            let length = u64::from_le_bytes(length_bytes);
            let mut buffer = vec![0; length as usize];

            // Read the tuple
            if reader.read_exact(&mut buffer).is_err() {
                return None; // Read error
            }

            // Deserialize the tuple
            let tuple: Tuple = match deserialize_binary(&buffer) {
                Ok(data) => data,
                Err(err) => return Some(Err(err)),
            };

            // Create a tuple of (Tuple, u64) to return
            let result = Some(Ok((tuple, offset)));

            // Update the offset for the next read
            offset += 8 + length; // 8 bytes for length prefix + length of the tuple

            result
        }))
    }

    /// Write a `Tuple` and return the length of data written.
    pub fn write(&mut self, tuple: Tuple) -> anyhow::Result<u64> {
        let serialized = serialize_binary(&tuple)?;
        let length = serialized.len() as u64;
        println!("[DEBUG] writing tuple of length {}: {:?}", length, tuple);
        self.write_to_file(length.to_le_bytes(), serialized)?;
        Ok(length)
    }

    // write binary data to file
    fn write_to_file(&mut self, length_bytes: [u8; 8], data: Vec<u8>) -> anyhow::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&self.file_path)
            .with_context(|| "ERROR: Internal Error: Could not open block file for writing.")?;

        // Write the length prefix and then the actual data
        file.write_all(&length_bytes).with_context(|| {
            "ERROR: Internal Error: Failed to write length prefix to block file."
        })?;
        file.write_all(&data)
            .with_context(|| "ERROR: Internal Error: Failed to write data to block file.")?;
        file.flush()?;
        file.sync_all()?;
        Ok(())
    }
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
