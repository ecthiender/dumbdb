/// A dumb, barebones storage engine for dumbdb
use std::{
    fs::File,
    io::{BufReader, Cursor, Write},
    path::{Path, PathBuf},
};

use anyhow::Context;
use rmp_serde::{Deserializer, Serializer};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{byte_lines::ByteLines, query::types::ColumnValue};

/// A tuple is a list of values (well, possible values, hence `Option<..>`). In
/// other words, this is a row of data.
pub type Tuple = Vec<Option<ColumnValue>>;

/// A block is like a table (more like a slice of a table); its a list of
/// tuples. This is atomic unit in our storage engine.
///
/// But in practice, it is used to represent an entire table. And is backed by
/// one single file on disk.
///
/// This manages storage of the table data on disk. And exposes APIs to write
/// new data, seek to a specific row offset, and read all of the contents of the
/// block as an iterator fashion.
/// Note: it does not provide any API to delete or update data.
#[derive(Debug, Clone)]
pub struct Block {
    // file path of the file on disk
    file_path: PathBuf,
}

impl Block {
    pub fn new(table_path: &Path) -> anyhow::Result<Self> {
        Ok(Self {
            file_path: table_path.to_path_buf(),
        })
    }

    pub fn seek_to(&self, cursor: usize) -> anyhow::Result<Option<Tuple>> {
        let mut reader = self.get_reader()?;
        let tuple = reader.nth(cursor).transpose()?;
        Ok(tuple)
    }

    // get an iterator over the file to read line by line
    pub fn get_reader(&self) -> anyhow::Result<impl Iterator<Item = Result<Tuple, anyhow::Error>>> {
        let file = File::open(&self.file_path)?;
        let reader = BufReader::new(file);
        let bytelines = ByteLines::new(reader);

        // we have a ByteLines iterator; we map it to a Tuple iterator
        Ok(bytelines.into_iter().map(|line| {
            let line = line?;
            let data: Tuple = deserialize_binary(line)?;
            Ok::<Tuple, anyhow::Error>(data)
        }))
    }

    pub fn write(&mut self, tuple: Tuple) -> anyhow::Result<()> {
        self.write_to_file(serialize_binary(&tuple)?)
    }

    fn write_to_file(&mut self, mut data: Vec<u8>) -> anyhow::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&self.file_path)
            .with_context(|| "Could not open file for writing.")?;

        data.push(b'\n');
        file.write_all(&data)
            .with_context(|| "FATAL: Internal Error: Failed writing data to file")?;
        file.flush()?;
        file.sync_all()?;
        Ok(())
    }
}

pub fn serialize_binary<T>(value: &T) -> anyhow::Result<Vec<u8>>
where
    T: Serialize,
{
    let mut data = Vec::new();
    value.serialize(&mut Serializer::new(&mut data))?;
    Ok(data)
}

pub fn deserialize_binary<T>(value: Vec<u8>) -> anyhow::Result<T>
where
    T: DeserializeOwned,
{
    let mut de = Deserializer::new(Cursor::new(&value));
    let data = Deserialize::deserialize(&mut de)?;
    Ok(data)
}
