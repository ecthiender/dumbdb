/// A dumb, barebones storage engine for dumbdb
use std::{
    fs::File,
    io::{Cursor, Write},
};

use anyhow::Context;
use rmp_serde::{Deserializer, Serializer};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::query::dml::put_item::PrimitiveValue;

// A tuple is a vector of values. Well, possible values (hence Option<Value>).
pub type Tuple = Vec<Option<PrimitiveValue>>;

// A block is like a table (more like a slice of a table); its a list of tuples.
pub type Block = Vec<Tuple>;

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

fn write_to_file(file: &mut File, mut data: Vec<u8>) -> anyhow::Result<()> {
    data.push(b'\n');
    file.write_all(&data)
        .with_context(|| "FATAL: Internal Error: Failed writing data to file")?;
    file.flush()?;
    file.sync_all()?;
    Ok(())
}
