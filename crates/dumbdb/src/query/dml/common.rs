use std::collections::HashMap;

use crate::{
    query::types::{ColumnDefinition, ColumnName, ColumnValue},
    storage::Tuple,
};

/// `Record` type is basically a user-friendly value of a `Tuple`. It is a map
/// of column name to column value (well, actually `Option<ColumnValue>`, and
/// that is to indicate possible null values).
pub type Record = HashMap<ColumnName, Option<ColumnValue>>;

pub fn build_record(columns: &[ColumnDefinition], item: Tuple) -> Record {
    let mut record = HashMap::new();
    for (idx, value) in item.into_iter().enumerate() {
        let col_name = columns[idx].name.clone();
        record.insert(col_name, value);
    }
    record
}
