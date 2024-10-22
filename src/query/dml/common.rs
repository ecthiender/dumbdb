use std::collections::HashMap;

use crate::{
    query::types::{ColumnDefinition, ColumnName, ColumnValue},
    storage::Tuple,
};

pub type Record = HashMap<ColumnName, Option<ColumnValue>>;

pub fn parse_record(columns: &[ColumnDefinition], item: Tuple) -> Record {
    let mut record = HashMap::new();
    for (idx, value) in item.into_iter().enumerate() {
        let col_name = columns[idx].name.clone();
        record.insert(col_name, value);
    }
    record
}
