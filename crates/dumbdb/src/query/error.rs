use std::path::PathBuf;

use thiserror;

use crate::{catalog::CatalogError, table::TableBufferError, TableName};

use super::types::{ColumnName, ColumnType, ColumnValue};

#[derive(thiserror::Error, Debug)]
pub enum QueryError {
    #[error("Table '{0}' not found.")]
    TableNotFound(TableName),
    #[error("Table name '{0}' already exists.")]
    TableAlreadyExists(TableName),
    #[error("Item object must contain primary key: {0}.")]
    ItemMustContainPrimaryKey(ColumnName),
    #[error("Record with primary key '{0}' already exists.")]
    PrimaryKeyAlreadyExists(ColumnValue),
    #[error("Unknown column in item object: {0}.")]
    UnknownColumnInItem(ColumnName),
    #[error("Column type mismatch. Column defined as type: {expected}, but provided value has type: {given}.")]
    ColumnTypeMismatch {
        expected: ColumnType,
        given: ColumnType,
    },
    #[error("Internal Error: {0}")]
    InternalError(InternalError),
    #[error("Internal Error: {0}")]
    TableStorageError(#[from] TableBufferError),
    #[error("Internal Error: {0}")]
    CatalogError(#[from] CatalogError),
}

#[derive(thiserror::Error, Debug)]
pub enum InternalError {
    #[error("Table filepath does not exist: {0}")]
    FilepathNotFound(PathBuf),
    #[error("Table filepath {0} already exists.")]
    FilepathAlreadyExists(PathBuf),
    #[error("Failed to create table file path: {filepath}. Error: {error}")]
    FailedToCreateFile {
        filepath: PathBuf,
        error: std::io::Error,
    },
    #[error("Failed to delete table file path: {filepath}. Error: {error}")]
    FailedToDeleteFile {
        filepath: PathBuf,
        error: std::io::Error,
    },
}
