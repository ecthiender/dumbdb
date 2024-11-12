use dumbdb::error::QueryError;

#[derive(thiserror::Error, Debug)]
pub enum AppError {
    #[error("Parse Error: {0}")]
    Parse(ParseError),
    #[error("Error: {0}")]
    Execute(QueryError),
}

impl From<QueryError> for AppError {
    fn from(err: QueryError) -> Self {
        Self::Execute(err)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ParseError {
    #[error("table name and key (separated by spaces) not found in get command.")]
    Get,
    #[error("Error parsing JSON document for put item. {0}")]
    Put(serde_json::Error),
    #[error("Error: {0}")]
    CreateTable(CreateTableError),
    #[error("Unknown command: {0}")]
    UnknownCommand(String),
}

impl From<ParseError> for AppError {
    fn from(err: ParseError) -> Self {
        Self::Parse(err)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CreateTableError {
    #[error("Invalid JSON document for create table. {0}")]
    InvalidJson(serde_json::Error),
    #[error("`columns` not defined in table definition.")]
    ColumnsNotFound,
    #[error("`primary_key` not defined in table definition.")]
    PrimaryKeyNotFound,
}

impl From<CreateTableError> for ParseError {
    fn from(err: CreateTableError) -> Self {
        Self::CreateTable(err)
    }
}
