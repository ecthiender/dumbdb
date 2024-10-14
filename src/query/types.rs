use std::fmt::Display;

use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableDefinition {
    pub name: TableName,
    pub columns: Vec<ColumnDefinition>,
    pub primary_key: ColumnName,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ColumnDefinition {
    pub name: ColumnName,
    pub r#type: ColumnType,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ColumnType {
    Integer,
    Float,
    Text,
    Boolean,
}

impl Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Integer => write!(f, "Integer"),
            Self::Float => write!(f, "Float"),
            Self::Boolean => write!(f, "Boolean"),
            Self::Text => write!(f, "Text"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Hash, Clone)]
#[serde(untagged)]
pub enum ColumnValue {
    Integer(u64),
    // Float(f64), // <-- f64 doesn't have PartialOrd, Ord, Eq or Hash. So we can't use it as a key in our index.
    Boolean(bool),
    Text(String),
}

impl Display for ColumnValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Integer(val) => write!(f, "{}", val),
            // Self::Float(val) => write!(f, "{}", val),
            Self::Boolean(val) => write!(f, "{}", val),
            Self::Text(val) => write!(f, "{}", val),
        }
    }
}

impl From<String> for ColumnValue {
    fn from(value: String) -> Self {
        match value.parse::<u64>() {
            Ok(int) => ColumnValue::Integer(int),
            Err(_) => match value.parse::<bool>() {
                Ok(boolean) => ColumnValue::Boolean(boolean),
                Err(_) => ColumnValue::Text(value),
            },
        }
    }
}

impl From<ColumnValue> for String {
    fn from(val: ColumnValue) -> String {
        format!("{}", val)
    }
}

// Expresion type

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Expression {
    #[serde(rename = "$and")]
    And(Vec<Expression>),
    #[serde(rename = "$or")]
    Or(Vec<Expression>),
    #[serde(rename = "$not")]
    Not(Box<Expression>),
    #[serde(untagged)]
    ColumnComparison(ColumnComparison),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
#[serde(rename_all = "snake_case")]
pub struct ColumnComparison {
    pub column: ColumnName,
    #[serde(rename = "op")]
    pub operator: Operator,
    pub value: ColumnValue,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Operator {
    #[serde(rename = "$eq")]
    Eq,
    #[serde(rename = "$neq")]
    Neq,
    #[serde(rename = "$gt")]
    Gt,
    #[serde(rename = "$lt")]
    Lt,
    #[serde(rename = "$gte")]
    Gte,
    #[serde(rename = "$lte")]
    Lte,
}

// newtype structs..

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, derive_more::Display)]
#[serde(into = "String")]
#[serde(from = "String")]
pub struct TableName(pub SmolStr);

impl From<&str> for TableName {
    fn from(value: &str) -> Self {
        TableName::new(value)
    }
}

impl From<String> for TableName {
    fn from(value: String) -> Self {
        TableName::new(&value)
    }
}

impl From<TableName> for String {
    fn from(val: TableName) -> Self {
        val.0.to_string()
    }
}

impl TableName {
    pub fn new(value: &str) -> Self {
        Self(SmolStr::new(value))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, derive_more::Display, Eq, Hash)]
#[serde(into = "String")]
#[serde(from = "String")]
pub struct ColumnName(pub SmolStr);

impl From<&str> for ColumnName {
    fn from(value: &str) -> Self {
        ColumnName::new(value)
    }
}

impl From<String> for ColumnName {
    fn from(value: String) -> Self {
        ColumnName::new(&value)
    }
}

impl From<ColumnName> for String {
    fn from(val: ColumnName) -> Self {
        val.0.to_string()
    }
}

impl ColumnName {
    pub fn new(value: &str) -> Self {
        Self(SmolStr::new(value))
    }
}
