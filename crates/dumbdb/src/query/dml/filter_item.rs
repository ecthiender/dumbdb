use futures::StreamExt;
use serde::{Deserialize, Serialize};

use crate::{
    catalog::Catalog,
    query::{
        error::QueryError,
        types::{ColumnDefinition, ColumnValue, Expression, Operator, TableName},
    },
    storage::Tuple,
    table::TableBufferError,
};

use super::{common::build_record, Record};

#[derive(Debug, Serialize, Deserialize)]
pub struct FilterItemCommand {
    pub table_name: TableName,
    pub filter: Expression,
}

pub async fn filter_item(
    command: FilterItemCommand,
    catalog: &Catalog,
) -> Result<Vec<Record>, QueryError> {
    match catalog.get_table(&command.table_name) {
        None => Err(QueryError::TableNotFound(command.table_name)),
        Some(table) => {
            let mut res = vec![];
            let mut stream = table
                .table_buffer
                .block
                .get_reader()
                .await
                .map_err(TableBufferError::StorageError)?;
            while let Some(tuple) = stream.next().await {
                let tuple = tuple.map_err(TableBufferError::StorageError)?;
                if evaluate_expression(&table.columns, &command.filter, &tuple) {
                    res.push(build_record(&table.columns, tuple));
                }
            }
            Ok(res)
        }
    }
}

/// Evaluate an `Expression` to be true or false, given a `Tuple`.
fn evaluate_expression(
    columns: &[ColumnDefinition],
    expression: &Expression,
    tuple: &Tuple,
) -> bool {
    match expression {
        Expression::ColumnComparison(comparison) => {
            let col_pos = columns
                .iter()
                .position(|col_def| col_def.name == comparison.column)
                .unwrap();
            // .with_context(|| "Internal Error: Column must exist.")?;
            let column_value = tuple[col_pos].clone().unwrap();
            // .with_context(|| "invariant violation: column value not found in tuple.")?;
            evaluate_binary_operator(&comparison.operator, &column_value, &comparison.value)
        }
        Expression::And(expressions) => expressions
            .iter()
            .all(|exp| evaluate_expression(columns, exp, tuple)),
        Expression::Or(expressions) => expressions
            .iter()
            .any(|exp| evaluate_expression(columns, exp, tuple)),
        Expression::Not(expression) => !evaluate_expression(columns, expression, tuple),
    }
}

fn evaluate_binary_operator(operator: &Operator, val_a: &ColumnValue, val_b: &ColumnValue) -> bool {
    match operator {
        Operator::Eq => val_a == val_b,
        Operator::Neq => val_a != val_b,
        Operator::Gt => val_a > val_b,
        Operator::Lt => val_a < val_b,
        Operator::Gte => val_a >= val_b,
        Operator::Lte => val_a <= val_b,
    }
}
