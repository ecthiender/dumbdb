use crate::error::{CreateTableError, ParseError};
use dumbdb::{
    CreateTableCommand, DropTableCommand, GetItemCommand, PutItemCommand, TableDefinition,
};
use serde_json;

#[derive(Debug)]
pub enum Command {
    CreateTable(CreateTableCommand),
    DropTable(DropTableCommand),
    Get(GetItemCommand),
    Put(PutItemCommand),
    // Filter(FilterItemCommand),
}

pub fn parse_command(input: String) -> Result<Command, ParseError> {
    let input = input.trim();
    // get <table-name> <key>
    // put <table-name> <json-val>
    // create-table <table-name> <json-val> | *<json-val> = {"columns": [{name: str, type: Type}], "primary_key": str}
    // LATER: create-table authors [id Integer, name Text] [primary key id]
    // drop-table <table-name>
    let (command_name, command_args) = take_while(input, ' ');

    match command_name {
        "get" => Ok(Command::Get(parse_get(command_args)?)),
        "put" => Ok(Command::Put(parse_put(command_args)?)),
        "create-table" => Ok(Command::CreateTable(parse_create_table(command_args)?)),
        "drop-table" => Ok(Command::DropTable(parse_drop_table(command_args))),
        _ => Err(ParseError::UnknownCommand(command_name.to_string())),
    }
}

fn take_while(s: &str, c: char) -> (&str, &str) {
    s.find(c)
        .map(|index| (&s[..index], &s[index + 1..]))
        .unwrap_or((s, ""))
}

fn parse_get(tokens: &str) -> Result<GetItemCommand, ParseError> {
    match tokens.split_whitespace().collect::<Vec<_>>().as_slice() {
        [name, key] => Ok(GetItemCommand {
            table_name: (*name).into(),
            key: (*key).into(),
        }),
        _ => Err(ParseError::Get),
    }
}

fn parse_put(tokens: &str) -> Result<PutItemCommand, ParseError> {
    let (table_name, args) = take_while(tokens, ' ');
    let item = serde_json::from_str(args).map_err(|e| ParseError::Put(e))?;
    Ok(PutItemCommand {
        table_name: table_name.into(),
        item,
    })
}

fn parse_create_table(tokens: &str) -> Result<CreateTableCommand, ParseError> {
    let (name, rest) = take_while(tokens, ' ');
    let json_val = serde_json::from_str(rest).map_err(|e| CreateTableError::InvalidJson(e))?;

    let cols = get_from_json_object(&json_val, "columns")
        .ok_or_else(|| CreateTableError::ColumnsNotFound)?;
    let columns = serde_json::from_value(cols).map_err(|e| CreateTableError::InvalidJson(e))?;

    let pk = get_from_json_object(&json_val, "primary_key")
        .ok_or_else(|| CreateTableError::PrimaryKeyNotFound)?;
    let primary_key = serde_json::from_value(pk).map_err(|e| CreateTableError::InvalidJson(e))?;

    Ok(TableDefinition {
        name: name.into(),
        columns,
        primary_key,
    })
}

fn get_from_json_object(json_val: &serde_json::Value, key: &str) -> Option<serde_json::Value> {
    json_val.as_object().and_then(|obj| obj.get(key)).cloned()
}

fn parse_drop_table(args: &str) -> DropTableCommand {
    DropTableCommand {
        table_name: args.into(),
    }
}
