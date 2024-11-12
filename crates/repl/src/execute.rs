use crate::parse::Command;
use dumbdb::{error::QueryError, Database, Record};
use std::fmt::{Debug, Display};

#[derive(Debug)]
pub enum Output<T> {
    Done,
    ResultOne(Option<T>),
    ResultMany(Vec<T>),
}

impl<T> Output<T> {
    pub fn fmap<U, F: Fn(T) -> U>(self, f: F) -> Output<U> {
        match self {
            Self::Done => Output::Done,
            Self::ResultOne(r) => Output::ResultOne(r.map(f)),
            Self::ResultMany(r) => Output::ResultMany(r.into_iter().map(f).collect()),
        }
    }
}

#[derive(Debug)]
pub struct OurRecord(pub Record);

impl Display for OurRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (key, value) in &self.0 {
            let _ = match value {
                None => writeln!(f, "{}: null", key),
                Some(val) => writeln!(f, "{}: {}", key, val),
            };
        }
        write!(f, "")
    }
}

pub async fn execute_command(
    db: &mut Database,
    cmd: Command,
) -> Result<Output<Record>, QueryError> {
    match cmd {
        Command::CreateTable(cmd) => {
            db.create_table(cmd).await?;
            println!("Table created.");
            Ok(Output::Done)
        }
        Command::DropTable(cmd) => {
            db.drop_table(cmd).await?;
            println!("Table deleted.");
            Ok(Output::Done)
        }
        Command::Get(cmd) => {
            let r = db.get_item(cmd).await?;
            Ok(Output::ResultOne(r))
        }
        Command::Put(cmd) => {
            db.put_item(cmd).await?;
            println!("Inserted");
            Ok(Output::Done)
        }
        Command::ListTables => {
            let tables = db.list_tables();
            println!("Tables");
            println!("------");
            for table in tables {
                println!("{}", table);
            }
            println!();
            Ok(Output::Done)
        }
    }
}
