mod error;
mod execute;
mod parse;

use dumbdb::Database;
use error::AppError;
use execute::{execute_command, OurRecord, Output};
use parse::parse_command;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::fmt::{Debug, Display};
use tokio;

const PROMPT: &str = "%~dumbdb> ";
const DB_PATH: &str = "./data/dumbdb";

#[tokio::main]
async fn main() {
    println!("Welcome to DumbDB!");

    println!("Loading {}", DB_PATH);
    let mut db = Database::new(DB_PATH)
        .await
        .expect("Failed to initialize the database.");
    let mut editor = DefaultEditor::new().expect("Failed to create TUI editor");
    if editor.load_history("history.txt").is_err() {
        println!("No previous history.");
    }
    loop {
        // Display a prompt and read input
        match editor.readline(PROMPT) {
            Ok(line) => {
                // Add line to history
                let _ = editor.add_history_entry(line.as_str());
                // Process the input
                if line == "exit" {
                    break;
                }
                match eval(&mut db, line).await {
                    Err(err) => {
                        println!("{}", err);
                    }
                    Ok(res) => {
                        pretty_print_output(res);
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                println!("Exit.");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                println!("Exit.");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    let _ = editor.save_history("history.txt");
}

async fn eval(db: &mut Database, input: String) -> Result<Output<OurRecord>, AppError> {
    let command = parse_command(input)?;
    // println!("Parsed command: {:?}", command);
    let output = execute_command(db, command).await?;
    Ok(output.fmap(|record| OurRecord(record)))
}

fn pretty_print_output<T: Display + Debug>(output: Output<T>) {
    match output {
        Output::Done => (),
        Output::ResultOne(r) => match r {
            None => (),
            Some(d) => {
                println!("Result");
                println!("------");
                println!("{}", d)
            }
        },
        Output::ResultMany(ds) => {
            println!("Result");
            println!("------");
            for d in ds {
                println!("{}", d);
            }
        }
    }
}
