mod error;
mod execute;
mod parse;

use clap::Parser;
use dumbdb::Database;
use error::AppError;
use execute::{execute_command, OurRecord, Output};
use parse::parse_command;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::{
    fmt::{Debug, Display},
    path::PathBuf,
};

const PROMPT: &str = "%~dumbdb> ";
const HISTORY_FILE: &str = "history.txt";

/// REPL config options
#[derive(clap::Parser, Debug)]
#[command(version, about, long_about = None)]
struct ReplOptions {
    /// Path to a database directory. The directory can be empty but it should exist.
    #[arg(short, long)]
    database_path: String,

    /// Prompt for the REPL.
    #[arg(long, default_value = PROMPT)]
    prompt: String,

    /// Filepath to store history.
    #[arg(long, default_value = HISTORY_FILE)]
    history_file: PathBuf,
}

#[tokio::main]
async fn main() {
    println!("Welcome to DumbDB!");

    let config = ReplOptions::parse();
    println!("Loading {}", config.database_path);

    let mut db = Database::new(&config.database_path)
        .await
        .expect("Failed to initialize the database.");
    let mut editor = DefaultEditor::new().expect("Failed to create TUI editor");
    if editor.load_history(&config.history_file).is_err() {
        println!("No previous history.");
    }
    loop {
        // Display a prompt and read input
        match editor.readline(&config.prompt) {
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
    let _ = editor.save_history(&config.history_file);
}

async fn eval(db: &mut Database, input: String) -> Result<Output<OurRecord>, AppError> {
    let command = parse_command(input)?;
    // println!("Parsed command: {:?}", command);
    let output = execute_command(db, command).await?;
    Ok(output.fmap(OurRecord))
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
