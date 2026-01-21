//! PyuDB CLI
//!
//! Interactive command-line interface for PyuDB

use std::io::{self, Write};
use std::sync::Arc;

use anyhow::Result;

use pyudb::storage::{StorageEngine, StorageConfig};
use pyudb::query::{QueryExecutor, QueryResult, Value};
use pyudb::txn::TransactionManager;

fn print_banner() {
    println!(r#"
  ____              ____  ____  
 |  _ \ _   _ _   _|  _ \| __ ) 
 | |_) | | | | | | | | | |  _ \ 
 |  __/| |_| | |_| | |_| | |_) |
 |_|    \__, |\__,_|____/|____/ 
        |___/                   
    "#);
    println!("PyuDB - A Distributed Database");
    println!("Version: {}", env!("CARGO_PKG_VERSION"));
    println!("Type 'help' for available commands, 'exit' to quit.");
    println!();
}

fn print_help() {
    println!("Available commands:");
    println!("  help              - Show this help message");
    println!("  exit, quit, \\q    - Exit the CLI");
    println!("  status            - Show database status");
    println!();
    println!("SQL commands:");
    println!("  SELECT ...        - Query data");
    println!("  INSERT ...        - Insert data");
    println!("  UPDATE ...        - Update data");
    println!("  DELETE ...        - Delete data");
    println!("  CREATE TABLE ...  - Create a table");
    println!("  DROP TABLE ...    - Drop a table");
    println!();
    println!("Transaction commands:");
    println!("  BEGIN             - Start a transaction");
    println!("  COMMIT            - Commit the current transaction");
    println!("  ROLLBACK          - Rollback the current transaction");
    println!();
}

fn format_result(result: &QueryResult) -> String {
    match result {
        QueryResult::Select { columns, rows } => {
            if rows.is_empty() {
                return "Empty set (0 rows)".to_string();
            }

            // Calculate column widths
            let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
            for row in rows {
                for (i, val) in row.iter().enumerate() {
                    let val_str = format_value(val);
                    if i < widths.len() {
                        widths[i] = widths[i].max(val_str.len());
                    }
                }
            }

            // Build table
            let mut output = String::new();

            // Header
            let header: Vec<String> = columns
                .iter()
                .zip(&widths)
                .map(|(c, w)| format!("{:width$}", c, width = *w))
                .collect();
            output.push_str(&format!("| {} |\n", header.join(" | ")));

            // Separator
            let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
            output.push_str(&format!("|-{}-|\n", sep.join("-+-")));

            // Rows
            for row in rows {
                let row_strs: Vec<String> = row
                    .iter()
                    .zip(&widths)
                    .map(|(v, w)| {
                        let val_str = format_value(v);
                        format!("{:width$}", val_str, width = *w)
                    })
                    .collect();
                output.push_str(&format!("| {} |\n", row_strs.join(" | ")));
            }

            output.push_str(&format!("\n{} row(s)\n", rows.len()));
            output
        }
        QueryResult::Insert { rows_affected } => {
            format!("INSERT OK, {} row(s) affected", rows_affected)
        }
        QueryResult::Update { rows_affected } => {
            format!("UPDATE OK, {} row(s) affected", rows_affected)
        }
        QueryResult::Delete { rows_affected } => {
            format!("DELETE OK, {} row(s) affected", rows_affected)
        }
        QueryResult::CreateTable { table_name } => {
            format!("Table '{}' created", table_name)
        }
        QueryResult::DropTable { table_name } => {
            format!("Table '{}' dropped", table_name)
        }
        QueryResult::Transaction { action } => {
            format!("Transaction: {}", action)
        }
    }
}

fn format_value(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::String(s) => s.clone(),
        Value::Blob(b) => format!("<blob {} bytes>", b.len()),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    print_banner();

    // Create data directory
    let data_dir = std::path::PathBuf::from("./pyudb_data");
    std::fs::create_dir_all(&data_dir)?;

    // Initialize storage
    let config = StorageConfig {
        data_dir,
        wal_segment_size: 64 * 1024 * 1024,
        sync_on_write: true,
    };

    let storage = Arc::new(StorageEngine::new(config)?);
    let txn_manager = Arc::new(TransactionManager::new(Arc::clone(&storage)));
    let mut executor = QueryExecutor::with_txn_manager(storage, txn_manager);

    println!("Database initialized. Ready for queries.\n");

    // REPL loop
    loop {
        print!("pyudb> ");
        io::stdout().flush()?;

        let mut input = String::new();
        if io::stdin().read_line(&mut input)? == 0 {
            // EOF
            println!();
            break;
        }

        let input = input.trim();
        if input.is_empty() {
            continue;
        }

        // Handle special commands
        match input.to_lowercase().as_str() {
            "help" | "\\h" => {
                print_help();
                continue;
            }
            "exit" | "quit" | "\\q" => {
                println!("Goodbye!");
                break;
            }
            "status" | "\\s" => {
                println!("Status: Running");
                println!("Mode: Single node (embedded)");
                continue;
            }
            _ => {}
        }

        // Execute SQL query
        match executor.execute(input) {
            Ok(result) => {
                println!("{}", format_result(&result));
            }
            Err(e) => {
                println!("Error: {}", e);
            }
        }
        println!();
    }

    Ok(())
}
