use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

use gqlite_core::Database;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let path = args.get(1).map(|s| s.as_str()).unwrap_or("default.graph");

    let db = match Database::open(path) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("failed to open database: {e}");
            std::process::exit(1);
        }
    };

    println!("gqlite v0.1.0");
    println!("Connected to: {}", db.path().display());
    println!("Type .help for usage hints.\n");

    let mut rl = DefaultEditor::new().expect("failed to create editor");

    // Load history
    let history_path = dirs_home().join(".gqlite_history");
    let _ = rl.load_history(&history_path);

    let mut buf = String::new();

    loop {
        let prompt = if buf.is_empty() { "gqlite> " } else { "   ...> " };
        match rl.readline(prompt) {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                // Dot commands (only at start of input)
                if buf.is_empty() && trimmed.starts_with('.') {
                    let _ = rl.add_history_entry(trimmed);
                    if handle_dot_command(trimmed, &db) {
                        break;
                    }
                    continue;
                }

                buf.push_str(trimmed);
                buf.push(' ');

                // Multi-line: continue until we see a semicolon
                if !trimmed.ends_with(';') {
                    continue;
                }

                // Remove trailing semicolon and whitespace
                let query = buf.trim().trim_end_matches(';').trim().to_string();
                let _ = rl.add_history_entry(&query);
                buf.clear();

                if query.is_empty() {
                    continue;
                }

                match db.query(&query) {
                    Ok(result) => {
                        if result.is_empty() {
                            println!("OK");
                        } else {
                            let col_names = result.column_names();
                            if !col_names.is_empty() {
                                println!("{}", col_names.join(" | "));
                                println!(
                                    "{}",
                                    "-".repeat(
                                        col_names.iter().map(|c| c.len()).sum::<usize>()
                                            + col_names.len().saturating_sub(1) * 3,
                                    )
                                );
                            }
                            for row in result.rows() {
                                println!("{}", row);
                            }
                            println!("({} rows)", result.num_rows());
                        }
                    }
                    Err(e) => {
                        eprintln!("Error: {e}");
                    }
                }
            }
            Err(ReadlineError::Eof | ReadlineError::Interrupted) => {
                break;
            }
            Err(e) => {
                eprintln!("Error: {e}");
                break;
            }
        }
    }

    let _ = rl.save_history(&history_path);
}

/// Handle dot commands. Returns true if the REPL should exit.
fn handle_dot_command(cmd: &str, db: &Database) -> bool {
    let parts: Vec<&str> = cmd.splitn(2, ' ').collect();
    let command = parts[0];
    let arg = parts.get(1).map(|s| s.trim());

    match command {
        ".quit" | ".exit" => return true,

        ".help" => {
            println!(".help             Show this message");
            println!(".quit             Exit gqlite");
            println!(".tables           List all tables");
            println!(".schema [TABLE]   Show table schema");
        }

        ".tables" => {
            let node_tables = db.node_table_names();
            let rel_tables = db.rel_table_names();
            if node_tables.is_empty() && rel_tables.is_empty() {
                println!("(no tables)");
            } else {
                for name in &node_tables {
                    println!("  {} (node)", name);
                }
                for name in &rel_tables {
                    println!("  {} (rel)", name);
                }
            }
        }

        ".schema" => {
            if let Some(table_name) = arg {
                match db.table_schema(table_name) {
                    Some(cols) => {
                        println!("Table: {}", table_name);
                        for (name, dtype) in &cols {
                            println!("  {} {}", name, dtype);
                        }
                    }
                    None => {
                        eprintln!("table '{}' not found", table_name);
                    }
                }
            } else {
                // Show all schemas
                for name in db.node_table_names() {
                    if let Some(cols) = db.table_schema(&name) {
                        println!("Node table: {}", name);
                        for (cname, dtype) in &cols {
                            println!("  {} {}", cname, dtype);
                        }
                    }
                }
                for name in db.rel_table_names() {
                    if let Some(cols) = db.table_schema(&name) {
                        println!("Rel table: {}", name);
                        for (cname, dtype) in &cols {
                            println!("  {} {}", cname, dtype);
                        }
                    }
                }
            }
        }

        _ => {
            eprintln!("unknown command: {}. Try .help", command);
        }
    }

    false
}

/// Get the user's home directory.
fn dirs_home() -> std::path::PathBuf {
    std::env::var("HOME")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("."))
}
