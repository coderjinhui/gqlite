use gqlite_cli::helper;

use std::path::PathBuf;
use std::time::Instant;

use rustyline::error::ReadlineError;
use rustyline::{CompletionType, Config, Editor};

use gqlite_core::{Database, DatabaseConfig};

const VERSION: &str = env!("CARGO_PKG_VERSION");

// ── CLI 参数 ──────────────────────────────────────────────────

struct CliArgs {
    database: String,
    read_only: bool,
}

fn parse_args() -> CliArgs {
    let args: Vec<String> = std::env::args().skip(1).collect();

    let mut database: Option<String> = None;
    let mut read_only = false;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            "-V" | "--version" => {
                println!("gqlite {VERSION}");
                std::process::exit(0);
            }
            "--read-only" => {
                read_only = true;
            }
            arg if arg.starts_with('-') => {
                eprintln!("Unknown option: {arg}");
                eprintln!("Try 'gqlite --help' for more information.");
                std::process::exit(1);
            }
            _ => {
                if database.is_some() {
                    eprintln!("Unexpected argument: {}", args[i]);
                    eprintln!("Try 'gqlite --help' for more information.");
                    std::process::exit(1);
                }
                database = Some(args[i].clone());
            }
        }
        i += 1;
    }

    let Some(database) = database else {
        eprintln!("Error: missing required argument <DATABASE>");
        eprintln!("Try 'gqlite --help' for more information.");
        std::process::exit(1);
    };

    CliArgs {
        database,
        read_only,
    }
}

fn print_usage() {
    println!("gqlite {VERSION} — lightweight embeddable graph database");
    println!();
    println!("USAGE:");
    println!("    gqlite [OPTIONS] <DATABASE>");
    println!();
    println!("ARGS:");
    println!("    <DATABASE>    Path to the .graph database file (created if not exists)");
    println!();
    println!("OPTIONS:");
    println!("    --read-only   Open database in read-only mode");
    println!("    -h, --help    Print help information");
    println!("    -V, --version Print version information");
    println!();
    println!("EXAMPLES:");
    println!("    gqlite mydb              Open or create mydb.graph");
    println!("    gqlite mydb.graph        Open or create mydb.graph");
    println!("    gqlite --read-only mydb  Open mydb.graph in read-only mode");
}

// ── main ───────────────────────────────────────────────────────

fn main() {
    let cli = parse_args();
    let db_path = ensure_graph_extension(&cli.database);

    let config = DatabaseConfig {
        read_only: cli.read_only,
        ..DatabaseConfig::default()
    };

    let mut db = match Database::open_with_config(&db_path, config) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Failed to open database '{}': {e}", db_path.display());
            std::process::exit(1);
        }
    };

    println!("gqlite v{VERSION}");
    println!("Connected to: {}", db.path().display());
    if cli.read_only {
        println!("(read-only mode)");
    }
    println!("Type .help for usage hints.\n");

    let config = Config::builder()
        .completion_type(CompletionType::List)
        .build();
    let mut rl = Editor::with_config(config).expect("failed to create editor");
    rl.set_helper(Some(helper::GqliteHelper));

    let history_path = home_dir().join(".gqlite_history");
    let _ = rl.load_history(&history_path);

    let mut buf = String::new();

    loop {
        let prompt = if buf.is_empty() {
            "gqlite> "
        } else {
            "   ...> "
        };
        match rl.readline(prompt) {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                // Dot commands — only when buffer is empty (start of input)
                if buf.is_empty() && trimmed.starts_with('.') {
                    let _ = rl.add_history_entry(trimmed);
                    match handle_dot_command(trimmed, &mut db) {
                        DotResult::Continue => continue,
                        DotResult::Quit => break,
                    }
                }

                buf.push_str(trimmed);
                buf.push(' ');

                // Multi-line: accumulate until semicolon
                if !trimmed.ends_with(';') {
                    continue;
                }

                let query = buf.trim().trim_end_matches(';').trim().to_string();
                let _ = rl.add_history_entry(&query);
                buf.clear();

                if query.is_empty() {
                    continue;
                }

                execute_and_print(&db, &query);
            }
            Err(ReadlineError::Eof | ReadlineError::Interrupted) => break,
            Err(e) => {
                eprintln!("Error: {e}");
                break;
            }
        }
    }

    let _ = rl.save_history(&history_path);
    println!("Bye!");
}

// ── 查询执行与输出格式化 ──────────────────────────────────────

fn execute_and_print(db: &Database, query: &str) {
    let start = Instant::now();

    match db.execute(query) {
        Ok(result) => {
            let elapsed = start.elapsed();
            if result.is_empty() {
                println!("OK ({:.3}s)", elapsed.as_secs_f64());
            } else {
                print_result_table(&result);
                println!(
                    "({} row{}, {:.3}s)",
                    result.num_rows(),
                    if result.num_rows() == 1 { "" } else { "s" },
                    elapsed.as_secs_f64(),
                );
            }
        }
        Err(e) => {
            eprintln!("Error: {e}");
        }
    }
}

/// 对齐列宽的表格输出
fn print_result_table(result: &gqlite_core::QueryResult) {
    let col_names = result.column_names();
    if col_names.is_empty() {
        return;
    }

    // 计算每列最大宽度
    let mut widths: Vec<usize> = col_names.iter().map(|c| c.len()).collect();
    for row in result.rows() {
        for (i, val) in row.values.iter().enumerate() {
            let w = val.to_string().len();
            if w > widths[i] {
                widths[i] = w;
            }
        }
    }

    // 表头
    let header: Vec<String> = col_names
        .iter()
        .enumerate()
        .map(|(i, name)| format!("{:width$}", name, width = widths[i]))
        .collect();
    println!("{}", header.join(" | "));

    // 分隔线
    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    println!("{}", sep.join("-+-"));

    // 数据行
    for row in result.rows() {
        let cells: Vec<String> = row
            .values
            .iter()
            .enumerate()
            .map(|(i, val)| format!("{:width$}", val, width = widths[i]))
            .collect();
        println!("{}", cells.join(" | "));
    }
}

// ── Dot 命令处理 ──────────────────────────────────────────────

enum DotResult {
    Continue,
    Quit,
}

fn handle_dot_command(cmd: &str, db: &mut Database) -> DotResult {
    let parts: Vec<&str> = cmd.splitn(2, ' ').collect();
    let command = parts[0];
    let arg = parts.get(1).map(|s| s.trim()).filter(|s| !s.is_empty());

    match command {
        ".quit" | ".exit" => return DotResult::Quit,

        ".help" => {
            println!("Dot commands:");
            println!("  .help              Show this message");
            println!("  .quit / .exit      Exit gqlite");
            println!("  .tables            List all tables");
            println!("  .schema [TABLE]    Show table schema");
            println!("  .database          Show current database info");
            println!("  .open <PATH>       Switch to another database file");
            println!("  .checkpoint        Trigger WAL checkpoint");
            println!();
            println!("GQL examples:");
            println!("  CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY(id));");
            println!("  CREATE (n:Person {{id: 1, name: 'Alice'}});");
            println!("  MATCH (n:Person) RETURN n.id, n.name;");
            println!("  MATCH (n:Person) WHERE n.id = 1 SET n.name = 'Bob';");
            println!("  MATCH (n:Person) WHERE n.id = 1 DELETE n;");
        }

        ".tables" => {
            let node_tables = db.node_table_names();
            let rel_tables = db.rel_table_names();
            if node_tables.is_empty() && rel_tables.is_empty() {
                println!("(no tables)");
            } else {
                for name in &node_tables {
                    println!("  {name} (node)");
                }
                for name in &rel_tables {
                    println!("  {name} (rel)");
                }
            }
        }

        ".schema" => {
            if let Some(table_name) = arg {
                match db.table_schema(table_name) {
                    Some(cols) => {
                        println!("Table: {table_name}");
                        for (name, dtype) in &cols {
                            println!("  {name} {dtype}");
                        }
                    }
                    None => eprintln!("Table '{table_name}' not found"),
                }
            } else {
                let mut any = false;
                for name in db.node_table_names() {
                    if let Some(cols) = db.table_schema(&name) {
                        println!("Node table: {name}");
                        for (cname, dtype) in &cols {
                            println!("  {cname} {dtype}");
                        }
                        any = true;
                    }
                }
                for name in db.rel_table_names() {
                    if let Some(cols) = db.table_schema(&name) {
                        println!("Rel table: {name}");
                        for (cname, dtype) in &cols {
                            println!("  {cname} {dtype}");
                        }
                        any = true;
                    }
                }
                if !any {
                    println!("(no tables)");
                }
            }
        }

        ".database" => {
            println!("Path:        {}", db.path().display());
            println!("Read-only:   {}", db.config().read_only);
            println!("Node tables: {}", db.node_table_names().len());
            println!("Rel tables:  {}", db.rel_table_names().len());
        }

        ".open" => {
            let Some(path_str) = arg else {
                eprintln!("Usage: .open <PATH>");
                return DotResult::Continue;
            };
            let new_path = ensure_graph_extension(path_str);
            match Database::open(&new_path) {
                Ok(new_db) => {
                    *db = new_db;
                    println!("Connected to: {}", db.path().display());
                }
                Err(e) => {
                    eprintln!("Failed to open '{}': {e}", new_path.display());
                }
            }
        }

        ".checkpoint" => match db.checkpoint() {
            Ok(()) => println!("Checkpoint completed."),
            Err(e) => eprintln!("Checkpoint failed: {e}"),
        },

        _ => {
            eprintln!("Unknown command: {command}. Try .help");
        }
    }

    DotResult::Continue
}

// ── 辅助函数 ──────────────────────────────────────────────────

/// 自动补全 .graph 扩展名，并转为绝对路径
fn ensure_graph_extension(path: &str) -> PathBuf {
    let p = PathBuf::from(path);
    let p = if p.extension().is_some() { p } else { p.with_extension("graph") };
    if p.is_absolute() {
        p
    } else {
        std::env::current_dir().unwrap_or_default().join(p)
    }
}

fn home_dir() -> PathBuf {
    std::env::var("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("."))
}
