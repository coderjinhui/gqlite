use gqlite_core::Database;

fn main() {
    // Open (or create) a database
    let db = Database::open("example.graph").expect("failed to open database");
    println!("Opened database at: {}", db.path().display());

    // Run a query (no-op while engine is stubbed)
    let result = db.query("MATCH (n:Person) RETURN n").unwrap();
    println!("Rows returned: {}", result.rows.len());
}
