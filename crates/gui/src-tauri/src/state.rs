use gqlite_core::Database;
use std::sync::Mutex;

pub struct AppState {
    pub db: Mutex<Option<Database>>,
    pub db_path: Mutex<Option<String>>,
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

impl AppState {
    pub fn new() -> Self {
        Self { db: Mutex::new(None), db_path: Mutex::new(None) }
    }
}
