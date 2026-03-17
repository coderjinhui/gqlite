use std::sync::Mutex;
use gqlite_core::Database;

pub struct AppState {
    pub db: Mutex<Option<Database>>,
    pub db_path: Mutex<Option<String>>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            db: Mutex::new(None),
            db_path: Mutex::new(None),
        }
    }
}
