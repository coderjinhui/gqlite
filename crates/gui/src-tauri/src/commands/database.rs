use serde::Serialize;
use tauri::State;

use crate::state::AppState;
use gqlite_core::Database;

#[derive(Serialize)]
pub struct DbInfo {
    pub path: String,
    pub read_only: bool,
    pub node_table_count: usize,
    pub rel_table_count: usize,
}

#[tauri::command]
pub fn open_database(path: String, state: State<AppState>) -> Result<DbInfo, String> {
    let db = Database::open(&path).map_err(|e| e.to_string())?;
    let info = DbInfo {
        path: path.clone(),
        read_only: db.config().read_only,
        node_table_count: db.node_table_names().len(),
        rel_table_count: db.rel_table_names().len(),
    };
    *state.db.lock().unwrap() = Some(db);
    *state.db_path.lock().unwrap() = Some(path);
    Ok(info)
}

#[tauri::command]
pub fn close_database(state: State<AppState>) -> Result<(), String> {
    *state.db.lock().unwrap() = None;
    *state.db_path.lock().unwrap() = None;
    Ok(())
}

#[tauri::command]
pub fn get_database_info(state: State<AppState>) -> Result<DbInfo, String> {
    let db_guard = state.db.lock().unwrap();
    let db = db_guard.as_ref().ok_or("No database is open")?;
    let path_guard = state.db_path.lock().unwrap();
    Ok(DbInfo {
        path: path_guard.clone().unwrap_or_default(),
        read_only: db.config().read_only,
        node_table_count: db.node_table_names().len(),
        rel_table_count: db.rel_table_names().len(),
    })
}

#[tauri::command]
pub fn checkpoint(state: State<AppState>) -> Result<(), String> {
    let db_guard = state.db.lock().unwrap();
    let db = db_guard.as_ref().ok_or("No database is open")?;
    db.checkpoint().map_err(|e| e.to_string())
}
