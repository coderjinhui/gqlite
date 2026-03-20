pub mod commands;
pub mod state;

use state::AppState;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .manage(AppState::new())
        .invoke_handler(tauri::generate_handler![
            commands::database::open_database,
            commands::database::close_database,
            commands::database::get_database_info,
            commands::database::checkpoint,
            commands::query::execute_query,
            commands::schema::get_tables,
            commands::schema::get_table_schema,
            commands::schema::get_table_data,
            commands::graph::get_graph_data,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
