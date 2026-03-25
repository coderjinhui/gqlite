import { invoke } from "@tauri-apps/api/core";
import type {
  DbInfo,
  QueryResponse,
  TablesResponse,
  ColumnDesc,
  GraphData,
} from "./types";

export async function openDatabase(path: string): Promise<DbInfo> {
  return invoke("open_database", { path });
}

export async function closeDatabase(): Promise<void> {
  return invoke("close_database");
}

export async function getDatabaseInfo(): Promise<DbInfo> {
  return invoke("get_database_info");
}

export async function doCheckpoint(): Promise<void> {
  return invoke("checkpoint");
}

export async function executeQuery(query: string): Promise<QueryResponse> {
  return invoke("execute_query", { query });
}

export async function getTables(): Promise<TablesResponse> {
  return invoke("get_tables");
}

export async function getTableSchema(
  tableName: string
): Promise<ColumnDesc[]> {
  return invoke("get_table_schema", { tableName });
}

export async function getTableData(
  tableName: string,
  limit: number,
  offset: number
): Promise<QueryResponse> {
  return invoke("get_table_data", { tableName, limit, offset });
}

export async function getGraphData(
  nodeTable: string,
  relTable: string | null,
  limit: number
): Promise<GraphData> {
  return invoke("get_graph_data", { nodeTable, relTable, limit });
}
