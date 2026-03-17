// TypeScript type definitions matching Rust backend responses

export interface DbInfo {
  path: string;
  read_only: boolean;
  node_table_count: number;
  rel_table_count: number;
}

export interface ColumnDesc {
  name: string;
  data_type: string;
}

export interface QueryResponse {
  columns: ColumnDesc[];
  rows: any[][];
  row_count: number;
  elapsed_ms: number;
}

export interface TableInfo {
  name: string;
  row_count: number;
  column_count: number;
}

export interface TablesResponse {
  node_tables: TableInfo[];
  rel_tables: TableInfo[];
}

export interface GraphNode {
  id: string;
  label: string;
  properties: Record<string, any>;
}

export interface GraphEdge {
  source: string;
  target: string;
  label: string;
  properties: Record<string, any>;
}

export interface GraphData {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

export type Page = "query" | "tables" | "graph";

export interface QueryTab {
  id: string;
  name: string;
  content: string;
  result: QueryResponse | null;
}
