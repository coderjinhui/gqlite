import { writable, derived, get } from "svelte/store";
import type { DbInfo, TablesResponse, Page } from "./types";

// ── Locale ──────────────────────────────────────────────────────

type Locale = "en" | "zh";

const translations: Record<Locale, Record<string, string>> = {
  en: {
    "app.title": "gqlite",
    "sidebar.database": "Database",
    "sidebar.no_database": "No database open",
    "sidebar.open": "Open",
    "sidebar.create": "New",
    "sidebar.close": "Close",
    "sidebar.checkpoint": "Checkpoint",
    "sidebar.tables": "Tables",
    "sidebar.node_tables": "Node Tables",
    "sidebar.rel_tables": "Relationship Tables",
    "sidebar.language": "Language",
    "tab.query": "Query",
    "tab.tables": "Tables",
    "tab.graph": "Graph",
    "query.placeholder": "-- Enter GQL query here\n-- Press Ctrl+Enter to execute",
    "query.execute": "Execute",
    "query.clear": "Clear",
    "query.new_tab": "New Tab",
    "result.columns": "Columns",
    "result.rows": "rows",
    "result.elapsed": "Elapsed",
    "result.no_results": "No results",
    "result.empty": "Query returned no rows",
    "table.select": "Select a table",
    "table.schema": "Schema",
    "table.data": "Data",
    "table.name": "Name",
    "table.type": "Type",
    "table.rows": "Rows",
    "table.page": "Page",
    "table.prev": "Prev",
    "table.next": "Next",
    "graph.node_table": "Node Table",
    "graph.rel_table": "Relationship Table",
    "graph.none": "(None)",
    "graph.limit": "Node Limit",
    "graph.load": "Load Graph",
    "graph.layout": "Layout",
    "graph.force": "Force",
    "graph.dagre": "Dagre",
    "graph.no_data": "Load graph data to visualize",
    "status.connected": "Connected",
    "status.disconnected": "Not connected",
    "status.read_only": "Read-only",
    "theme.light": "Light",
    "theme.dark": "Dark",
    "error.title": "Error",
  },
  zh: {
    "app.title": "gqlite",
    "sidebar.database": "数据库",
    "sidebar.no_database": "未打开数据库",
    "sidebar.open": "打开",
    "sidebar.create": "新建",
    "sidebar.close": "关闭",
    "sidebar.checkpoint": "检查点",
    "sidebar.tables": "表",
    "sidebar.node_tables": "节点表",
    "sidebar.rel_tables": "关系表",
    "sidebar.language": "语言",
    "tab.query": "查询",
    "tab.tables": "表浏览",
    "tab.graph": "图可视化",
    "query.placeholder": "-- 在此输入 GQL 查询\n-- 按 Ctrl+Enter 执行",
    "query.execute": "执行",
    "query.clear": "清空",
    "query.new_tab": "新标签",
    "result.columns": "列",
    "result.rows": "行",
    "result.elapsed": "耗时",
    "result.no_results": "无结果",
    "result.empty": "查询无返回行",
    "table.select": "选择一个表",
    "table.schema": "结构",
    "table.data": "数据",
    "table.name": "名称",
    "table.type": "类型",
    "table.rows": "行数",
    "table.page": "页",
    "table.prev": "上一页",
    "table.next": "下一页",
    "graph.node_table": "节点表",
    "graph.rel_table": "关系表",
    "graph.none": "（无）",
    "graph.limit": "节点上限",
    "graph.load": "加载图",
    "graph.layout": "布局",
    "graph.force": "力导向",
    "graph.dagre": "层级",
    "graph.no_data": "加载图数据以可视化",
    "status.connected": "已连接",
    "status.disconnected": "未连接",
    "status.read_only": "只读",
    "theme.light": "浅色",
    "theme.dark": "深色",
    "error.title": "错误",
  },
};

export const locale = writable<Locale>("en");

export function t(key: string): string {
  const loc = get(locale);
  return translations[loc]?.[key] ?? key;
}

export const tt = derived(locale, ($locale) => {
  return (key: string): string => {
    return translations[$locale]?.[key] ?? key;
  };
});

export function toggleLocale() {
  locale.update((l) => (l === "en" ? "zh" : "en"));
}

// ── Theme ───────────────────────────────────────────────────────

export const darkMode = writable(false);

export function toggleTheme() {
  darkMode.update((d) => {
    const next = !d;
    if (next) {
      document.documentElement.classList.add("dark");
    } else {
      document.documentElement.classList.remove("dark");
    }
    return next;
  });
}
