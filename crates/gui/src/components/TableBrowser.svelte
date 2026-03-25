<script lang="ts">
  import type { TablesResponse, ColumnDesc, QueryResponse } from "$lib/types";
  import { getTableSchema, getTableData } from "$lib/api";
  import { tt } from "$lib/i18n";
  import ResultTable from "./ResultTable.svelte";

  interface Props {
    tables: TablesResponse;
  }

  let { tables }: Props = $props();

  let selectedTable = $state<string>("");
  let schema = $state<ColumnDesc[]>([]);
  let tableData = $state<QueryResponse | null>(null);
  let page = $state(0);
  let pageSize = 50;
  let activeView = $state<"schema" | "data">("data");

  let allTables = $derived([
    ...tables.node_tables.map((t) => ({ ...t, kind: "node" as const })),
    ...tables.rel_tables.map((t) => ({ ...t, kind: "rel" as const })),
  ]);

  async function loadTable(name: string) {
    selectedTable = name;
    page = 0;
    try {
      schema = await getTableSchema(name);
      tableData = await getTableData(name, pageSize, 0);
    } catch (e: any) {
      schema = [];
      tableData = null;
    }
  }

  async function loadPage(p: number) {
    if (!selectedTable) return;
    page = p;
    try {
      tableData = await getTableData(selectedTable, pageSize, p * pageSize);
    } catch (e: any) {
      tableData = null;
    }
  }
</script>

<div class="flex h-full">
  <!-- Table list -->
  <div class="w-48 border-r overflow-y-auto shrink-0 py-2" style="border-color: var(--border-color); background: var(--bg-secondary);">
    {#each allTables as table}
      <button
        class="block w-full text-left px-3 py-1 text-xs cursor-pointer"
        style={selectedTable === table.name
          ? "background: var(--accent-color); color: white;"
          : "color: var(--text-primary);"}
        onclick={() => loadTable(table.name)}
      >
        <span class="inline-block w-3 text-center mr-1 opacity-50">{table.kind === "node" ? "N" : "R"}</span>
        {table.name}
        <span class="float-right" style="color: {selectedTable === table.name ? 'rgba(255,255,255,0.7)' : 'var(--text-muted)'};">{table.row_count}</span>
      </button>
    {/each}
  </div>

  <!-- Table content -->
  <div class="flex-1 flex flex-col min-w-0">
    {#if selectedTable}
      <!-- View toggle -->
      <div class="flex items-center px-3 py-1 gap-2 border-b shrink-0" style="border-color: var(--border-color); background: var(--bg-secondary);">
        <span class="text-xs font-medium" style="color: var(--text-primary);">{selectedTable}</span>
        <div class="flex gap-1 ml-auto">
          <button
            class="text-xs px-2 py-0.5 rounded cursor-pointer"
            style={activeView === "schema"
              ? "background: var(--accent-color); color: white;"
              : "background: var(--bg-hover); color: var(--text-secondary);"}
            onclick={() => activeView = "schema"}
          >
            {$tt("table.schema")}
          </button>
          <button
            class="text-xs px-2 py-0.5 rounded cursor-pointer"
            style={activeView === "data"
              ? "background: var(--accent-color); color: white;"
              : "background: var(--bg-hover); color: var(--text-secondary);"}
            onclick={() => activeView = "data"}
          >
            {$tt("table.data")}
          </button>
        </div>
      </div>

      {#if activeView === "schema"}
        <div class="flex-1 overflow-auto p-3">
          <table class="w-full text-xs border-collapse">
            <thead>
              <tr>
                <th class="px-3 py-1.5 text-left font-medium border-b" style="border-color: var(--border-color); color: var(--text-secondary);">{$tt("table.name")}</th>
                <th class="px-3 py-1.5 text-left font-medium border-b" style="border-color: var(--border-color); color: var(--text-secondary);">{$tt("table.type")}</th>
              </tr>
            </thead>
            <tbody>
              {#each schema as col}
                <tr>
                  <td class="px-3 py-1 border-b font-mono" style="border-color: var(--border-color);">{col.name}</td>
                  <td class="px-3 py-1 border-b" style="border-color: var(--border-color); color: var(--text-secondary);">{col.data_type}</td>
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      {:else if tableData}
        <div class="flex-1 overflow-hidden">
          <ResultTable result={tableData} />
        </div>
        <!-- Pagination -->
        <div class="flex items-center justify-center px-3 py-1 gap-2 border-t shrink-0" style="border-color: var(--border-color); background: var(--bg-secondary);">
          <button
            class="text-xs px-2 py-0.5 rounded cursor-pointer disabled:opacity-30"
            style="background: var(--bg-hover); color: var(--text-secondary);"
            onclick={() => loadPage(page - 1)}
            disabled={page === 0}
          >
            {$tt("table.prev")}
          </button>
          <span class="text-xs" style="color: var(--text-secondary);">
            {$tt("table.page")} {page + 1}
          </span>
          <button
            class="text-xs px-2 py-0.5 rounded cursor-pointer disabled:opacity-30"
            style="background: var(--bg-hover); color: var(--text-secondary);"
            onclick={() => loadPage(page + 1)}
            disabled={tableData.row_count < pageSize}
          >
            {$tt("table.next")}
          </button>
        </div>
      {/if}
    {:else}
      <div class="flex items-center justify-center h-full text-xs" style="color: var(--text-muted);">
        {$tt("table.select")}
      </div>
    {/if}
  </div>
</div>
