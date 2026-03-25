<script lang="ts">
  import QueryEditor from "../components/QueryEditor.svelte";
  import ResultTable from "../components/ResultTable.svelte";
  import { dbInfo, statusMessage } from "../stores/database";
  import { tt } from "$lib/i18n";
  import { executeQuery } from "$lib/api";
  import type { QueryResponse, QueryTab } from "$lib/types";

  let tabs = $state<QueryTab[]>([
    { id: "1", name: "Query 1", content: "", result: null },
  ]);
  let activeTabId = $state("1");
  let nextId = $state(2);
  let executing = $state(false);

  let activeTab = $derived(tabs.find((t) => t.id === activeTabId)!);

  function addTab() {
    const id = String(nextId++);
    tabs = [...tabs, { id, name: `Query ${id}`, content: "", result: null }];
    activeTabId = id;
  }

  function closeTab(id: string) {
    if (tabs.length <= 1) return;
    tabs = tabs.filter((t) => t.id !== id);
    if (activeTabId === id) {
      activeTabId = tabs[0].id;
    }
  }

  async function handleExecute() {
    if (!$dbInfo || !activeTab.content.trim() || executing) return;
    executing = true;
    statusMessage.set("Executing...");
    try {
      const result = await executeQuery(activeTab.content);
      tabs = tabs.map((t) =>
        t.id === activeTabId ? { ...t, result } : t
      );
      statusMessage.set(
        `${result.row_count} ${$tt("result.rows")} | ${$tt("result.elapsed")}: ${result.elapsed_ms.toFixed(1)}ms`
      );
    } catch (e: any) {
      tabs = tabs.map((t) =>
        t.id === activeTabId
          ? {
              ...t,
              result: {
                columns: [{ name: "error", data_type: "String" }],
                rows: [[e.toString()]],
                row_count: 1,
                elapsed_ms: 0,
              },
            }
          : t
      );
      statusMessage.set("");
    }
    executing = false;
  }

  function updateContent(content: string) {
    tabs = tabs.map((t) =>
      t.id === activeTabId ? { ...t, content } : t
    );
  }
</script>

<div class="flex flex-col h-full">
  <!-- Tab strip -->
  <div class="flex items-center h-7 border-b px-1 gap-0.5 shrink-0" style="background: var(--bg-secondary); border-color: var(--border-color);">
    {#each tabs as tab}
      <button class="flex items-center gap-1 px-2 py-0.5 rounded text-xs cursor-pointer"
        style={tab.id === activeTabId ? "background: var(--bg-primary); color: var(--text-primary);" : "color: var(--text-secondary);"}
        onclick={() => activeTabId = tab.id}
      >
        <span>{tab.name}</span>
        {#if tabs.length > 1}
          <span
            role="button"
            tabindex="-1"
            class="text-xs opacity-50 hover:opacity-100 cursor-pointer"
            onclick={(e) => { e.stopPropagation(); closeTab(tab.id); }}
            onkeydown={(e) => { if (e.key === 'Enter') { e.stopPropagation(); closeTab(tab.id); } }}
          >x</span>
        {/if}
      </button>
    {/each}
    <button
      class="text-xs px-1.5 py-0.5 rounded cursor-pointer"
      style="color: var(--text-muted);"
      onclick={addTab}
      title={$tt("query.new_tab")}
    >+</button>
  </div>

  <!-- Editor + toolbar -->
  <div class="flex flex-col shrink-0" style="height: 40%;">
    <div class="flex items-center px-2 py-1 gap-2 border-b shrink-0" style="border-color: var(--border-color);">
      <button
        class="text-xs px-3 py-1 rounded font-medium cursor-pointer disabled:opacity-50"
        style="background: var(--accent-color); color: white;"
        onclick={handleExecute}
        disabled={!$dbInfo || executing}
      >
        {executing ? "..." : $tt("query.execute")} <span class="opacity-60 text-[10px]">Ctrl+Enter</span>
      </button>
    </div>
    <div class="flex-1 overflow-hidden">
      <QueryEditor
        content={activeTab.content}
        onchange={updateContent}
        onexecute={handleExecute}
      />
    </div>
  </div>

  <!-- Results -->
  <div class="flex-1 overflow-hidden border-t" style="border-color: var(--border-color);">
    {#if activeTab.result}
      <ResultTable result={activeTab.result} />
    {:else}
      <div class="flex items-center justify-center h-full text-xs" style="color: var(--text-muted);">
        {$tt("result.no_results")}
      </div>
    {/if}
  </div>
</div>
