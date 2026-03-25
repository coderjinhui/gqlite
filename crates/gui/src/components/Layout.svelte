<script lang="ts">
  import Sidebar from "./Sidebar.svelte";
  import StatusBar from "./StatusBar.svelte";
  import Query from "../pages/Query.svelte";
  import Tables from "../pages/Tables.svelte";
  import Graph from "../pages/Graph.svelte";
  import { currentPage } from "../stores/database";
  import { tt } from "$lib/i18n";

  const tabs = [
    { id: "query" as const, key: "tab.query" },
    { id: "tables" as const, key: "tab.tables" },
    { id: "graph" as const, key: "tab.graph" },
  ];
</script>

<div class="flex h-screen w-screen overflow-hidden" style="background: var(--bg-primary); color: var(--text-primary);">
  <!-- Sidebar -->
  <Sidebar />

  <!-- Main area -->
  <div class="flex flex-col flex-1 min-w-0">
    <!-- Tab bar -->
    <div class="flex items-center h-[var(--tabbar-height)] border-b px-2 gap-1 shrink-0" style="background: var(--bg-secondary); border-color: var(--border-color);">
      {#each tabs as tab}
        <button
          class="px-3 py-1 rounded text-xs font-medium transition-colors cursor-pointer"
          style={$currentPage === tab.id
            ? "background: var(--accent-color); color: white;"
            : "color: var(--text-secondary);"}
          style:hover={$currentPage !== tab.id ? "background: var(--bg-hover);" : ""}
          onclick={() => currentPage.set(tab.id)}
        >
          {$tt(tab.key)}
        </button>
      {/each}
    </div>

    <!-- Content -->
    <div class="flex-1 overflow-auto">
      {#if $currentPage === "query"}
        <Query />
      {:else if $currentPage === "tables"}
        <Tables />
      {:else if $currentPage === "graph"}
        <Graph />
      {/if}
    </div>

    <!-- Status bar -->
    <StatusBar />
  </div>
</div>
