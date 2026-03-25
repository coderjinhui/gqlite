<script lang="ts">
  import { dbInfo, tables, errorMessage } from "../stores/database";
  import { tt, locale, toggleLocale, darkMode, toggleTheme } from "$lib/i18n";
  import { openDatabase, closeDatabase, getTables, doCheckpoint } from "$lib/api";
  import type { TablesResponse } from "$lib/types";

  let pathInput = $state("");
  let showPathInput = $state(false);

  async function handleOpenPath() {
    const path = pathInput.trim();
    if (!path) return;
    try {
      const info = await openDatabase(path);
      dbInfo.set(info);
      const t = await getTables();
      tables.set(t);
      errorMessage.set("");
      showPathInput = false;
      pathInput = "";
    } catch (e: any) {
      errorMessage.set(e.toString());
    }
  }

  async function handleClose() {
    try {
      await closeDatabase();
      dbInfo.set(null);
      tables.set(null);
      errorMessage.set("");
    } catch (e: any) {
      errorMessage.set(e.toString());
    }
  }

  async function handleCheckpoint() {
    try {
      await doCheckpoint();
    } catch (e: any) {
      errorMessage.set(e.toString());
    }
  }
</script>

<div
  class="flex flex-col h-full w-[var(--sidebar-width)] border-r shrink-0 select-none"
  style="background: var(--bg-sidebar); border-color: var(--border-color);"
>
  <!-- Logo / Title -->
  <div class="flex items-center justify-between px-3 py-2 border-b" style="border-color: var(--border-color);">
    <span class="font-bold text-sm" style="color: var(--accent-color);">gqlite</span>
    <div class="flex gap-1">
      <button
        class="text-xs px-1.5 py-0.5 rounded cursor-pointer"
        style="color: var(--text-secondary); background: var(--bg-hover);"
        onclick={toggleLocale}
        title={$tt("sidebar.language")}
      >
        {$locale === "en" ? "中" : "EN"}
      </button>
      <button
        class="text-xs px-1.5 py-0.5 rounded cursor-pointer"
        style="color: var(--text-secondary); background: var(--bg-hover);"
        onclick={toggleTheme}
      >
        {$darkMode ? "☀" : "☾"}
      </button>
    </div>
  </div>

  <!-- Database section -->
  <div class="px-3 py-2 border-b" style="border-color: var(--border-color);">
    <div class="text-xs font-semibold mb-1.5 uppercase tracking-wider" style="color: var(--text-muted);">
      {$tt("sidebar.database")}
    </div>

    {#if $dbInfo}
      <div class="text-xs mb-2 truncate" title={$dbInfo.path} style="color: var(--text-secondary);">
        {$dbInfo.path.split("/").pop()}
      </div>
      <div class="flex gap-1 flex-wrap">
        <button
          class="text-xs px-2 py-0.5 rounded cursor-pointer"
          style="background: var(--bg-hover); color: var(--text-secondary);"
          onclick={handleCheckpoint}
        >
          {$tt("sidebar.checkpoint")}
        </button>
        <button
          class="text-xs px-2 py-0.5 rounded cursor-pointer"
          style="background: var(--bg-hover); color: var(--error-color);"
          onclick={handleClose}
        >
          {$tt("sidebar.close")}
        </button>
      </div>
    {:else}
      <div class="text-xs mb-2" style="color: var(--text-muted);">
        {$tt("sidebar.no_database")}
      </div>
      {#if showPathInput}
        <div class="flex flex-col gap-1">
          <input
            type="text"
            class="text-xs px-2 py-1 rounded border w-full"
            style="background: var(--bg-primary); border-color: var(--border-color); color: var(--text-primary);"
            placeholder="/path/to/db.graph"
            bind:value={pathInput}
            onkeydown={(e) => { if (e.key === 'Enter') handleOpenPath(); if (e.key === 'Escape') { showPathInput = false; pathInput = ''; } }}
          />
          <div class="flex gap-1">
            <button
              class="text-xs px-2 py-0.5 rounded cursor-pointer flex-1"
              style="background: var(--accent-color); color: white;"
              onclick={handleOpenPath}
            >
              OK
            </button>
            <button
              class="text-xs px-2 py-0.5 rounded cursor-pointer"
              style="background: var(--bg-hover); color: var(--text-secondary);"
              onclick={() => { showPathInput = false; pathInput = ''; }}
            >
              Cancel
            </button>
          </div>
        </div>
      {:else}
        <div class="flex gap-1">
          <button
            class="text-xs px-2 py-1 rounded cursor-pointer"
            style="background: var(--accent-color); color: white;"
            onclick={() => showPathInput = true}
          >
            {$tt("sidebar.open")} / {$tt("sidebar.create")}
          </button>
        </div>
      {/if}
    {/if}
  </div>

  <!-- Tables list -->
  <div class="flex-1 overflow-y-auto px-3 py-2">
    {#if $tables}
      {#if $tables.node_tables.length > 0}
        <div class="text-xs font-semibold mb-1 uppercase tracking-wider" style="color: var(--text-muted);">
          {$tt("sidebar.node_tables")}
        </div>
        {#each $tables.node_tables as table}
          <div
            role="listitem"
            class="flex items-center justify-between px-2 py-1 rounded text-xs mb-0.5"
            style="color: var(--text-primary);"
            onmouseenter={(e) => { (e.currentTarget as HTMLElement).style.background = 'var(--bg-hover)' }}
            onmouseleave={(e) => { (e.currentTarget as HTMLElement).style.background = 'transparent' }}
          >
            <span class="truncate">{table.name}</span>
            <span class="text-xs shrink-0 ml-1" style="color: var(--text-muted);">{table.row_count}</span>
          </div>
        {/each}
      {/if}

      {#if $tables.rel_tables.length > 0}
        <div class="text-xs font-semibold mt-2 mb-1 uppercase tracking-wider" style="color: var(--text-muted);">
          {$tt("sidebar.rel_tables")}
        </div>
        {#each $tables.rel_tables as table}
          <div
            role="listitem"
            class="flex items-center justify-between px-2 py-1 rounded text-xs mb-0.5"
            style="color: var(--text-primary);"
            onmouseenter={(e) => { (e.currentTarget as HTMLElement).style.background = 'var(--bg-hover)' }}
            onmouseleave={(e) => { (e.currentTarget as HTMLElement).style.background = 'transparent' }}
          >
            <span class="truncate">{table.name}</span>
            <span class="text-xs shrink-0 ml-1" style="color: var(--text-muted);">{table.row_count}</span>
          </div>
        {/each}
      {/if}
    {/if}
  </div>

  <!-- Error display -->
  {#if $errorMessage}
    <div class="px-3 py-1.5 text-xs border-t" style="background: var(--error-color); color: white; border-color: var(--border-color);">
      {$errorMessage}
    </div>
  {/if}
</div>
