<script lang="ts">
  import type { QueryResponse } from "$lib/types";
  import { tt } from "$lib/i18n";

  interface Props {
    result: QueryResponse;
  }

  let { result }: Props = $props();
</script>

<div class="flex flex-col h-full overflow-hidden">
  <!-- Column headers info -->
  <div class="flex items-center px-3 py-1 text-xs border-b shrink-0" style="background: var(--bg-secondary); border-color: var(--border-color); color: var(--text-secondary);">
    <span>{result.row_count} {$tt("result.rows")}</span>
    {#if result.elapsed_ms > 0}
      <span class="ml-2">| {$tt("result.elapsed")}: {result.elapsed_ms.toFixed(1)}ms</span>
    {/if}
  </div>

  <!-- Table -->
  <div class="flex-1 overflow-auto">
    {#if result.rows.length === 0}
      <div class="flex items-center justify-center h-full text-xs" style="color: var(--text-muted);">
        {$tt("result.empty")}
      </div>
    {:else}
      <table class="w-full text-xs border-collapse">
        <thead class="sticky top-0" style="background: var(--bg-secondary);">
          <tr>
            <th class="px-3 py-1.5 text-left font-medium border-b" style="border-color: var(--border-color); color: var(--text-muted); width: 40px;">
              #
            </th>
            {#each result.columns as col}
              <th class="px-3 py-1.5 text-left font-medium border-b" style="border-color: var(--border-color); color: var(--text-secondary);">
                {col.name}
                <span class="font-normal ml-1" style="color: var(--text-muted);">{col.data_type}</span>
              </th>
            {/each}
          </tr>
        </thead>
        <tbody>
          {#each result.rows as row, i}
            <tr
              style="border-color: var(--border-color);"
              onmouseenter={(e) => { (e.currentTarget as HTMLElement).style.background = 'var(--bg-hover)' }}
              onmouseleave={(e) => { (e.currentTarget as HTMLElement).style.background = 'transparent' }}
            >
              <td class="px-3 py-1 border-b" style="border-color: var(--border-color); color: var(--text-muted);">
                {i + 1}
              </td>
              {#each row as cell}
                <td class="px-3 py-1 border-b font-mono" style="border-color: var(--border-color);">
                  {#if cell === null}
                    <span style="color: var(--text-muted);">NULL</span>
                  {:else if typeof cell === "object"}
                    {JSON.stringify(cell)}
                  {:else}
                    {cell}
                  {/if}
                </td>
              {/each}
            </tr>
          {/each}
        </tbody>
      </table>
    {/if}
  </div>
</div>
