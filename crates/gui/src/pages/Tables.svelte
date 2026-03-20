<script lang="ts">
  import TableBrowser from "../components/TableBrowser.svelte";
  import { dbInfo, tables } from "../stores/database";
  import { tt } from "$lib/i18n";
</script>

<div class="h-full overflow-auto">
  {#if $dbInfo && $tables}
    <TableBrowser tables={$tables} />
  {:else}
    <div class="flex items-center justify-center h-full text-xs" style="color: var(--text-muted);">
      {$tt("sidebar.no_database")}
    </div>
  {/if}
</div>
