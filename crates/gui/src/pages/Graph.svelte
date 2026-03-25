<script lang="ts">
  import GraphView from "../components/GraphView.svelte";
  import { dbInfo, tables } from "../stores/database";
  import { tt } from "$lib/i18n";
</script>

<div class="h-full">
  {#if $dbInfo && $tables}
    <GraphView tables={$tables} />
  {:else}
    <div class="flex items-center justify-center h-full text-xs" style="color: var(--text-muted);">
      {$tt("sidebar.no_database")}
    </div>
  {/if}
</div>
