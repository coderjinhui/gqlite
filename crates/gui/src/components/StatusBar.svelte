<script lang="ts">
  import { dbInfo, statusMessage } from "../stores/database";
  import { tt } from "$lib/i18n";
</script>

<div
  class="flex items-center justify-between px-3 h-[var(--statusbar-height)] border-t text-xs shrink-0"
  style="background: var(--bg-secondary); border-color: var(--border-color); color: var(--text-secondary);"
>
  <div class="flex items-center gap-2">
    <span
      class="inline-block w-2 h-2 rounded-full"
      style={$dbInfo ? "background: var(--success-color);" : "background: var(--text-muted);"}
    ></span>
    <span>
      {$dbInfo ? $tt("status.connected") : $tt("status.disconnected")}
      {#if $dbInfo?.read_only}
        <span style="color: var(--warning-color);">({$tt("status.read_only")})</span>
      {/if}
    </span>
  </div>
  <div>
    {$statusMessage}
  </div>
</div>
