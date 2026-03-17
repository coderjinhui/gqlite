<script lang="ts">
  import { onMount } from "svelte";
  import type { TablesResponse, GraphData } from "$lib/types";
  import { getGraphData } from "$lib/api";
  import { tt } from "$lib/i18n";

  interface Props {
    tables: TablesResponse;
  }

  let { tables }: Props = $props();

  let graphContainer: HTMLDivElement;
  let selectedNodeTable = $state("");
  let selectedRelTable = $state("");
  let nodeLimit = $state(200);
  let graphData = $state<GraphData | null>(null);
  let loading = $state(false);
  let errorMsg = $state("");
  let graphInstance: any = null;

  // Node table color palette
  const colors = [
    "#228be6", "#40c057", "#fab005", "#fa5252", "#7950f2",
    "#20c997", "#fd7e14", "#e64980", "#15aabf", "#82c91e",
  ];

  async function loadGraph() {
    if (!selectedNodeTable) return;
    loading = true;
    errorMsg = "";
    try {
      graphData = await getGraphData(
        selectedNodeTable,
        selectedRelTable || null,
        nodeLimit
      );
      await renderGraph();
    } catch (e: any) {
      errorMsg = e.toString();
      console.error("Graph load error:", e);
    }
    loading = false;
  }

  async function renderGraph() {
    if (!graphData || !graphContainer) return;

    // Destroy existing instance
    if (graphInstance) {
      graphInstance.destroy();
      graphInstance = null;
    }

    // Dynamically import G6 to keep initial bundle small
    const G6 = await import("@antv/g6");

    const nodeColor = (label: string) => {
      const allLabels = [...new Set(graphData!.nodes.map((n) => n.label))];
      const idx = allLabels.indexOf(label);
      return colors[idx % colors.length];
    };

    const nodes = graphData.nodes.map((n) => ({
      id: n.id,
      data: {
        label: n.label,
        properties: n.properties,
      },
      style: {
        fill: nodeColor(n.label),
        stroke: nodeColor(n.label),
        labelText: getNodeLabel(n.properties, n.label),
        labelFill: "#fff",
        labelFontSize: 10,
        size: 28,
      },
    }));

    const edges = graphData.edges.map((e, i) => ({
      id: `edge-${i}`,
      source: e.source,
      target: e.target,
      data: {
        label: e.label,
        properties: e.properties,
      },
      style: {
        labelText: e.label,
        labelFontSize: 9,
        labelFill: "var(--text-secondary)",
        endArrow: true,
        stroke: "var(--text-muted)",
      },
    }));

    graphInstance = new G6.Graph({
      container: graphContainer,
      data: { nodes, edges },
      autoFit: "view",
      layout: {
        type: "d3-force",
        preventOverlap: true,
        nodeStrength: -200,
        linkDistance: 120,
      },
      node: {
        type: "circle",
        style: {
          size: 28,
          labelPlacement: "center",
        },
      },
      edge: {
        type: "line",
        style: {
          endArrow: true,
        },
      },
      behaviors: ["drag-canvas", "zoom-canvas", "drag-element", "click-select"],
      plugins: [{ type: "minimap", size: [120, 80] }],
    });

    await graphInstance.render();
  }

  function getNodeLabel(props: Record<string, any>, fallback: string): string {
    // Try to find a meaningful label: name, id, or first string property
    for (const key of ["name", "title", "label", "id"]) {
      if (props[key] && typeof props[key] === "string") {
        return String(props[key]).substring(0, 12);
      }
    }
    // Use first column value
    const keys = Object.keys(props).filter(k => !k.startsWith("_"));
    if (keys.length > 0) {
      const val = props[keys[0]];
      if (val !== null && val !== undefined) {
        return String(val).substring(0, 12);
      }
    }
    return fallback;
  }

  onMount(() => {
    return () => {
      if (graphInstance) {
        graphInstance.destroy();
      }
    };
  });
</script>

<div class="flex flex-col h-full">
  <!-- Controls -->
  <div class="flex items-center px-3 py-2 gap-3 border-b shrink-0 flex-wrap" style="border-color: var(--border-color); background: var(--bg-secondary);">
    <label class="flex items-center gap-1 text-xs" style="color: var(--text-secondary);">
      {$tt("graph.node_table")}:
      <select
        class="text-xs px-1.5 py-0.5 rounded border"
        style="background: var(--bg-primary); border-color: var(--border-color); color: var(--text-primary);"
        bind:value={selectedNodeTable}
      >
        <option value="">--</option>
        {#each tables.node_tables as t}
          <option value={t.name}>{t.name}</option>
        {/each}
      </select>
    </label>

    <label class="flex items-center gap-1 text-xs" style="color: var(--text-secondary);">
      {$tt("graph.rel_table")}:
      <select
        class="text-xs px-1.5 py-0.5 rounded border"
        style="background: var(--bg-primary); border-color: var(--border-color); color: var(--text-primary);"
        bind:value={selectedRelTable}
      >
        <option value="">{$tt("graph.none")}</option>
        {#each tables.rel_tables as t}
          <option value={t.name}>{t.name}</option>
        {/each}
      </select>
    </label>

    <label class="flex items-center gap-1 text-xs" style="color: var(--text-secondary);">
      {$tt("graph.limit")}:
      <input
        type="number"
        class="w-16 text-xs px-1.5 py-0.5 rounded border"
        style="background: var(--bg-primary); border-color: var(--border-color); color: var(--text-primary);"
        bind:value={nodeLimit}
        min="1"
        max="1000"
      />
    </label>

    <button
      class="text-xs px-3 py-1 rounded font-medium cursor-pointer disabled:opacity-50"
      style="background: var(--accent-color); color: white;"
      onclick={loadGraph}
      disabled={!selectedNodeTable || loading}
    >
      {loading ? "..." : $tt("graph.load")}
    </button>
  </div>

  <!-- Graph area -->
  <div class="flex-1 relative" style="background: var(--bg-primary);">
    {#if errorMsg}
      <div class="absolute top-2 left-2 right-2 z-10 text-xs px-3 py-2 rounded" style="background: var(--error-color); color: white;">
        {errorMsg}
      </div>
    {/if}
    {#if !graphData && !errorMsg}
      <div class="flex items-center justify-center h-full text-xs" style="color: var(--text-muted);">
        {$tt("graph.no_data")}
      </div>
    {/if}
    <div bind:this={graphContainer} class="absolute inset-0"></div>
  </div>
</div>
