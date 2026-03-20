<script lang="ts">
  import { onMount } from "svelte";
  import { EditorView, keymap, placeholder as cmPlaceholder } from "@codemirror/view";
  import { EditorState } from "@codemirror/state";
  import { defaultKeymap, history, historyKeymap } from "@codemirror/commands";
  import { syntaxHighlighting, defaultHighlightStyle, bracketMatching } from "@codemirror/language";
  import { sql } from "@codemirror/lang-sql";

  interface Props {
    content: string;
    onchange: (content: string) => void;
    onexecute: () => void;
  }

  let { content, onchange, onexecute }: Props = $props();

  let editorContainer: HTMLDivElement;
  let view: EditorView | null = null;

  onMount(() => {
    const executeKeymap = keymap.of([
      {
        key: "Ctrl-Enter",
        mac: "Cmd-Enter",
        run: () => {
          onexecute();
          return true;
        },
      },
    ]);

    const updateListener = EditorView.updateListener.of((update) => {
      if (update.docChanged) {
        onchange(update.state.doc.toString());
      }
    });

    const theme = EditorView.theme({
      "&": {
        height: "100%",
        fontSize: "13px",
        fontFamily: "'SF Mono', 'Fira Code', 'Cascadia Code', monospace",
      },
      ".cm-content": {
        padding: "8px 0",
        caretColor: "var(--accent-color)",
      },
      ".cm-gutters": {
        background: "var(--bg-secondary)",
        borderRight: "1px solid var(--border-color)",
        color: "var(--text-muted)",
      },
      ".cm-activeLineGutter": {
        background: "var(--bg-hover)",
      },
      ".cm-activeLine": {
        background: "var(--bg-hover)",
      },
      "&.cm-focused .cm-cursor": {
        borderLeftColor: "var(--accent-color)",
      },
      "&.cm-focused .cm-selectionBackground, .cm-selectionBackground": {
        background: "var(--accent-color)33",
      },
    });

    const state = EditorState.create({
      doc: content,
      extensions: [
        executeKeymap,
        history(),
        keymap.of([...defaultKeymap, ...historyKeymap]),
        sql(),
        syntaxHighlighting(defaultHighlightStyle),
        bracketMatching(),
        cmPlaceholder("-- Enter GQL query here\n-- Press Ctrl+Enter to execute"),
        updateListener,
        theme,
        EditorView.lineWrapping,
      ],
    });

    view = new EditorView({
      state,
      parent: editorContainer,
    });

    return () => {
      view?.destroy();
    };
  });

  // Sync external content changes
  $effect(() => {
    if (view && content !== view.state.doc.toString()) {
      view.dispatch({
        changes: {
          from: 0,
          to: view.state.doc.length,
          insert: content,
        },
      });
    }
  });
</script>

<div class="h-full w-full overflow-hidden" style="background: var(--bg-primary);" bind:this={editorContainer}></div>
