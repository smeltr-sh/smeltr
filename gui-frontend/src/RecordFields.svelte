<!--
  One record, as the preview draws it (`recordDiff.ts`). Nesting is indent and
  a longer name rather than braces; what a step did to a field is a character
  in the gutter and a color on the field's name, so the values themselves
  stay plain text at one left edge. The name column is as wide as the
  record's longest name, up to a cap past which a name wraps at its dots, and
  the types sit in a column beside it.

  What the step removed waits on one folded line at the end, so a rename
  reads as its new name alone.

  Structs and arrays draw a page at a time, and one too big for what is left
  of its page starts folded (`recordLines`). Filtered to what changed, the
  tree is dropped and each field states its whole path: at four or five lines
  an outline costs more than it explains.
-->
<script lang="ts">
  import { SvelteMap, SvelteSet } from 'svelte/reactivity';
  import { formatDiffValue } from './diff';
  import {
    changedLines,
    changeFitsInline,
    diffTree,
    leafLines,
    recordLines,
    splitPath,
    withoutRemoved,
    type Declared,
    type RecordLine,
  } from './recordDiff';

  let {
    before,
    after,
    keep,
    declared,
    changedOnly = false,
    showTypes = true,
  }: {
    before: unknown;
    after: unknown;
    /** Each side's record type, null where that side did not compile. */
    declared?: { before: Declared | null; after: Declared | null };
    /** Whether a field the grid draws as one cell is drawn here. */
    keep?: (path: string) => boolean;
    changedOnly?: boolean;
    showTypes?: boolean;
  } = $props();

  let split = $derived(withoutRemoved(diffTree(before, after, keep, declared)));
  let tree = $derived(split.tree);
  let removedOpen = $state(false);
  let removed = $derived(removedOpen ? leafLines(split.removed) : []);

  // The reader's choices, by a struct's or an array's path: open or folded,
  // and how many pages past the first it draws.
  let folds = $state(new SvelteMap<string, boolean>());
  let pages = $state(new SvelteMap<string, number>());

  let lines = $derived.by(() => {
    const choices = { open: (p: string) => folds.get(p), pages: (p: string) => pages.get(p) };
    return changedOnly ? changedLines(tree, choices) : recordLines(tree, choices);
  });

  // In characters: the column is monospace, and a name's length is its width.
  const INDENT_CH = 2;
  const NAME_CH = 34;
  const nameWidth = (l: RecordLine) =>
    l.kind === 'more' ? 0 : l.depth * INDENT_CH + l.label.length;
  let nameCh = $derived(
    Math.min(NAME_CH, Math.max(0, ...lines.map(nameWidth), ...removed.map(nameWidth))),
  );
  let typeCh = $derived(
    showTypes
      ? Math.max(
          0,
          ...[...lines, ...removed].map((l) => (l.kind === 'more' ? 0 : (l.type?.length ?? 0))),
        )
      : 0,
  );

  const GLYPH = { added: '+', changed: '~', removed: '−', unchanged: '' } as const;

  // Which held-back values the reader has asked for, by path. A change too
  // long to draw both halves of is still a change someone needs to read, so
  // the rule standing in for the old value opens it rather than only saying
  // it exists.
  let revealed = $state(new SvelteSet<string>());

  function toggleHeld(path: string) {
    if (revealed.has(path)) revealed.delete(path);
    else revealed.add(path);
  }

  // Which clamped values the reader has opened, by path, and which run past
  // their clamp: only those offer to open.
  let opened = $state(new SvelteSet<string>());
  let overflowing = $state(new SvelteSet<string>());

  /** Says whether a clamped value is cut short, as its width changes. */
  function clampWatch(node: HTMLElement, path: string) {
    let at = path;
    const check = () => {
      if (!node.classList.contains('clamped')) return;
      const over = node.scrollHeight > node.clientHeight + 1;
      if (over && !overflowing.has(at)) overflowing.add(at);
      else if (!over && overflowing.has(at)) overflowing.delete(at);
    };
    const observer = new ResizeObserver(check);
    observer.observe(node);
    return {
      update(next: string) {
        at = next;
        check();
      },
      destroy: () => observer.disconnect(),
    };
  }

  function toggleLong(path: string) {
    if (opened.has(path)) opened.delete(path);
    else opened.add(path);
  }

  const count = (n: number, one: string) => `${n} ${one}${n === 1 ? '' : 's'}`;
</script>

{#snippet name(label: string)}
  <!-- A long name breaks after a dot, where its parts already divide. -->
  {#each label.split('.') as part, k}{#if k > 0}.<wbr />{/if}{part}{/each}
{/snippet}

{#snippet field(line: Extract<RecordLine, { kind: 'field' }>)}
  {@const inline = line.state === 'changed' && changeFitsInline(line.before, line.after)}
  {@const body = formatDiffValue(line.state === 'removed' ? line.before : line.after)}
  <div class="line state-{line.state}">
    <span class="gutter">{GLYPH[line.state]}</span>
    <span class="key">
      <span class="name" style:padding-left={`${line.depth * INDENT_CH}ch`}>
        {#if changedOnly}
          {@const parts = splitPath(line.label)}
          <span class="path-prefix">{@render name(parts.prefix)}</span>{parts.leaf}
        {:else}
          {@render name(line.label)}
        {/if}
      </span>
      {#if showTypes}<span class="type">{line.type ?? ''}</span>{/if}
    </span>
    {#if inline}
      <span class="value">
        <span class="gone">{formatDiffValue(line.before)}</span>
        <span class="arrow">&rarr;</span>{formatDiffValue(line.after)}
      </span>
    {:else}
      {@const open = opened.has(line.path)}
      <span class="value-col">
        <span
          class="value"
          class:gone={line.state === 'removed'}
          class:clamped={!open}
          use:clampWatch={line.path}>{body}</span
        >
        {#if open || overflowing.has(line.path)}
          <button
            type="button"
            class="more"
            aria-expanded={open}
            aria-label={`${open ? 'collapse' : 'expand'} the value of ${line.path}`}
            onclick={() => toggleLong(line.path)}
            >{open ? 'less' : `Show all ${body.length.toLocaleString('en-US')} characters`}</button
          >
        {/if}
      </span>
    {/if}
  </div>
  {#if line.state === 'changed' && !inline}
    <!-- The old value did not fit beside the new one, so it keeps the
         line it would have taken: a dotted rule saying something is held
         back, which opens it. -->
    {@const open = revealed.has(line.path)}
    <button
      type="button"
      class="held"
      aria-expanded={open}
      aria-label={`${open ? 'hide' : 'show'} the previous value of ${line.path}`}
      onclick={() => toggleHeld(line.path)}
    >
      {#if open}
        <span class="value gone">{formatDiffValue(line.before)}</span>
      {:else}
        <span class="held-rule"></span>
      {/if}
    </button>
  {/if}
{/snippet}

{#if lines.length === 0 && split.removed.length === 0}
  <span class="dim">no change at this step</span>
{:else}
  <div class="fields" style:--name-w={`${nameCh}ch`} style:--type-w={`${typeCh}ch`}>
    {#each lines as line (line.kind + line.path + line.depth)}
      {#if line.kind === 'group'}
        <button
          type="button"
          class="line group"
          aria-expanded={line.open}
          onclick={() => folds.set(line.path, !line.open)}
        >
          <span class="gutter chevron">{line.open ? '▾︎' : '▸︎'}</span>
          <span class="key">
            <span class="name" style:padding-left={`${line.depth * INDENT_CH}ch`}
              >{@render name(line.label)}</span
            >
            {#if showTypes}<span class="type">{line.type ?? ''}</span>{/if}
          </span>
          <span class="group-marks">
            {#if line.branch === 'array'}
              <span class="dim">{count(line.size, 'element')}</span>
            {:else if !line.open}
              <span class="dim">{count(line.size, 'field')}</span>
            {/if}
            {#if !line.open}
              {#if line.marks.added > 0}<span class="mark added">+{line.marks.added}</span>{/if}
              {#if line.marks.changed > 0}<span class="mark changed">~{line.marks.changed}</span
                >{/if}
              {#if line.marks.removed > 0}<span class="mark removed"
                  >&minus;{line.marks.removed}</span
                >{/if}
            {/if}
          </span>
        </button>
      {:else if line.kind === 'more'}
        {@const path = line.path}
        <div class="line">
          <span class="gutter"></span>
          <span class="dim" style:margin-left={`${line.depth * INDENT_CH}ch`}
            >{line.shown} of {line.total} shown ·&nbsp;</span
          ><button
            type="button"
            class="more"
            onclick={() => pages.set(path, (pages.get(path) ?? 0) + 1)}>show more</button
          >
        </div>
      {:else}
        {@render field(line)}
      {/if}
    {/each}
    {#if split.removed.length > 0}
      <button
        type="button"
        class="line group removed-fold"
        aria-expanded={removedOpen}
        onclick={() => (removedOpen = !removedOpen)}
      >
        <span class="gutter chevron">{removedOpen ? '▾︎' : '▸︎'}</span>
        <span class="removed-names"
          >{split.removed.length} removed: {split.removed.map((n) => n.path).join(', ')}</span
        >
      </button>
      {#each removed as line (line.kind + line.path)}
        {#if line.kind === 'field'}{@render field(line)}{/if}
      {/each}
    {/if}
  </div>
{/if}

<style>
  .dim {
    color: var(--dim);
    font-weight: 400;
  }

  .fields {
    --key-w: calc(var(--name-w) + var(--type-w) + 0.6rem);
    font-family: var(--mono);
    font-size: var(--text-note);
    line-height: 1.75;
    padding: 0.2rem 0 0.3rem;
  }

  /* A wrapped value makes the row as tall as it needs; the mark and the name
     stay on its first line rather than centering against it. */
  .line {
    display: flex;
    align-items: baseline;
    gap: 0.5rem;
  }

  .group {
    width: 100%;
    border: none;
    background: none;
    padding: 0;
    font: inherit;
    font-weight: 600;
    color: inherit;
    text-align: left;
    cursor: pointer;
  }

  .group:hover .chevron,
  .group:focus-visible .chevron {
    color: var(--ink);
  }

  .group-marks {
    display: flex;
    gap: 0.5rem;
    font-weight: 400;
  }

  /* The gutter is a fixed column at the far left: the mark never moves with
     the depth of the field it belongs to. A group's chevron sits in it too,
     so a group's name lines up with its siblings'. */
  .gutter {
    flex-shrink: 0;
    width: 0.75rem;
    text-align: center;
    color: var(--dim);
  }

  .key {
    display: flex;
    flex-shrink: 0;
    width: var(--key-w);
    color: var(--dim);
  }

  .group .key {
    color: var(--ink);
  }

  .name {
    box-sizing: border-box;
    flex-shrink: 0;
    width: var(--name-w);
    overflow-wrap: anywhere;
  }

  .type {
    flex-shrink: 0;
    width: var(--type-w);
    margin-left: 0.6rem;
    color: var(--dim);
    font-weight: 400;
  }

  /* Every value starts here, whatever its key's depth. Wrapped rather than
     clipped: a value the panel is too narrow for is still one someone has to
     read, and the field cannot be scrolled sideways. */
  .value-col {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    min-width: 0;
  }

  .value {
    min-width: 0;
    overflow-wrap: anywhere;
    white-space: pre-wrap;
  }

  /* Three lines is enough to recognize a value by; past that it is opened
     deliberately, so one long field cannot push the rest of the record off
     the panel. */
  .clamped {
    display: -webkit-box;
    -webkit-line-clamp: 3;
    line-clamp: 3;
    -webkit-box-orient: vertical;
    overflow: hidden;
  }

  .more {
    flex-shrink: 0;
    border: none;
    background: none;
    padding: 0;
    font: inherit;
    color: var(--accent);
    cursor: pointer;
  }

  .more:hover {
    text-decoration: underline;
    text-underline-offset: 2px;
  }

  .gone {
    text-decoration: line-through;
    color: var(--dim);
  }

  /* The nesting recedes so the eye lands on the field's own name. */
  .path-prefix {
    opacity: 0.6;
  }

  .arrow {
    color: var(--dim);
    padding: 0 0.3rem;
  }

  .state-added .gutter {
    color: var(--added);
  }

  .state-changed .gutter {
    color: var(--changed);
  }

  .state-removed .gutter,
  .state-removed .key {
    color: var(--removed);
  }

  .state-removed .key {
    text-decoration: line-through;
  }

  .state-removed {
    opacity: 0.7;
  }

  /* What the step removed, named on one line until asked for. */
  .removed-fold {
    font-weight: 400;
    color: var(--dim);
  }

  .removed-fold .chevron {
    color: var(--removed);
  }

  .removed-names {
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  /* Sits under the value it belongs to, and is as wide as the line so the
     rule is a target rather than a hairline to hit. */
  .held {
    display: flex;
    width: 100%;
    padding: 0 0 0 calc(1.75rem + var(--key-w));
    border: none;
    background: none;
    font: inherit;
    text-align: left;
    cursor: pointer;
  }

  .held:hover .held-rule,
  .held:focus-visible .held-rule {
    opacity: 1;
  }

  .held-rule {
    flex-grow: 1;
    height: 5px;
    border-bottom: 1px dotted var(--dim);
    opacity: 0.45;
  }

  /* What a closed group is hiding -- the same marks a folded grid row
     carries, so closing a group costs nothing but the fields themselves. */
  .mark {
    font-weight: 600;
  }

  .mark.added {
    color: var(--added);
  }

  .mark.changed {
    color: var(--changed);
  }

  .mark.removed {
    color: var(--removed);
  }
</style>
