<!--
  One record, as the preview draws it (`recordDiff.ts`). Nesting is indent and
  a longer name rather than braces. What the steps did is a bar beside each run
  of lines they changed, from the depth where the run starts and in the color
  of what they did, so names and values stay plain text. The name column is as wide as the
  record's longest name, up to a cap past which a name wraps at its dots, and
  the types sit in a column beside it.

  What the step removed waits on one folded line at the end, so a rename
  reads as its new name alone.

  A run's sign is the step that last touched its lines, or +, ~ or − where no
  step is named, shown where the run starts and where that step changes
  (`runMarks`). Held, a field's sign lists every step that touched it, and
  each jumps to its step.

  Structs and arrays draw a page at a time, and one too big for what is left
  of its page starts folded (`recordLines`). With full paths the tree is
  dropped and each field states its whole path (`pathLines`). Either way it
  can be filtered to what changed.
-->
<script lang="ts">
  import { SvelteMap, SvelteSet } from 'svelte/reactivity';
  import { formatDiffValue } from './diff';
  import { stepName, type Step } from './pipeline';
  import StepGlyph from './StepGlyph.svelte';
  import Clamped from './Clamped.svelte';
  import { float } from './float';
  import {
    changeFitsInline,
    diffTree,
    leafLines,
    PAGE_LINES,
    pathLines,
    recordLines,
    runMarks,
    splitPath,
    withoutRemoved,
    type Declared,
    type RecordLine,
    type RunMark,
  } from './recordDiff';

  let {
    before,
    after,
    keep,
    declared,
    changedOnly = false,
    fullPaths = false,
    showTypes = true,
    touched,
    onjump,
  }: {
    before: unknown;
    after: unknown;
    /** Each side's record type, null where that side did not compile. */
    declared?: { before: Declared | null; after: Declared | null };
    /** Whether a field the grid draws as one cell is drawn here. */
    keep?: (path: string) => boolean;
    changedOnly?: boolean;
    /** Each field by its whole path, rather than nested. */
    fullPaths?: boolean;
    showTypes?: boolean;
    /** The steps that touched a field on this record, in the order they ran,
     * each with its number in the step list. Without it no step is shown. */
    touched?: (path: string) => { step: Step; number: number }[];
    onjump?: (step: Step) => void;
  } = $props();

  let split = $derived(withoutRemoved(diffTree(before, after, keep, declared)));
  let tree = $derived(split.tree);
  let removedOpen = $state(false);
  // Opened, a page at a time like the rest of the record.
  let removedPages = $state(0);
  let removedAll = $derived(removedOpen ? leafLines(split.removed) : []);
  let removed = $derived(removedAll.slice(0, PAGE_LINES * (1 + removedPages)));

  // The reader's choices, by a struct's or an array's path: open or folded,
  // and how many pages past the first it draws.
  // The field whose steps are listed, while the pointer or focus is on its glyph or the list. Leaving
  // waits a moment, so the pointer can cross the gap between the two.
  let byOpen = $state<string | null>(null);
  let byAnchor: HTMLElement | undefined;
  let byClosing: ReturnType<typeof setTimeout> | undefined;
  function showBy(path: string, at: HTMLElement) {
    clearTimeout(byClosing);
    byAnchor = at;
    byOpen = path;
  }
  function hideBy() {
    clearTimeout(byClosing);
    byClosing = setTimeout(() => (byOpen = null), 150);
  }
  $effect(() => () => clearTimeout(byClosing));

  let folds = $state(new SvelteMap<string, boolean>());
  let pages = $state(new SvelteMap<string, number>());

  let lines = $derived.by(() => {
    const choices = { open: (p: string) => folds.get(p), pages: (p: string) => pages.get(p) };
    return fullPaths
      ? pathLines(tree, choices, changedOnly)
      : recordLines(tree, choices, changedOnly);
  });

  // In characters: the column is monospace, and a name's length is its width.
  const INDENT_CH = 2;
  // Ahead of every name, its sign and then a group's chevron, so names at one depth line up.
  const LEAD_CH = 4;
  const NAME_CH = 34;
  const nameWidth = (l: RecordLine) =>
    l.kind === 'more' ? 0 : l.depth * INDENT_CH + LEAD_CH + l.label.length;

  const stepOf = (path: string) => touched?.(path).at(-1)?.step.id ?? null;
  let marks = $derived(runMarks(lines, stepOf));
  let removedMarks = $derived(runMarks(removed, stepOf));
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

  // Which clamped values the reader has opened, by path.
  let opened = $state(new SvelteSet<string>());

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

{#snippet more(depth: number, shown: number, total: number, onmore: () => void)}
  <div class="line">
    <span class="dim" style:margin-left={`${depth * INDENT_CH + LEAD_CH}ch`}
      >{shown} of {total} shown ·&nbsp;</span
    ><button type="button" class="more" onclick={onmore}>show more</button>
  </div>
{/snippet}

{#snippet run(mark: RunMark)}
  {#if mark.state !== 'unchanged'}<span
      class="bar state-{mark.state}"
      style:left={`${mark.depth * INDENT_CH}ch`}
    ></span>{/if}
{/snippet}

{#snippet field(line: Extract<RecordLine, { kind: 'field' }>, mark: RunMark)}
  {@const inline = line.state === 'changed' && changeFitsInline(line.before, line.after)}
  {@const body = formatDiffValue(line.state === 'removed' ? line.before : line.after)}
  {@const by = touched?.(line.path) ?? []}
  {@const last = by.at(-1)}
  <div class="line state-{line.state}" class:run-start={mark.start}>
    {@render run(mark)}
    <span class="key">
      <span class="name" style:padding-left={`${line.depth * INDENT_CH}ch`}>
        <span class="sign state-{mark.state}">
          {#if mark.sign && last}
            <span
              class="by"
              role="group"
              onpointerenter={(e) => showBy(line.path, e.currentTarget)}
              onpointerleave={hideBy}
              onfocusin={(e) => showBy(line.path, e.currentTarget)}
              onfocusout={hideBy}
            >
              <button
                type="button"
                class="by-glyph"
                aria-label={stepName(last.step, last.number)}
                onclick={() => onjump?.(last.step)}><StepGlyph type={last.step.type} /></button
              >
              {#if byOpen === line.path}
                <span
                  class="by-list"
                  popover="manual"
                  {@attach float(() => byAnchor?.getBoundingClientRect())}
                >
                  {#each by as t (t.step.id)}
                    <button type="button" onclick={() => onjump?.(t.step)}
                      ><StepGlyph type={t.step.type} /> {stepName(t.step, t.number)}</button
                    >
                  {/each}
                </span>
              {/if}
            </span>
          {:else if mark.sign}{GLYPH[line.state]}{/if}
        </span><span class="chevron"></span>
        {#if fullPaths}
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
      <Clamped
        text={body}
        open={opened.has(line.path)}
        ontoggle={() => toggleLong(line.path)}
        label={line.path}
        gone={line.state === 'removed'}
      />
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
  <!-- A character's slack, so a name that just fits does not wrap on a rounding. -->
  <div class="fields" style:--name-w={`${nameCh + 1}ch`} style:--type-w={`${typeCh}ch`}>
    {#each lines as line, i (line.kind + line.path + line.depth)}
      {@const mark = marks[i]}
      {#if line.kind === 'group'}
        <button
          type="button"
          class:run-start={mark.start}
          class="line group"
          aria-expanded={line.open}
          onclick={() => folds.set(line.path, !line.open)}
        >
          {@render run(mark)}
          <span class="key">
            <span class="name" style:padding-left={`${line.depth * INDENT_CH}ch`}
              ><span class="sign state-{mark.state}"
                >{#if mark.sign}{@const by = mark.step
                    ? touched?.(line.fields[0].path).at(-1)
                    : null}{#if by}<span title={stepName(by.step, by.number)}
                      ><StepGlyph type={by.step.type} /></span
                    >{:else if !touched}{GLYPH[mark.state]}{/if}{/if}</span
              ><span class="chevron">{line.open ? '▾︎' : '▸︎'}</span>{@render name(line.label)}</span
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
        {@render more(line.depth, line.shown, line.total, () =>
          pages.set(path, (pages.get(path) ?? 0) + 1),
        )}
      {:else}
        {@render field(line, mark)}
      {/if}
    {/each}
    {#if split.removed.length > 0}
      <button
        type="button"
        class="line group removed-fold"
        aria-expanded={removedOpen}
        onclick={() => (removedOpen = !removedOpen)}
      >
        <span class="sign"></span><span class="chevron">{removedOpen ? '▾︎' : '▸︎'}</span>
        <span class="removed-names"
          >{split.removed.length} removed: {split.removed.map((n) => n.path).join(', ')}</span
        >
      </button>
      {#each removed as line, i (line.kind + line.path)}
        {#if line.kind === 'field'}{@render field(line, removedMarks[i])}{/if}
      {/each}
      {#if removed.length < removedAll.length}
        {@render more(0, removed.length, removedAll.length, () => removedPages++)}
      {/if}
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
    padding: 0.2rem 0 0.3rem 0.5rem;
  }

  /* A wrapped value makes the row as tall as it needs; the sign and the name
     stay on its first line rather than centering against it. */
  .line {
    position: relative;
    display: flex;
    align-items: baseline;
    gap: 0.5rem;
  }

  /* Runs apart by a gap, so two in a row read as two. */
  .run-start {
    margin-top: 0.3rem;
  }

  .bar {
    position: absolute;
    top: 0;
    bottom: 0;
    width: 3px;
    margin-left: -0.45rem;
    background: currentColor;
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

  .sign,
  .chevron {
    display: inline-block;
    width: 2ch;
    color: var(--dim);
  }

  /* The step that last touched the field, in the color of what it did, and on hover every step that did. */
  .by-glyph {
    border: 0;
    background: none;
    padding: 0;
    color: inherit;
    cursor: pointer;
  }

  .by-glyph:hover {
    color: var(--ink);
  }

  .by-list {
    display: flex;
    flex-direction: column;
    padding: 0.25rem;
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    box-shadow: 0 4px 14px rgba(0, 0, 0, 0.18);
    white-space: nowrap;
  }

  .by-list button {
    border: 0;
    background: none;
    padding: 0.1rem 0.4rem;
    text-align: left;
    font: inherit;
    color: var(--ink);
    cursor: pointer;
  }

  .by-list button:hover {
    background: color-mix(in srgb, var(--ink) 10%, var(--panel));
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

  /* Wrapped rather than clipped: a value the panel is too narrow for is still
     one someone has to read, and the field cannot be scrolled sideways. */
  .value {
    min-width: 0;
    overflow-wrap: anywhere;
    white-space: pre-wrap;
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

  .sign.state-added,
  .bar.state-added {
    color: var(--added);
  }

  .sign.state-changed,
  .bar.state-changed {
    color: var(--changed);
  }

  .sign.state-removed,
  .bar.state-removed {
    color: var(--removed);
  }

  .state-removed .name {
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
    padding: 0 0 0 calc(0.5rem + var(--key-w));
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
