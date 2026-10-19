<!--
  One record, as the preview draws it (`recordDiff.ts`). Nesting is indent and
  a longer name rather than braces; what a step did to a field is a character
  in the gutter and a color on the field's name, so the values themselves
  stay plain text at one left edge. A type sits at the right of the name
  column, which is fixed and mostly empty, so values keep their width.

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
    recordLines,
    splitPath,
    valueFitsUnclamped,
    type Declared,
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

  let tree = $derived(diffTree(before, after, keep, declared));

  // The reader's choices, by a struct's or an array's path: open or folded,
  // and how many pages past the first it draws.
  let folds = $state(new SvelteMap<string, boolean>());
  let pages = $state(new SvelteMap<string, number>());

  let lines = $derived.by(() => {
    const choices = { open: (p: string) => folds.get(p), pages: (p: string) => pages.get(p) };
    return changedOnly ? changedLines(tree, choices) : recordLines(tree, choices);
  });

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

{#if lines.length === 0}
  <span class="dim">no change at this step</span>
{:else}
  <div class="fields">
    {#each lines as line (line.kind + line.path + line.depth)}
      {#if line.kind === 'group'}
        <button
          type="button"
          class="group"
          style={`padding-left: ${line.depth * 14}px`}
          aria-expanded={line.open}
          onclick={() => folds.set(line.path, !line.open)}
        >
          <span class="group-name" style={`width: calc(13.2rem - ${line.depth * 14}px)`}>
            <span class="chevron">{line.open ? '▾︎' : '▸︎'}</span>
            <span class="name">{line.label}</span>
            {#if showTypes && line.type !== null}<span class="type">{line.type}</span>{/if}
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
          <span class="dim" style={`margin-left: ${line.depth * 14}px`}
            >{line.shown} of {line.total} shown ·&nbsp;</span
          ><button
            type="button"
            class="more"
            onclick={() => pages.set(path, (pages.get(path) ?? 0) + 1)}>show more</button
          >
        </div>
      {:else}
        {@const inline = line.state === 'changed' && changeFitsInline(line.before, line.after)}
        {@const body = formatDiffValue(line.state === 'removed' ? line.before : line.after)}
        {@const opens = !inline && !valueFitsUnclamped(body)}
        <div class="line state-{line.state}" class:open={opens && opened.has(line.path)}>
          <span class="gutter">{GLYPH[line.state]}</span>
          <span class="key" style={`padding-left: ${line.depth * 14}px`}>
            <span class="name">
              {#if changedOnly}
                {@const name = splitPath(line.label)}
                <span class="path-prefix">{name.prefix}</span>{name.leaf}
              {:else}
                {line.label}
              {/if}
            </span>
            {#if showTypes && line.type !== null}<span class="type">{line.type}</span>{/if}
          </span>
          {#if line.state === 'changed' && inline}
            <span class="value">
              <span class="gone">{formatDiffValue(line.before)}</span>
              <span class="arrow">&rarr;</span>{formatDiffValue(line.after)}
            </span>
          {:else}
            {@const open = opened.has(line.path)}
            <span class="value" class:gone={line.state === 'removed'} class:clamped={opens && !open}
              >{body}</span
            >
            {#if opens}
              <button
                type="button"
                class="more"
                aria-expanded={open}
                aria-label={`${open ? 'collapse' : 'expand'} the value of ${line.path}`}
                onclick={() => toggleLong(line.path)}>{open ? 'less' : 'more'}</button
              >
            {/if}
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
      {/if}
    {/each}
  </div>
{/if}

<style>
  .dim {
    color: var(--dim);
    font-weight: 400;
  }

  .fields {
    font-family: var(--mono);
    font-size: var(--text-note);
    line-height: 1.75;
    padding: 0.2rem 0 0.3rem;
  }

  .group {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    width: 100%;
    margin-left: 1.05rem;
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

  /* A fixed column, like the mark gutter beside a field: the label does not
     shift with whether a group happens to be open. */
  .chevron {
    display: inline-block;
    width: 0.75rem;
    color: var(--dim);
  }

  .group-marks {
    display: flex;
    gap: 0.5rem;
    margin-left: 0.6rem;
    font-weight: 400;
  }

  /* A wrapped value makes the row as tall as it needs; the mark and the name
     stay on its first line rather than centering against it. */
  .line {
    display: flex;
    align-items: baseline;
    gap: 0.5rem;
  }

  /* Opened, the value drops below its name and takes the whole width: a raw
     line beside a 13rem key would be set in a ribbon. The control stays up on
     the name's line. */
  .line.open {
    flex-wrap: wrap;
  }

  .line.open .value {
    order: 1;
    flex-basis: 100%;
    margin-left: 1.25rem;
  }

  /* The gutter is a fixed column at the far left: the mark never moves with
     the depth of the field it belongs to. */
  .gutter {
    flex-shrink: 0;
    width: 0.75rem;
    text-align: center;
    color: var(--dim);
  }

  .key {
    display: flex;
    flex-shrink: 0;
    box-sizing: border-box;
    width: 13rem;
    color: var(--dim);
  }

  /* A long name gives way before the type does: the type is short, and says
     what the value beside it is. */
  .name {
    flex: 1 1 0;
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  /* At the right of the name column, so types line up down the record
     without taking width from the values. */
  .type {
    flex-shrink: 0;
    margin-left: 0.6rem;
    color: var(--dim);
    font-weight: 400;
  }

  /* As wide as a field's gutter and name together, so a group's type lines up
     with its fields'. */
  .group-name {
    display: flex;
    flex-shrink: 0;
    align-items: center;
    gap: 0.4rem;
  }

  /* Every value starts here, whatever its key's depth. Wrapped rather than
     clipped: a value the panel is too narrow for is still one someone has to
     read, and the field cannot be scrolled sideways. */
  .value {
    min-width: 0;
    overflow-wrap: anywhere;
    white-space: pre-wrap;
  }

  /* Two lines is enough to recognize a value by; past that it is opened
     deliberately, so one long field cannot push the rest of the record off
     the panel. */
  .clamped {
    display: -webkit-box;
    -webkit-line-clamp: 2;
    line-clamp: 2;
    -webkit-box-orient: vertical;
    overflow: hidden;
  }

  .more {
    flex-shrink: 0;
    align-self: flex-start;
    border: none;
    background: none;
    padding: 0;
    font: inherit;
    color: var(--ink);
    text-decoration: underline;
    text-underline-offset: 2px;
    cursor: pointer;
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

  .state-added .gutter,
  .state-added .key {
    color: var(--added);
  }

  .state-changed .gutter,
  .state-changed .key {
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

  /* Sits under the value it belongs to, and is as wide as the line so the
     rule is a target rather than a hairline to hit. */
  .held {
    display: flex;
    width: 100%;
    padding: 0 0 0 14.25rem;
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
