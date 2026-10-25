<!--
  The find box over a list of records. Its text is a keyword by default: a record stays when it holds the text. Text
  with an operator or a call in it is tried as an expression (`PreviewRequest.filter`); one that compiles to a Bool
  keeps the rows it is true for, and anything else stays a keyword, so a bare word like `slow` is searched for rather
  than read as a field. The mode beside the text says which it is, and a click reads it the other way; asked for, an
  expression that does not compile keeps no rows and says why.

  The expressions it was last left with are offered again, after whatever the caller offers.
-->
<script lang="ts" module>
  /** How long typing settles before a preview runs over it: the steps', and the find box's expression. */
  export const RECOMPUTE_MS = 400;

  /** What the box keeps. A keyword is the caller's to match; an expression's rows are the preview's row numbers. */
  export type FindFilter =
    | { kind: 'none' }
    | { kind: 'keyword'; text: string }
    | { kind: 'expression'; rows: Set<number> }
    /** An expression that cannot run now (`paused`): it keeps every row until it can. */
    | { kind: 'paused' };
</script>

<script lang="ts" generics="T extends { section?: string }">
  import type { Snippet } from 'svelte';
  import { untrack } from 'svelte';
  import SearchBox from './SearchBox.svelte';
  import { previewFailure, type PreviewResult } from './api';
  import { store, storedJson } from './stored';

  let {
    query = $bindable(''),
    filter = $bindable({ kind: 'none' }),
    error = $bindable(''),
    evaluate,
    paused = false,
    remember,
    items = [],
    onpick,
    option: itemOption,
    before,
    id,
    label,
    placeholder,
  }: {
    query?: string;
    /** Out: what the text keeps. */
    filter?: FindFilter;
    /** Out: why an expression asked for does not compile. */
    error?: string;
    /** Runs the expression; a new one runs it again. Without one, the text is a keyword. */
    evaluate: ((expression: string) => Promise<PreviewResult>) | null;
    /** An expression waits rather than runs. */
    paused?: boolean;
    /** What the recent expressions are kept under. */
    remember: string;
    /** Offered ahead of the recent expressions. */
    items?: T[];
    onpick?: (item: T) => boolean | void;
    option?: Snippet<[T]>;
    /** Inside the box, ahead of the text: what already narrows the search. */
    before?: Snippet;
    id: string;
    label: string;
    placeholder: string;
  } = $props();

  const RECENT_EXPRESSIONS = 5;

  const looksLikeExpression = (q: string) => /[=<>!&|()]/.test(q);
  /** How the reader said the text is read, by the mode beside it, over what it looks like. Cleared with the text. */
  let readAs = $state<'keyword' | 'expression' | null>(null);
  $effect(() => {
    if (!query.trim()) readAs = null;
  });
  /** What the mode says: the reader's choice at once, else the guess an answer confirmed. */
  let asExpression = $derived(
    readAs ? readAs === 'expression' : filter.kind === 'expression' || filter.kind === 'paused',
  );

  $effect(() => {
    const [q, as, run, wait] = [query.trim(), readAs, evaluate, paused];
    error = '';
    if (!q) {
      filter = { kind: 'none' };
      return;
    }
    const keyword = { kind: 'keyword' as const, text: q };
    if (!run || as === 'keyword' || (as === null && !looksLikeExpression(q))) {
      filter = keyword;
      return;
    }
    if (wait) {
      filter = { kind: 'paused' };
      return;
    }
    // Asked for, an expression that does not compile keeps no rows and says why; guessed, it stays a keyword.
    const failed = (why: string) => {
      filter = as === 'expression' ? { kind: 'expression', rows: new Set() } : keyword;
      error = as === 'expression' ? why : '';
    };
    // Until it answers, the rows an expression kept stay; with none, the text is searched for.
    if (untrack(() => filter.kind) !== 'expression') filter = keyword;
    let current = true;
    const timer = setTimeout(async () => {
      try {
        const res = await run(q);
        if (!current) return;
        if (res.ok) filter = { kind: 'expression', rows: new Set(res.rows.map((r) => r.row)) };
        else failed(previewFailure(res));
      } catch (e) {
        if (current) failed(String(e));
      }
    }, RECOMPUTE_MS);
    return () => {
      current = false;
      clearTimeout(timer);
    };
  });

  let recent = $state<string[]>([]);
  $effect(() => {
    const key = remember;
    untrack(() => {
      const saved = storedJson<unknown>(key, []);
      recent = Array.isArray(saved) ? saved.filter((e): e is string => typeof e === 'string') : [];
    });
  });

  /** Keeps the expression the rows are filtered by, once the box is left with it. */
  function rememberExpression() {
    const q = query.trim();
    if (!q || filter.kind !== 'expression') return;
    recent = [q, ...recent.filter((e) => e !== q)].slice(0, RECENT_EXPRESSIONS);
    store(remember, JSON.stringify(recent));
  }

  type Recent = { recent: string; section: string };
  let offered = $derived.by((): (T | Recent)[] => {
    const q = query.trim();
    const again = recent
      .filter((e) => e !== q && e.toLowerCase().includes(q.toLowerCase()))
      .map((e) => ({ recent: e, section: 'Recent' }));
    return [...items, ...again];
  });
  const isRecent = (item: T | Recent): item is Recent => 'recent' in item;

  function pick(item: T | Recent): boolean | void {
    if (!isRecent(item)) return onpick?.(item);
    query = item.recent;
    return true;
  }
</script>

<SearchBox
  bind:query
  items={offered}
  onpick={pick}
  onleave={rememberExpression}
  {id}
  {label}
  {placeholder}
  mono={asExpression}
  {before}
>
  {#snippet after()}{#if query.trim()}<button
        type="button"
        class="mode"
        class:expression={asExpression}
        onclick={() => (readAs = asExpression ? 'keyword' : 'expression')}
        >{asExpression ? 'ƒ expression' : 'keyword'}</button
      >{/if}{/snippet}
  {#snippet option(item)}
    {#if isRecent(item)}
      <code>{item.recent}</code>
    {:else}
      {@render itemOption?.(item)}
    {/if}
  {/snippet}
</SearchBox>

<style>
  .mode {
    flex: none;
    background: none;
    cursor: pointer;
    font: inherit;
    font-size: var(--text-note);
    color: var(--dim);
    border: 1px solid var(--border);
    border-radius: 999px;
    padding: 0 0.5rem;
    white-space: nowrap;
  }
  .mode.expression {
    color: var(--accent);
    border-color: var(--accent);
    font-family: var(--mono);
  }
</style>
