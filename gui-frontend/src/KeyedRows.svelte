<!--
  A map edited as rows: each row a key and the columns `cells` draws for its value. The rows are the editing state
  and the caller rebuilds its map from them on `onchange`: a key mid-edit is briefly blank or a duplicate, and must
  not lose its row. A row whose key an earlier row already names is not written; `refusal` says why a row cannot be
  saved, under the row.
-->
<script module lang="ts">
  /** Whether `rows[i]` names a key an earlier row already names, and so is not written. */
  export function shadowed(rows: { key: string }[], i: number): boolean {
    const key = rows[i].key.trim();
    return key !== '' && rows.slice(0, i).some((r) => r.key.trim() === key);
  }
</script>

<script lang="ts" generics="R extends { key: string }">
  import Field from './Field.svelte';
  import type { Snippet } from 'svelte';
  import { editable } from './access';

  let {
    rows,
    field,
    label,
    keyLabel,
    keyPlaceholder,
    add,
    blank,
    help,
    cells,
    refusal,
    below,
    onchange,
  }: {
    /** The rows, a `$state` array this adds to and removes from. */
    rows: R[];
    field: string;
    label: string;
    /** The key input's accessible name. */
    keyLabel: string;
    keyPlaceholder: string;
    /** The add button's text. */
    add: string;
    /** A new row. */
    blank: () => R;
    help: Snippet;
    /** The value's columns for `row`, after its key. */
    cells: Snippet<[R]>;
    /** Why `rows[i]` cannot be saved, or nothing. */
    refusal: (i: number) => string | undefined;
    /** What else to say under `row`, when it is not refused. */
    below?: Snippet<[R]>;
    /** Called after any row changes. */
    onchange: () => void;
  } = $props();

  const readOnly = !editable();
</script>

<Field {field} {label} group {help}>
  <div class="rows">
    {#each rows as row, i (i)}
      <div class="row">
        <input
          type="text"
          class="key"
          placeholder={keyPlaceholder}
          value={row.key}
          oninput={(e) => {
            row.key = e.currentTarget.value;
            onchange();
          }}
          aria-label={keyLabel}
        />
        {@render cells(row)}
        {#if !readOnly}<button
            type="button"
            class="compact"
            title="remove"
            onclick={() => {
              rows.splice(i, 1);
              onchange();
            }}>&#10005;</button
          >{/if}
      </div>
      {@const refused = refusal(i)}
      {#if refused}
        <div class="refused">{refused}</div>
      {:else if below}
        {@render below(row)}
      {/if}
    {/each}
  </div>
  {#if !readOnly}
    <button type="button" class="add-small" onclick={() => rows.push(blank())}>{add}</button>
  {/if}
</Field>

<style>
  .rows {
    display: flex;
    flex-direction: column;
    gap: 0.3rem;
    max-width: 100%;
    overflow-x: auto;
  }

  .row {
    display: flex;
    gap: 0.35rem;
    align-items: center;
    width: max-content;
  }

  .row > :global(input) {
    flex: none;
    font-family: var(--mono);
  }

  /* What the save would refuse or leave out, said in the row that caused it. */
  .refused {
    color: var(--error);
    font-size: var(--text-note);
    line-height: 1.4;
    max-width: 34rem;
  }

  /* Dashed to read as adding rather than acting on what is already there. */
  .add-small {
    align-self: flex-start;
    /* A grid stretches a child across its column; the button stays its own width. */
    justify-self: start;
    background: none;
    border: 1px dashed var(--dim);
    padding: 0.2rem 0.55rem;
    font-size: var(--text-note);
  }
</style>
