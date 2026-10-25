<!--
  An `exceptions` layer (`ExceptionsDto`) as an answer matrix: a row for each of `rows`, categories with their kinds
  under them, and a column for each answer. The group's on the Routes page, a route's or a destination's in its
  overrides.

  A row's dot sits under its answer: filled where this layer answers the row, hollow where the row inherits, from its
  category or from `beneath`. A category whose kinds answer differently shows half a dot under each of their answers.
  A dash marks an answer the row's kinds cannot take.

  A category folds its kinds away. It starts open where this layer answers one of its kinds.
-->
<script lang="ts">
  import { untrack } from 'svelte';
  import Help from './Help.svelte';
  import Locked from './Locked.svelte';
  import {
    ANSWERS,
    KINDS,
    KIND_HELP,
    isCategory,
    kindsOf,
    lay,
    nameOf,
    ownAnswer,
    possible,
    set,
    type Answer,
    type AnswerName,
    type ExceptionsDto,
  } from './exceptions';

  let {
    value = $bindable(),
    rows,
    beneath,
    fixed = false,
    targets,
    name,
  }: {
    value: ExceptionsDto | undefined;
    /** The categories and kinds to show, in order. */
    rows: string[];
    /** Each kind's answer where this layer says nothing. */
    beneath: Record<string, Answer>;
    /** A category answered here stays answered: picking its answer again does not unset it. */
    fixed?: boolean;
    /** The destinations a deliver answer may name. */
    targets: string[];
    /** What this answers for, for a screen reader. */
    name: string;
  } = $props();

  let answers = $derived(lay(beneath, value));
  /** Rows whose deliver answer has a destination and is picking another. */
  let adding = $state<string[]>([]);
  /** Categories whose kinds are shown. Seeded once, so answering a kind does not open or close anything. */
  let open = $state<string[]>(
    untrack(() =>
      rows.filter(
        (r) => isCategory(r) && kindsOf(r).some((k) => ownAnswer(value, k) !== undefined),
      ),
    ),
  );
  /** A kind row is hidden when its category is shown above it and folded. */
  const categoryOf = (kind: string) => KINDS.find(([k]) => k === kind)?.[1];
  let shownRows = $derived(
    rows.filter((r) => {
      const c = isCategory(r) ? undefined : categoryOf(r);
      return c === undefined || !rows.includes(c) || open.includes(c);
    }),
  );

  /** `mix`: only some of the row's kinds take `answer`, so picking it gives it to all of them. */
  function pick(row: string, answer: AnswerName, mix: boolean) {
    const own = ownAnswer(value, row);
    if (own !== undefined && nameOf(own) === answer) {
      if (mix) value = set(value, row, own);
      else if (!(fixed && isCategory(row))) value = set(value, row, undefined);
      return;
    }
    value = set(value, row, answer === 'deliver' ? { deliver: [] } : answer);
  }

  function deliverTo(row: string, to: string[]) {
    // In place, not through `set`: the kinds under a category keep their own answers.
    const key = isCategory(row) ? 'categories' : 'kinds';
    value = { ...value, [key]: { ...value?.[key], [row]: { deliver: to } } };
    adding = adding.filter((r) => r !== row);
  }
</script>

<div class="matrix">
  <p class="legend">
    <span><span class="mark set"></span>set here</span>
    <span><span class="mark"></span>inherited</span>
    <span><span class="mark mix"></span>some kinds</span>
    <span><span class="mark no"></span>not possible</span>
  </p>
  <div class="mx-row mx-head" aria-hidden="true">
    <span></span>{#each ANSWERS as answer (answer)}<span>{answer}</span>{/each}
  </div>
  {#each shownRows as row (row)}
    {@const own = ownAnswer(value, row)}
    {@const shown = [...new Set(kindsOf(row).map((k) => nameOf(answers[k])))]}
    {@const level = isCategory(row) ? 'category' : 'kind'}
    {@const alone = level === 'kind' && !rows.includes(categoryOf(row) ?? '')}
    <div class="mx-row {level}" class:alone role="radiogroup" aria-label="{name}: {row}">
      <span class="mx-name"
        >{#if level === 'category'}{@const isOpen = open.includes(row)}<button
            type="button"
            class="fold"
            aria-expanded={isOpen}
            aria-label={row}
            onclick={() => (open = isOpen ? open.filter((c) => c !== row) : [...open, row])}
            >{isOpen ? '▾' : '▸'}</button
          >{/if}{row}{#if level === 'kind'}<Help label={row}>{KIND_HELP[row]}</Help>{/if}</span
      >
      <!-- The answers are read-only where the page is not the person's to change (`access.ts`); the folds are not. -->
      <Locked>
        {#each ANSWERS as answer (answer)}
          {@const mix = shown.length > 1 && shown.includes(answer)}
          {@const isSet = !mix && own !== undefined && nameOf(own) === answer}
          {@const inherited = !mix && own === undefined && shown.includes(answer)}
          <button
            type="button"
            role="radio"
            class="dot"
            class:set={isSet}
            class:inh={inherited}
            class:mix
            class:no={!possible(row, answer)}
            disabled={!possible(row, answer)}
            aria-checked={isSet || inherited}
            aria-label="{row}: {answer}{!possible(row, answer)
              ? ', not possible'
              : inherited
                ? ', inherited'
                : mix
                  ? ', some kinds'
                  : ''}"
            onclick={() => pick(row, answer, mix)}
          ></button>
        {/each}
      </Locked>
    </div>
    <Locked>
      {#if own !== undefined && typeof own === 'object'}
        {@const left = targets.filter((t) => !own.deliver.includes(t))}
        <div class="deliver">
          <span class="dim">to</span>
          {#each own.deliver as t (t)}
            <span class="chip"
              >{t}<button
                type="button"
                aria-label="Remove {t}"
                onclick={() =>
                  deliverTo(
                    row,
                    own.deliver.filter((x) => x !== t),
                  )}>×</button
              ></span
            >
          {/each}
          {#if left.length > 0}
            {#if own.deliver.length > 0 && !adding.includes(row)}
              <button
                type="button"
                class="plus"
                aria-label="{row}: add destination"
                onclick={() => (adding = [...adding, row])}>+</button
              >
            {:else}
              <select
                class="chip"
                aria-label="{row}: add destination"
                onchange={(e) => deliverTo(row, [...own.deliver, e.currentTarget.value])}
              >
                <option value="">{own.deliver.length > 0 ? '+' : 'choose destination'}</option>
                {#each left as t (t)}<option>{t}</option>{/each}
              </select>
            {/if}
          {/if}
        </div>
      {/if}
    </Locked>
  {/each}
</div>

<style>
  .matrix {
    width: 100%;
    border-top: 1px solid var(--border);
  }
  .legend {
    display: flex;
    flex-wrap: wrap;
    gap: 0.2rem 0.9rem;
    margin: 0;
    padding: 0.3rem 0.25rem;
    font-size: var(--text-note);
    color: var(--dim);
  }
  .mx-row {
    display: grid;
    grid-template-columns: minmax(0, 1fr) repeat(5, 3rem);
    align-items: center;
    border-bottom: 1px solid var(--border);
    padding: 0 0.25rem;
  }
  .mx-head span {
    font-family: var(--mono);
    font-size: var(--text-micro);
    color: var(--dim);
    text-align: center;
  }
  .mx-row:not(.mx-head):is(:hover, :focus-within) {
    background: color-mix(in oklch, var(--ink) 4%, transparent);
  }
  .category {
    background: color-mix(in oklch, var(--ink) 3%, transparent);
  }
  .mx-name {
    display: flex;
    align-items: baseline;
    gap: 0 0.4rem;
    min-width: 0;
    overflow-wrap: anywhere;
    font-family: var(--mono);
    font-size: var(--text-label);
  }
  .fold {
    border: 0;
    background: none;
    padding: 0;
    width: 1rem;
    height: auto;
    color: var(--dim);
    cursor: pointer;
  }
  /* Under the category's name, past its caret. */
  .kind .mx-name {
    padding-left: 2rem;
    font-size: var(--text-note);
  }
  /* A kind shown without its category stands level with the categories' names. */
  .kind.alone .mx-name {
    padding-left: 1.4rem;
    font-size: var(--text-label);
  }
  .deliver {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.35rem 0.6rem;
    padding: 0.4rem 0.25rem 0.5rem 1.25rem;
    border-bottom: 1px solid var(--border);
    font-size: var(--text-note);
  }
  .chip,
  .plus {
    font-family: var(--mono);
    font-size: var(--text-micro);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    background: var(--field);
    color: var(--ink);
    padding: 0.1rem 0.4rem;
    height: auto;
  }
  .chip button {
    border: 0;
    background: none;
    color: var(--dim);
    padding: 0 0 0 0.35rem;
    font-size: inherit;
  }
  /* A speck where the column is open, a hollow ring where the row inherits, a filled dot where it is set here, half
     of one where only some kinds take it, and a dash where it is not possible. */
  button.dot {
    border: 0;
    background: none;
    padding: 0;
    height: 1.85rem;
    display: flex;
    align-items: center;
    justify-content: center;
    cursor: pointer;
  }
  .dot::before,
  .mark {
    content: '';
    display: inline-block;
    width: 0.3rem;
    height: 0.3rem;
    border-radius: 999px;
    background: var(--border);
  }
  .mark {
    margin: 0 0.3rem 0 0.2rem;
  }
  .dot:hover:not(:disabled)::before,
  :is(.dot.inh, .dot.set, .dot.mix)::before,
  .mark {
    width: 0.7rem;
    height: 0.7rem;
    background: none;
    border: 1.5px solid var(--ink);
  }
  .dot:hover:not(.set, .inh, .mix)::before {
    border-color: var(--dim);
  }
  /* Hovered too: the hover ring above would empty it. */
  .dot.set::before,
  .dot.set:hover::before,
  .mark.set {
    background: var(--ink);
  }
  .dot.mix::before,
  .dot.mix:hover::before,
  .mark.mix {
    background: linear-gradient(90deg, var(--ink) 50%, transparent 50%);
  }
  .dot:disabled {
    cursor: not-allowed;
  }
  /* By what is possible, not by `disabled`: a read-only page disables every dot. */
  .dot.no::before,
  .mark.no {
    width: 0.6rem;
    height: 1.5px;
    border: 0;
    border-radius: 0;
    background: var(--border);
  }
  @media (max-width: 480px) {
    .mx-row {
      grid-template-columns: minmax(0, 1fr) repeat(5, 2.4rem);
      padding: 0;
    }
    .kind .mx-name {
      padding-left: 1.6rem;
    }
  }
</style>
