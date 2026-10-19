<!--
  One choice among a few views of the same thing, side by side: the pressed
  segment is the one on screen.
-->
<script lang="ts" generics="T extends string">
  let {
    options,
    value = $bindable(),
    label,
    marked,
  }: {
    options: { value: T; label: string }[];
    value: T;
    /** What is being chosen, for a screen reader. */
    label: string;
    /** The option that is the one in effect, where looking at another changes nothing until it is acted on. */
    marked?: T;
  } = $props();
</script>

<div class="seg" role="group" aria-label={label}>
  {#each options as option (option.value)}
    <button
      type="button"
      aria-pressed={value === option.value}
      aria-current={marked === option.value ? 'true' : undefined}
      onclick={() => (value = option.value)}
      >{option.label}{#if marked === option.value}<span class="mark" aria-hidden="true"
          >&#10003;</span
        >{/if}</button
    >
  {/each}
</div>

<style>
  .seg {
    display: inline-flex;
    border: 1px solid var(--border);
    border-radius: 999px;
    overflow: hidden;
    background: var(--bg);
  }
  .seg button {
    border: 0;
    border-radius: 0;
    background: none;
    padding: 0.25rem 0.8rem;
    font-size: var(--text-label);
    font-weight: 500;
    color: var(--dim);
  }
  .seg button + button {
    border-left: 1px solid var(--border);
  }
  .seg button[aria-pressed='true'] {
    background: var(--ink);
    color: var(--bg);
  }
  .mark {
    margin-left: 0.35rem;
  }
  .seg button:focus-visible {
    outline: 2px solid var(--accent);
    outline-offset: -2px;
  }
</style>
