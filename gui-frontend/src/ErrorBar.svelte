<!--
  What the leader refused or failed to answer, from every call (`errors.svelte.ts`). App mounts one under the nav,
  held at the top of the window as the page scrolls, and a dialog mounts one over its body, since its backdrop hides
  the page's: `since` keeps that one to what was reported while the dialog was open.

  An error a field or a step holds is a link here, which opens what hides it and goes to it. A severe one is told first,
  in the app's notice (`tell`), and stays here once read.
-->
<script lang="ts">
  import { errors, pageOf } from './errors.svelte';
  import { tell } from './Confirm.svelte';
  import { keepClear } from './keepClear';
  import StateMark from './StateMark.svelte';

  let { since }: { since?: number } = $props();

  const shown = $derived(
    errors.entries.filter(
      (e) =>
        (since === undefined || e.seq > since) &&
        (e.errors.length > 0 || (e.placed?.().length ?? 0) > 0),
    ),
  );

  // The page's own bar clears what another page reported, and tells a severe one: a dialog's would tell it twice.
  $effect(() => {
    if (since !== undefined) return;
    const left = () => errors.left(pageOf(location.hash));
    addEventListener('hashchange', left);
    return () => removeEventListener('hashchange', left);
  });
  $effect(() => {
    if (since !== undefined || errors.entries.length === 0) return;
    for (const entry of errors.untold()) {
      const said = [...entry.errors, ...(entry.placed?.() ?? [])].map((e) => e.message);
      void tell({ title: 'not deployed', body: said.join('\n') });
    }
  });
</script>

{#if shown.length > 0}
  <div
    class="error-bar"
    class:in-dialog={since !== undefined}
    {@attach since === undefined && keepClear('top')}
  >
    {#each shown as entry (entry.seq)}
      <div class="entry">
        <span class="mark"><StateMark state="error" size={16} /></span>
        <!-- The alert is the messages alone, without the mark and the dismiss beside them. -->
        <ul role="alert">
          {#each entry.placed?.() ?? [] as error, i (i)}
            <li>
              <a
                href={location.hash}
                onclick={(e) => {
                  e.preventDefault();
                  error.go();
                }}><span class="where">{error.where}</span> {error.message}</a
              >
            </li>
          {/each}
          {#each entry.errors as error, i (i)}
            <li>{error.message}</li>
          {/each}
        </ul>
        <button
          type="button"
          class="dismiss"
          aria-label="Dismiss"
          title="Dismiss"
          onclick={() => errors.dismiss(entry)}>&times;</button
        >
      </div>
    {/each}
  </div>
{/if}

<style>
  .error-bar {
    position: sticky;
    top: 0;
    z-index: 5;
    background: color-mix(in srgb, var(--error) 12%, var(--bg));
    border-bottom: 1px solid color-mix(in srgb, var(--error) 45%, var(--border));
  }

  .error-bar.in-dialog {
    position: static;
    border: 1px solid color-mix(in srgb, var(--error) 45%, var(--border));
    border-radius: var(--radius-card);
    margin-bottom: 0.8rem;
  }

  .entry {
    display: flex;
    align-items: flex-start;
    gap: 0.6rem;
    max-width: var(--measure, var(--width-page));
    margin-inline: auto;
    padding: 0.45rem 1.5rem;
    font-size: var(--text-note);
  }

  .in-dialog .entry {
    padding: 0.45rem 0.7rem;
  }

  .entry + .entry {
    border-top: 1px solid color-mix(in srgb, var(--error) 25%, var(--border));
  }

  .mark {
    flex: none;
    display: flex;
    margin-top: 0.15rem;
  }

  ul {
    flex: 1;
    min-width: 0;
    margin: 0;
    padding: 0;
    list-style: none;
    display: grid;
    gap: 0.2rem;
    white-space: pre-line;
    overflow-wrap: anywhere;
  }

  a {
    color: var(--error);
  }

  .where {
    font-family: var(--mono);
    font-weight: 600;
  }

  .dismiss {
    flex: none;
    padding: 0 0.35rem;
    border: none;
    background: none;
    color: var(--dim);
    font-size: 1.1rem;
    line-height: 1;
    cursor: pointer;
  }
</style>
