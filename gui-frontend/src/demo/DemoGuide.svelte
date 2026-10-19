<!--
  The demo's guide, under its strip: the tasks a visitor works through (`guide.ts`), each ticked when its work is
  there. It opens on a first visit and remembers being closed. While open, it rings the control the next task,
  or the one whose Show me was followed, is done with, wherever the visitor is.
-->
<script lang="ts">
  import { onMount, untrack } from 'svelte';
  import { currentBranch, setScope } from '../api';
  import {
    CHANGED_EVENT,
    TASKS,
    look,
    sawStatus,
    stepOnScreen,
    type Step,
    type World,
  } from './guide';

  const OPEN_KEY = 'smeltr-demo-guide-open';
  const RING = 'data-guide-ring';

  let store: Storage | null = null;
  try {
    store = sessionStorage;
  } catch {
    // Blocked storage reopens the guide on every reload.
  }

  let open = $state(store?.getItem(OPEN_KEY) !== '0');
  let world = $state<World | null>(null);
  let done = $derived(TASKS.map((t) => (world ? t.done(world) : false)));
  let count = $derived(done.filter(Boolean).length);
  let next = $derived(done.indexOf(false));
  /** The task whose Show me was followed last; the ring follows it until it is done. */
  let shown = $state(-1);
  let ringing = $derived(shown >= 0 && !done[shown] ? shown : next);

  async function refresh() {
    world = await look(currentBranch(), sawStatus(location.hash, store));
  }

  let ringed: Element | null = null;
  // Raw: a proxy of the step's hint would never be the step's own, and the ring compares them.
  let hint = $state.raw<Step['hint']>();
  let copied = $state(false);

  /** Moves the ring to the next task's control on screen, and brings a newly ringed one into view. */
  function ring() {
    const task = open && world ? TASKS[ringing] : undefined;
    const found = task ? stepOnScreen(task, location.hash) : null;
    const el = found?.el ?? null;
    if (found?.step.hint !== untrack(() => hint)) copied = false;
    hint = found?.step.hint;
    if (el === ringed) return;
    ringed?.removeAttribute(RING);
    el?.setAttribute(RING, '');
    el?.scrollIntoView({ block: 'center', behavior: 'smooth' });
    ringed = el;
  }

  $effect(ring);

  async function copy(text: string) {
    try {
      await navigator.clipboard.writeText(text);
      copied = true;
    } catch {
      // No clipboard here; the text stays on screen to select.
    }
  }

  function toggle() {
    open = !open;
    try {
      store?.setItem(OPEN_KEY, open ? '1' : '0');
    } catch {
      // Not remembered, only.
    }
  }

  onMount(() => {
    void refresh();
    const again = () => void refresh();
    window.addEventListener('hashchange', again);
    window.addEventListener(CHANGED_EVENT, again);
    // Pages and dialogs draw their controls after the hash changes, and redraw and enable them as the visitor works.
    let frame = 0;
    const pages = new MutationObserver(() => {
      cancelAnimationFrame(frame);
      frame = requestAnimationFrame(ring);
    });
    pages.observe(document.body, {
      childList: true,
      subtree: true,
      attributeFilter: ['disabled', 'open'],
    });
    return () => {
      window.removeEventListener('hashchange', again);
      window.removeEventListener(CHANGED_EVENT, again);
      pages.disconnect();
      cancelAnimationFrame(frame);
      ringed?.removeAttribute(RING);
    };
  });
</script>

<button
  type="button"
  class="toggle"
  aria-expanded={open}
  aria-controls="demo-guide"
  onclick={toggle}
>
  Guide · {count}/{TASKS.length}
</button>

{#if open}
  <div class="guide" id="demo-guide">
    <h2>Fix two problems</h2>
    <ol>
      {#each TASKS as task, i (task.id)}
        <li class:done={done[i]} class:next={i === next}>
          <span class="mark" aria-hidden="true">{done[i] ? '✓' : i + 1}</span>
          <span class="text">{task.text}</span>
          {#if !done[i]}
            <a
              href={task.hash}
              onclick={(e) => {
                shown = i;
                if (!task.scope) return;
                e.preventDefault();
                setScope(task.scope, task.hash);
              }}>Show me</a
            >
          {/if}
          <span class="sr-only">{done[i] ? '(done)' : ''}</span>
        </li>
        {#if hint && i === ringing}
          <li class="hint">
            <span class="text">{hint.text}</span>
            <code>{hint.copy}</code>
            <button type="button" onclick={() => copy(hint!.copy)}
              >{copied ? 'Copied' : 'Copy'}</button
            >
          </li>
        {/if}
      {/each}
      <li class="last" class:next={next === -1}>
        <span class="mark" aria-hidden="true">→</span>
        <span class="text">
          Ready to ship it? On a real fleet you'd deploy now, and Status would clear.
          <a href="https://smeltr.sh/get/" target="_blank" rel="noopener">Get smeltr</a>
        </span>
      </li>
    </ol>
  </div>
{/if}

<style>
  .toggle {
    font: inherit;
    font-weight: 600;
    background: none;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 0.1rem 0.6rem;
    color: var(--ink);
    cursor: pointer;
  }

  .guide {
    flex-basis: 100%;
    padding-block: 0.4rem 0.6rem;
  }

  h2 {
    margin: 0 0 0.4rem;
    font-size: var(--text-label);
  }

  ol {
    list-style: none;
    margin: 0;
    padding: 0;
    display: grid;
    gap: 0.25rem;
  }

  li {
    display: flex;
    align-items: baseline;
    gap: 0.6rem;
    color: var(--dim);
  }

  li.next {
    color: var(--ink);
    font-weight: 600;
  }

  li.hint {
    flex-wrap: wrap;
    padding-left: 1.9rem;
    font-weight: 400;
    color: var(--ink);
  }

  li.hint code {
    font-family: var(--mono);
    font-size: var(--text-micro);
    overflow-wrap: anywhere;
  }

  li.hint button {
    font: inherit;
    font-size: var(--text-micro);
    background: none;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 0 0.5rem;
    color: var(--ink);
    cursor: pointer;
  }

  li.done .text {
    text-decoration: line-through;
  }

  .mark {
    display: inline-grid;
    place-items: center;
    min-width: 1.3rem;
    height: 1.3rem;
    border: 1px solid var(--border);
    border-radius: 50%;
    font-size: var(--text-micro);
  }

  li.done .mark {
    background: var(--added);
    border-color: var(--added);
    color: var(--on-accent);
  }

  :global([data-guide-ring]) {
    outline: 3px solid var(--accent);
    outline-offset: 3px;
    animation: guide-ring 1.6s ease-in-out infinite;
  }

  @keyframes -global-guide-ring {
    50% {
      outline-offset: 6px;
    }
  }

  @media (prefers-reduced-motion: reduce) {
    :global([data-guide-ring]) {
      animation: none;
    }
  }

  .sr-only {
    position: absolute;
    width: 1px;
    height: 1px;
    overflow: hidden;
    clip-path: inset(50%);
  }
</style>
