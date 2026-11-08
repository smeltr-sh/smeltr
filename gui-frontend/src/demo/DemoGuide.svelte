<!--
  The demo's guide, under its strip: the tasks a visitor works through (`guide.ts`), each ticked when its work is
  there. It opens on a first visit and remembers being closed. While open, it rings the control the next task,
  or the one whose Show me was followed, is done with, wherever the visitor is. A click on it, off its links and
  buttons, folds it.
-->
<script lang="ts">
  import { onMount } from 'svelte';
  import CopyButton from '../CopyButton.svelte';
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
  /** The video that works through these same tasks in this demo. */
  const WALKTHROUGH_URL = 'https://smeltr.sh/learn/demo-walkthrough/fixing-a-feed';
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

  /** Folds the guide on a click anywhere in it but its links and buttons, or text being selected. */
  function fold(e: MouseEvent) {
    if ((e.target as Element).closest('a, button, code')) return;
    if (getSelection()?.toString()) return;
    toggle();
  }

  async function refresh() {
    world = await look(currentBranch(), sawStatus(location.hash, store));
  }

  let ringed: Element | null = null;
  // Raw: a proxy of the step's hint would never be the step's own, and the ring compares them.
  let hint = $state.raw<Step['hint']>();

  /** Moves the ring to the next task's control on screen, and brings a newly ringed one into view. */
  function ring() {
    const task = open && world ? TASKS[ringing] : undefined;
    const found = task ? stepOnScreen(task, location.hash) : null;
    const el = found?.el ?? null;
    hint = found?.step.hint;
    if (el === ringed) return;
    ringed?.removeAttribute(RING);
    el?.setAttribute(RING, '');
    el?.scrollIntoView({ block: 'center', behavior: 'smooth' });
    ringed = el;
  }

  $effect(ring);

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
  <!-- The Guide button folds it from the keyboard; a click on the guide itself is a shortcut for the mouse. -->
  <!-- svelte-ignore a11y_click_events_have_key_events, a11y_no_static_element_interactions -->
  <div class="guide" id="demo-guide" onclick={fold}>
    <h2>Fix two problems</h2>
    <p>
      Cisco ASA logs are arriving with no route to take them, and one FortiGate's firmware now sends
      proto as a name instead of a number.
      <a href={WALKTHROUGH_URL} target="_blank" rel="noopener">Stuck? Watch the walkthrough</a>
    </p>
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
            <CopyButton text={hint.copy} />
          </li>
        {/if}
      {/each}
      <li class="last" class:next={next === -1}>
        <span class="mark" aria-hidden="true">→</span>
        <span class="text">
          Ready to ship it? On a real fleet a reviewer would approve and deploy it now, and Status
          would clear.
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
    cursor: pointer;
    padding-block: 0.4rem 0.6rem;
  }

  h2 {
    margin: 0 0 0.4rem;
    font-size: var(--text-label);
  }

  p {
    margin: 0 0 0.4rem;
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

  li.hint :global(button) {
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
