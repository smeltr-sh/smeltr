<!--
  The person's agent, folded up from the bottom of whatever page they are on, so they can ask about what is in
  front of them without leaving it. The same conversation as on the Agent page; the panel keeps which one is open
  for the browser tab, and how tall the person dragged it.
-->
<script lang="ts">
  import Conversation from './Conversation.svelte';
  import { reported } from './errors.svelte';
  import { listConversations, refused, type ConversationSummaryDto } from './api';

  let { onclose }: { onclose: () => void } = $props();

  const OPEN_KEY = 'smeltr-agent-panel-conversation';
  const HEIGHT_KEY = 'smeltr-agent-panel-height';
  /** The share of the window the panel takes, and the least and most it may be dragged to. A phone gives it
   * more, since a third of a phone holds little more than the message box. */
  const HEIGHT = matchMedia('(max-width: 48rem)').matches ? 0.55 : 0.3;
  const LEAST = 0.15;
  const MOST = 0.8;

  let open = $state(sessionStorage.getItem(OPEN_KEY) ?? '');
  let height = $state(Number(localStorage.getItem(HEIGHT_KEY)) || HEIGHT);
  let conversations = $state<ConversationSummaryDto[]>([]);
  let body = $state<HTMLElement>();

  async function refresh() {
    try {
      const listed = await listConversations();
      if (refused(listed)) return;
      conversations = listed.conversations;
      if (open && !conversations.some((c) => c.id === open)) open = '';
    } catch (e) {
      reported(e);
    }
  }

  $effect(() => {
    void refresh();
  });

  $effect(() => {
    sessionStorage.setItem(OPEN_KEY, open);
  });

  // A chat reads from its newest message: whatever the conversation grows by keeps the panel at its end.
  $effect(() => {
    if (!body) return;
    const end = () => body!.scrollTo({ top: body!.scrollHeight });
    const grew = new ResizeObserver(end);
    for (const child of body.children) grew.observe(child);
    return () => grew.disconnect();
  });

  // The page underneath scrolls past the panel, so nothing is hidden behind it.
  $effect(() => {
    document.body.style.paddingBottom = `${height * 100}vh`;
    return () => (document.body.style.paddingBottom = '');
  });

  function drag(e: PointerEvent) {
    const handle = e.currentTarget as HTMLElement;
    handle.setPointerCapture(e.pointerId);
    const move = (m: PointerEvent) => {
      height = Math.min(MOST, Math.max(LEAST, 1 - m.clientY / innerHeight));
    };
    const up = () => {
      handle.removeEventListener('pointermove', move);
      localStorage.setItem(HEIGHT_KEY, String(height));
    };
    handle.addEventListener('pointermove', move);
    handle.addEventListener('pointerup', up, { once: true });
  }
</script>

<aside class="panel" style:height="{height * 100}vh" aria-label="Agent">
  <div
    class="handle"
    role="separator"
    aria-orientation="horizontal"
    aria-label="Resize the agent panel"
    onpointerdown={drag}
  ></div>
  <div class="bar">
    <select aria-label="Conversation" bind:value={open}>
      <option value="">New conversation</option>
      {#each conversations as c (c.id)}<option value={c.id}>{c.title}</option>{/each}
    </select>
    <span class="spacer"></span>
    <a href={open ? `#/me/agent/${open}` : '#/me/agent'} onclick={onclose}>Open on the Agent page</a
    >
    <button type="button" class="compact" aria-label="Close the agent panel" onclick={onclose}
      >&times;</button
    >
  </div>
  <div class="body" bind:this={body}>
    <Conversation
      {open}
      compact
      onstarted={(id) => {
        open = id;
        void refresh();
      }}
      onchanged={refresh}
    />
  </div>
</aside>

<style>
  .panel {
    position: fixed;
    left: 0;
    right: 0;
    bottom: 0;
    /* Under the menus its own chips open, which hang over it. */
    z-index: 15;
    display: grid;
    grid-template-rows: auto auto minmax(0, 1fr);
    grid-template-columns: minmax(0, 1fr);
    background: var(--bg);
    border-top: 1px solid var(--border);
    box-shadow: 0 -6px 18px rgb(0 0 0 / 0.12);
  }
  .handle {
    height: 5px;
    cursor: ns-resize;
    touch-action: none;
  }
  .handle:hover {
    background: var(--border);
  }
  .bar {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    padding: 0 1.25rem 0.2rem;
    font-size: var(--text-note);
    border-bottom: 1px solid var(--border);
  }
  .bar select {
    flex: 0 1 28rem;
    min-width: 0;
    padding-block: 0.1rem;
    font-size: var(--text-note);
  }
  .bar a {
    font-size: var(--text-note);
    white-space: nowrap;
  }
  @media (max-width: 48rem) {
    .bar {
      padding-inline: 1rem;
    }
    .body {
      padding-inline: 1rem;
    }
  }
  .spacer {
    flex: 1 1 auto;
  }
  /* The prompt sits at the bottom edge, as a terminal's does, and what was said grows up from it. */
  .body {
    display: flex;
    flex-direction: column;
    overflow: auto;
    padding: 0.4rem 1.25rem 0.5rem;
  }
  .body > :global(*) {
    margin-top: auto;
  }
</style>
