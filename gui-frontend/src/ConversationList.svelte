<!--
  The person's conversations with their agent, the one added to last first, each a link to it on the Agent page.
-->
<script lang="ts">
  import { ago } from './time';
  import { reported } from './errors.svelte';
  import { firstLineText } from './markdown';
  import { listConversations, refused, type ConversationSummaryDto } from './api';

  let { open }: { open?: string } = $props();

  let conversations = $state<ConversationSummaryDto[]>([]);

  export async function refresh() {
    try {
      const listed = await listConversations();
      if (refused(listed)) return;
      conversations = listed.conversations;
    } catch (e) {
      reported(e);
    }
  }

  $effect(() => {
    void refresh();
  });
</script>

<nav class="list" aria-label="Conversations">
  <a class="new" class:here={!open} href="#/me/agent">New conversation</a>
  {#each conversations as c (c.id)}
    <a class="item" class:here={c.id === open} href="#/me/agent/{c.id}">
      <span class="title">{firstLineText(c.title)}</span>
      <span class="dim">{c.running ? 'working…' : ago(c.updatedAt / 1000)}</span>
    </a>
  {:else}
    <p class="empty">No conversations yet.</p>
  {/each}
</nav>

<style>
  .list {
    display: grid;
    gap: 0.15rem;
  }
  .list a {
    display: grid;
    gap: 0.1rem;
    padding: 0.4rem 0.6rem;
    border-radius: var(--radius-card);
    color: var(--ink);
    text-decoration: none;
  }
  .list a:hover {
    background: var(--panel);
  }
  .list a.here {
    background: var(--panel);
    font-weight: 600;
  }
  .list .new {
    margin-bottom: 0.4rem;
  }
  .title {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .dim {
    color: var(--dim);
    font-size: var(--text-note);
    font-weight: 400;
  }
  .empty {
    color: var(--dim);
  }
</style>
