<!--
  The person signed in talking with their own agent: their conversations, and the one open.
-->
<script lang="ts">
  import SectionShell from './SectionShell.svelte';
  import PageHeader from './PageHeader.svelte';
  import Conversation from './Conversation.svelte';
  import ConversationList from './ConversationList.svelte';

  let { conversation: open }: { conversation?: string } = $props();

  let list = $state<ConversationList>();
</script>

<SectionShell>
  <PageHeader title="Agent">
    {#snippet help()}
      Your agent works in smeltr for you, through the leader's API, within your agent permissions.
      What it reads to do that is sent to the model endpoint your administrator set.
    {/snippet}
  </PageHeader>

  <div class="layout">
    <ConversationList bind:this={list} {open} />
    <Conversation
      {open}
      onstarted={(id) => (location.hash = `#/me/agent/${id}`)}
      onchanged={() => list?.refresh()}
      ondeleted={() => {
        location.hash = '#/me/agent';
        list?.refresh();
      }}
    />
  </div>
</SectionShell>

<style>
  .layout {
    display: grid;
    grid-template-columns: minmax(12rem, 16rem) minmax(0, 1fr);
    gap: 1.25rem;
    align-items: start;
  }
  @media (max-width: 48rem) {
    .layout {
      grid-template-columns: minmax(0, 1fr);
    }
  }
</style>
