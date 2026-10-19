<!--
  A person in a conversation, by the initials of their name.
-->
<script lang="ts">
  import { initials } from './conversation';

  let { name }: { name: string } = $props();
</script>

<span class="avatar" aria-hidden="true">{initials(name)}</span>

<style>
  .avatar {
    flex-shrink: 0;
    width: 1.6rem;
    height: 1.6rem;
    border-radius: 50%;
    display: inline-grid;
    place-items: center;
    border: 1px solid var(--border);
    background: var(--panel);
    color: var(--ink);
    font-size: var(--text-micro);
    font-weight: 600;
  }
</style>
