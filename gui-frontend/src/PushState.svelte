<!--
  One branch against the host's branch of the same name, compact enough for a
  table cell. Renders nothing while no remote is set, which is when a branch
  carries no `push`.
-->
<script module lang="ts">
  import type { BranchDto as Branch } from './api';

  /** The word this shows first, for a column that sorts by it. */
  export function pushWord(b: Branch): string | undefined {
    if (!b.push) return undefined;
    if (b.push.state === 'pushed') return 'on the remote';
    if (b.push.state === 'waiting') return b.push.message === '' ? 'pushing' : 'not pushed';
    return 'refused';
  }
</script>

<script lang="ts">
  import { unreported } from './errors.svelte';
  import InlineError from './InlineError.svelte';
  import { errorText, overwriteBranch, type BranchDto } from './api';
  import { shortCommit } from './time';
  import { ask } from './Confirm.svelte';
  import Help from './Help.svelte';

  let { branch, onoverwritten }: { branch: BranchDto; onoverwritten: () => void } = $props();

  let busy = $state(false);
  let error = $state('');

  async function overwrite() {
    const push = branch.push;
    if (!push || push.state !== 'refused' || busy) return;
    const host = push.hostTip ? `at ${shortCommit(push.hostTip)}` : 'gone';
    const ok = await ask({
      title: 'overwrite remote',
      body:
        `Push ${branch.name} at ${shortCommit(branch.head)} over the host's branch, now ${host}. ` +
        `Commits someone else pushed there are dropped from that branch.`,
      action: 'Overwrite remote',
      danger: true,
    });
    if (!ok || busy) return;
    busy = true;
    error = '';
    try {
      const res = await overwriteBranch(branch.name, branch.head, push.hostTip ?? null);
      if (res.ok) onoverwritten();
      else error = errorText(res.errors) || 'the host refused the push';
    } catch (e) {
      error = unreported(e);
    } finally {
      busy = false;
    }
  }
</script>

{#if branch.push}
  {@const push = branch.push}
  <span class="push">
    {#if push.state === 'pushed'}
      <span class="dim">on the remote</span>
    {:else if push.state === 'waiting'}
      {#if push.message === ''}
        <span class="dim">pushing&hellip;</span>
      {:else}
        <span class="warn">not pushed<Help label="not pushed">{push.message}</Help></span>
      {/if}
    {:else}
      <span class="refused"
        >refused<Help label="refused"
          >Someone else pushed to this branch on the host, so pushing ours would drop their commits.
          Overwriting the remote puts ours there anyway.{#if push.message}
            {push.message}{/if}</Help
        ></span
      >
      <span class="mono dim">ours {shortCommit(branch.head)}</span>
      <span class="mono dim">host {push.hostTip ? shortCommit(push.hostTip) : '—'}</span>
      <button type="button" class="compact" disabled={busy} onclick={overwrite}>
        {busy ? 'Overwriting…' : 'Overwrite remote'}
      </button>
    {/if}
    <InlineError message={error} inline />
  </span>
{/if}

<style>
  .push {
    display: inline-flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.2rem 0.5rem;
  }
  .dim {
    color: var(--dim);
  }
  .warn {
    color: var(--warn);
  }
  .refused {
    color: var(--error);
  }
  .mono {
    font-family: var(--mono);
  }
  button {
    white-space: nowrap;
  }
</style>
