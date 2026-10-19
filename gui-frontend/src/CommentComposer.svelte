<!--
  A box to start a thread on what `anchor` names: the text, Cancel, and
  Comment, which stays off until there is something to send.
-->
<script lang="ts">
  import { unreported } from './errors.svelte';
  import InlineError from './InlineError.svelte';
  import Locked from './Locked.svelte';
  import { commentOnChange, errorText, type CommentAnchor } from './api';

  let {
    branch,
    anchor,
    oncancel,
    onposted,
  }: {
    branch: string;
    anchor: CommentAnchor;
    oncancel: () => void;
    /** The comment landed: the timeline has it. */
    onposted: () => Promise<void> | void;
  } = $props();

  let body = $state('');
  let busy = $state(false);
  let postError = $state('');

  async function submit(e: SubmitEvent) {
    e.preventDefault();
    if (busy || !body.trim()) return;
    busy = true;
    postError = '';
    try {
      const res = await commentOnChange(branch, body, { anchor });
      if (res.ok) {
        body = '';
        await onposted();
      } else {
        postError = errorText(res.errors);
      }
    } catch (err) {
      postError = unreported(err);
    } finally {
      busy = false;
    }
  }

  function keydown(e: KeyboardEvent) {
    if (e.key === 'Escape') oncancel();
  }
</script>

<form class="composer" onsubmit={submit}>
  <!-- svelte-ignore a11y_autofocus -->
  <textarea
    rows="3"
    aria-label="Comment"
    placeholder="Comment"
    autofocus
    bind:value={body}
    onkeydown={keydown}></textarea>
  <InlineError message={postError} />
  <div class="actions">
    <button type="button" class="compact" disabled={busy} onclick={oncancel}>Cancel</button>
    <Locked refusedInDemo>
      <button type="submit" class="compact primary" disabled={busy || !body.trim()}>
        {busy ? 'Sending…' : 'Comment'}
      </button>
    </Locked>
  </div>
</form>

<style>
  .composer {
    display: grid;
    gap: 0.4rem;
  }
  textarea {
    width: 100%;
    box-sizing: border-box;
    resize: vertical;
  }
  .actions {
    display: flex;
    justify-content: flex-end;
    gap: 0.5rem;
  }
</style>
