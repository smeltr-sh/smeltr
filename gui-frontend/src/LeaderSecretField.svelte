<!--
  A secret only the leader holds -- the OIDC client secret, the git host's token, the model endpoint's API key --
  as a setting shows it: never read back, only whether one is stored. Stored, it offers Replace and no input at all,
  so neither a stray keystroke nor a password manager can put something over it. Replace opens an empty input, and
  Cancel takes it back.

  `replacing` is the caller's to reset once a save has landed, which is when the input has done its work.
-->
<script lang="ts">
  import LockIcon from './LockIcon.svelte';
  import PasswordField from './PasswordField.svelte';

  let {
    value = $bindable(''),
    replacing = $bindable(false),
    stored,
    ariaLabel,
    required = false,
  }: {
    value: string;
    replacing?: boolean;
    /** Whether the leader holds one that a save would keep. */
    stored: boolean;
    ariaLabel: string;
    /** As `PasswordField`'s. */
    required?: boolean;
  } = $props();
</script>

{#if stored && !replacing}
  <div class="row">
    <span class="held"><LockIcon /> Stored</span>
    <button type="button" aria-label="replace the {ariaLabel}" onclick={() => (replacing = true)}
      >Replace</button
    >
  </div>
{:else}
  <div class="row">
    <span class="input"><PasswordField bind:value {ariaLabel} {required} /></span>
    {#if stored}
      <button
        type="button"
        onclick={() => {
          value = '';
          replacing = false;
        }}>Cancel</button
      >
    {/if}
  </div>
{/if}

<style>
  .row {
    display: flex;
    gap: 0.5rem;
    align-items: center;
  }

  .input {
    flex: 1;
    min-width: 0;
  }

  /* Sized and framed as the input it stands in for, so the form does not shift when Replace opens one. */
  .held {
    flex: 1;
    display: inline-flex;
    align-items: center;
    gap: 0.4rem;
    box-sizing: border-box;
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 0.35rem 0.6rem;
  }
</style>
