<!--
  One file's raw text, by its full path in the repository. The way into a file that does not
  load: no section editor can open it, since none of them can read it. A save is a write like
  any other, on the branch and against the commit it was read at.
-->
<script lang="ts">
  import { reported } from './errors.svelte';
  import { getConfigFile, putConfigFile } from './api';
  import JsonEditPage from './JsonEditPage.svelte';
  import SectionShell from './SectionShell.svelte';
  import { canEdit } from './access';
  import { scopeOf } from './objectChanges';

  let { path, back }: { path: string; back: string } = $props();

  let text = $state<string | null>(null);
  let loadFailed = $state(false);
  // The commit the text was read at, where the leader says; otherwise the save goes against the
  // base the rest of the editor holds.
  let readAt: string | undefined;

  async function load(of: string) {
    text = null;
    loadFailed = false;
    try {
      const res = await getConfigFile(of);
      if (res.ok) {
        text = res.text;
        readAt = res.commit;
      } else {
        loadFailed = true;
      }
    } catch (e) {
      loadFailed = true;
      reported(e);
    }
  }

  $effect(() => {
    void load(path);
  });

  async function save(draft: string) {
    const res = await putConfigFile(path, draft, readAt);
    // The leader's refusal is the error bar's to say.
    if (!res.ok) return { ok: false, errors: '' };
    readAt = res.commit;
    return { ok: true, errors: '' };
  }
</script>

{#if loadFailed}
  <SectionShell />
{:else if text === null}
  <SectionShell><p class="dim">Loading…</p></SectionShell>
{:else}
  <!-- A file belongs to the area its path is under, whichever scope the chip is on. -->
  <JsonEditPage
    title={path}
    initialText={text}
    backHash={back}
    onSave={save}
    readOnly={!canEdit(scopeOf(path))}
  />
{/if}

<style>
  .dim {
    color: var(--dim);
  }
</style>
