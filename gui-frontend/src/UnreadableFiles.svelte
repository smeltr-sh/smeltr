<!--
  The files of a bundle that do not load, each by its full path, with why, a way to edit its raw
  text and, where the bundle can stand without it, a way to delete it. Both are writes like any
  other edit, on the branch.

  `problems` are the load errors the page was given. Those a listed file accounts for are shown
  on it; the rest go to `rest`, or by default to the one line config pages have always shown.
  Nothing is asked of the leader while there are no problems.
-->
<script lang="ts">
  import { reported } from './errors.svelte';
  import type { Snippet } from 'svelte';
  import { currentScope, deleteConfigFile, unreadableFiles, type UnreadableFileDto } from './api';
  import { ask } from './Confirm.svelte';
  import { editable } from './access';
  import { aboutFile, fileHash } from './unreadable';

  let {
    problems,
    scope = currentScope(),
    rest,
    onchanged,
  }: {
    problems: string[];
    scope?: string;
    rest?: Snippet<[string[]]>;
    /** A file was deleted: what the page read is stale. */
    onchanged?: () => void;
  } = $props();

  let files = $state<UnreadableFileDto[]>([]);
  // The commit the list was read at, where the leader says; a delete is made against it.
  let readAt: string | undefined;
  let deleting = $state<string | null>(null);

  let left = $derived(problems.filter((m) => !aboutFile(m, files)));

  async function load(inScope: string) {
    try {
      const res = await unreadableFiles(inScope);
      files = res.files ?? [];
      readAt = res.commit;
    } catch {
      // The problems still show, on the line, as they would without this list.
      files = [];
    }
  }

  $effect(() => {
    if (problems.length > 0) void load(scope);
    else files = [];
  });

  async function remove(file: UnreadableFileDto) {
    const ok = await ask({
      title: 'delete file',
      body: `Delete ${file.path}. Like any other edit, this is saved on your branch and reaches the fleet when it is deployed.`,
      action: 'Delete',
      danger: true,
    });
    if (!ok) return;
    deleting = file.path;
    try {
      // Listed again first: a save elsewhere on the page moves the branch past what the list read,
      // and the delete is checked against the read that still names this file.
      await load(scope);
      const res = await deleteConfigFile(file.path, readAt);
      if (res.ok) {
        await load(scope);
        onchanged?.();
      }
    } catch (e) {
      reported(e);
    } finally {
      deleting = null;
    }
  }
</script>

{#if files.length > 0}
  <div class="unreadable" role="alert">
    <p class="lead">
      {files.length === 1 ? 'This file does' : 'These files do'} not load, so no editor can open
      {files.length === 1 ? 'it' : 'them'}. Edit the raw text here.
    </p>
    <ul>
      {#each files as f (f.path)}
        <li>
          <div class="file">
            <span class="mono path">{f.path}</span>
            <span class="why">{f.why}</span>
          </div>
          <div class="actions">
            <a class="btn compact" href={fileHash(f.path, location.hash)}>Edit</a>
            {#if f.deletable && editable()}
              <button
                type="button"
                class="compact danger"
                disabled={deleting !== null}
                onclick={() => remove(f)}>{deleting === f.path ? 'Deleting…' : 'Delete'}</button
              >
            {/if}
          </div>
        </li>
      {/each}
    </ul>
  </div>
{/if}

{#if left.length > 0}
  {#if rest}
    {@render rest(left)}
  {:else}
    <p class="warning">the config bundle has an unrelated problem: {left.join('; ')}</p>
  {/if}
{/if}

<style>
  .unreadable {
    border: 1px solid var(--warn);
    border-radius: var(--radius-card);
    padding: 0.6rem 0.8rem;
    margin: 0 0 1rem;
    font-size: var(--text-body);
  }

  .lead {
    margin: 0 0 0.5rem;
    color: var(--warn);
  }

  ul {
    list-style: none;
    margin: 0;
    padding: 0;
    display: grid;
    gap: 0.5rem;
  }

  li {
    display: flex;
    flex-wrap: wrap;
    align-items: flex-start;
    justify-content: space-between;
    gap: 0.4rem 1rem;
  }

  .file {
    display: flex;
    flex-direction: column;
    gap: 0.15rem;
    min-width: 0;
  }

  .mono {
    font-family: var(--mono);
  }

  .path {
    overflow-wrap: anywhere;
  }

  .why {
    color: var(--dim);
    font-size: var(--text-label);
    overflow-wrap: anywhere;
  }

  .actions {
    display: flex;
    gap: 0.4rem;
  }

  .warning {
    color: var(--warn);
    font-size: var(--text-body);
    border: 1px solid var(--warn);
    border-radius: var(--radius);
    padding: 0.5rem 0.7rem;
    margin: 0 0 1rem;
  }
</style>
