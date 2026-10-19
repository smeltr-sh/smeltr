<script lang="ts">
  import { browser } from '$app/env';
  import { page } from '$app/state';

  let query = $derived(browser ? (page.url.searchParams.get('q') ?? '') : '');
</script>

<form role="search" aria-label="Documentation search" action="/docs/search" method="get">
  <label for="docs-search">Search documentation</label>
  <div class="search-row">
    <input id="docs-search" type="search" name="q" value={query} placeholder="Find a topic…" />
    <button type="submit" aria-label="Search documentation">→</button>
  </div>
</form>

<style>
  form {
    margin-bottom: 1.25rem;
  }
  label {
    display: block;
    margin-bottom: 0.5rem;
    font-size: 0.85rem;
    font-weight: 600;
  }
  .search-row {
    display: flex;
    gap: 0.35rem;
  }
  input {
    width: 100%;
    min-width: 0;
    min-height: 44px;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 0.6rem;
    background: var(--bg);
    color: var(--ink);
    font: inherit;
    font-size: 1rem;
  }
  button {
    flex-shrink: 0;
    min-width: 44px;
    min-height: 44px;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    background: var(--panel);
    color: var(--ink);
    font: inherit;
    cursor: pointer;
  }
</style>
