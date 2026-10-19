<script lang="ts">
  import { browser, dev } from '$app/env';
  import { page } from '$app/state';
  import DocsLayout from '#lib/components/DocsLayout.svelte';

  type SectionResult = { title: string; url: string; excerpt: string };
  type Result = {
    url: string;
    meta: { title: string };
    excerpt: string;
    sub_results: SectionResult[];
  };
  type Search = { results: { id: string; data: () => Promise<Result> }[] };
  type Pagefind = { search: (query: string) => Promise<Search> };

  let query = $derived(browser ? (page.url.searchParams.get('q') ?? '').trim() : '');
  let results = $state<Result[]>([]);
  let loading = $state(false);
  let failed = $state(false);
  let limit = $state(10);
  let retry = $state(0);

  $effect(() => {
    const term = query;
    // Read the retry counter so a failed request can be run again.
    retry;
    let canceled = false;
    results = [];
    limit = 10;
    failed = false;
    loading = !!term;
    if (term) {
      // Pagefind writes this module after prerendering. It must remain a runtime import.
      const bundle = '/pagefind/pagefind.js';
      void (async () => {
        try {
          const pagefind: Pagefind = await import(/* @vite-ignore */ bundle);
          const search = await pagefind.search(term);
          const matches = await Promise.all(search.results.map((result) => result.data()));
          if (!canceled) results = matches;
        } catch {
          if (!canceled) failed = true;
        } finally {
          if (!canceled) loading = false;
        }
      })();
    }
    return () => {
      canceled = true;
    };
  });
</script>

<svelte:head>
  <title>Search documentation — smeltr</title>
  <meta name="robots" content="noindex" />
</svelte:head>

<DocsLayout sections={[]} searchable={false}>
  <h1>Search documentation</h1>
  <p class="intro">Find setup instructions, troubleshooting help, and expression examples.</p>
  <noscript
    ><p>Search needs JavaScript. You can still <a href="/docs">browse all guides</a>.</p></noscript
  >
  <div role="status" aria-live="polite">
    {#if loading}
      <p>Searching for “{query}”…</p>
    {:else if failed}
      <p>Search couldn't load. Try again or <a href="/docs">browse all guides</a>.</p>
    {:else if query}
      <p>{results.length} {results.length === 1 ? 'page' : 'pages'} found for “{query}”.</p>
    {/if}
  </div>
  {#if failed}
    <button type="button" onclick={() => retry++}>Try again</button>
    {#if dev}<p class="hint">
        To try search locally, run <code>npm run build</code> and <code>npm run preview</code>.
      </p>{/if}
  {:else if !loading && query && results.length === 0}
    <p>
      Try fewer words, a setting name, or a topic such as <a href="/docs/search?q=password"
        >password</a
      >
      or <a href="/docs/search?q=worker">worker</a>.
    </p>
  {:else if !query}
    <p>
      Enter a topic in the search box, or try <a href="/docs/search?q=reverse%20proxy"
        >reverse proxy</a
      >, <a href="/docs/search?q=password">password</a>, or
      <a href="/docs/search?q=parseTime">parseTime</a>.
    </p>
  {/if}
  <ol class="results" aria-label="Search results" aria-busy={loading}>
    {#each results.slice(0, limit) as result (result.url)}
      {@const sections = result.sub_results.filter((section) => section.url.includes('#'))}
      <li>
        <h2><a href={result.url}>{result.meta.title}</a></h2>
        <!-- Pagefind escapes document text before adding its highlight markup. -->
        <p class="excerpt">{@html result.excerpt}</p>
        {#if sections.length}
          <ul class="sub-results" aria-label="Matching sections">
            {#each sections.slice(0, 3) as section (section.url)}
              <li><a href={section.url}>{section.title}</a></li>
            {/each}
          </ul>
          {#if sections.length > 3}
            <details>
              <summary>{sections.length - 3} more matching sections</summary>
              <ul class="sub-results">
                {#each sections.slice(3) as section (section.url)}
                  <li><a href={section.url}>{section.title}</a></li>
                {/each}
              </ul>
            </details>
          {/if}
        {/if}
      </li>
    {/each}
  </ol>
  {#if results.length > limit}<button type="button" onclick={() => (limit += 10)}
      >Show more results</button
    >{/if}
</DocsLayout>

<style>
  h1 {
    font-size: clamp(2rem, 4vw, 3rem);
    letter-spacing: -0.03em;
  }
  .intro,
  .excerpt {
    color: var(--ink);
  }
  .hint {
    color: var(--dim);
  }
  .results {
    list-style: none;
    padding: 0;
    max-width: 48rem;
  }
  .results > li {
    border-top: 1px solid var(--border);
    padding: 1.5rem 0;
  }
  h2 {
    font-size: 1.35rem;
  }
  .excerpt {
    overflow-wrap: anywhere;
  }
  .excerpt :global(mark) {
    background: var(--panel);
    color: var(--ink);
    font-weight: 600;
  }
  .sub-results {
    padding-left: 1.25rem;
    display: grid;
    gap: 0.5rem;
  }
  summary {
    cursor: pointer;
    padding-block: 0.65rem;
  }
  button {
    padding: 0.65rem 1rem;
    min-height: 44px;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    background: var(--panel);
    color: var(--ink);
    font: inherit;
    cursor: pointer;
  }
</style>
