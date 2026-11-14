/** The kinds of page the index holds. A page names its kind with `data-pagefind-filter="kind:…"`. */
export type Kind = 'docs' | 'video';

export type SectionResult = { title: string; url: string; excerpt: string };
export type Result = {
  url: string;
  meta: { title: string };
  /** Text around the match. Pagefind escapes the page's text before it adds its `<mark>`s. */
  excerpt: string;
  sub_results: SectionResult[];
};
type Pagefind = {
  search: (
    query: string,
    options: { filters: { kind: Kind } },
  ) => Promise<{ results: { data: () => Promise<Result> }[] }>;
};

/** The pages of one kind that match `query`, best match first. Throws when the index can't load, which is always
 * under `vite dev`: Pagefind writes its index after the build. */
export async function search(query: string, kind: Kind): Promise<Result[]> {
  // Pagefind writes this module after prerendering. It must remain a runtime import.
  const bundle = '/pagefind/pagefind.js';
  const pagefind: Pagefind = await import(/* @vite-ignore */ bundle);
  const found = await pagefind.search(query, { filters: { kind } });
  return Promise.all(found.results.map((result) => result.data()));
}
