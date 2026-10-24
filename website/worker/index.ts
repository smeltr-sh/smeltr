// The site's Worker. Every page is an asset; this runs only for /api/* (`run_worker_first` in wrangler.jsonc).
//
// /api/releases is the list a leader reads to say a newer release is out (`src/leader/updates.rs`). A leader names
// its release in its User-Agent, `smeltr/1.2.0`, and nothing else, so a count by release is all this keeps.

interface Env {
  RELEASES: AnalyticsEngineDataset;
  /** Optional. GitHub allows an address 60 anonymous requests an hour, and Cloudflare shares its addresses. */
  GITHUB_TOKEN?: string;
}

const GITHUB = 'https://api.github.com/repos/smeltr-sh/smeltr/releases?per_page=100';

/** How long a data center keeps GitHub's answer before asking again. */
const FRESH_SECONDS = 60 * 60;

/** How long the last good answer stands in for GitHub when GitHub fails. */
const STALE_SECONDS = 7 * 24 * 60 * 60;

/** Each data center's cache keys, on a host nothing serves so they cannot collide with a page. */
const FRESH = new Request('https://releases.cache/fresh');
const STALE = new Request('https://releases.cache/stale');

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname !== '/api/releases' || request.method !== 'GET') {
      return new Response(null, { status: 404 });
    }
    const release = /^smeltr\/(\S{1,64})$/.exec(request.headers.get('user-agent') ?? '')?.[1];
    if (release) env.RELEASES.writeDataPoint({ blobs: [release], indexes: [release] });
    return releases(env, ctx);
  },
};

/** GitHub's list, cut to what a leader reads: each release's tag, page and whether it is a draft or pre-release. */
async function releases(env: Env, ctx: ExecutionContext): Promise<Response> {
  const cache = caches.default;
  const fresh = await cache.match(FRESH);
  if (fresh) return fresh;
  const headers: Record<string, string> = {
    'user-agent': 'smeltr.sh',
    accept: 'application/vnd.github+json',
  };
  if (env.GITHUB_TOKEN) headers.authorization = `Bearer ${env.GITHUB_TOKEN}`;
  const answer = await fetch(GITHUB, { headers }).catch(() => null);
  if (!answer?.ok) {
    return (
      (await cache.match(STALE)) ??
      new Response(`GitHub answered ${answer?.status ?? 'nothing'}`, { status: 502 })
    );
  }
  const list = ((await answer.json()) as Published[]).map(
    ({ tag_name, html_url, draft, prerelease }) => ({ tag_name, html_url, draft, prerelease }),
  );
  const body = JSON.stringify(list);
  const keep = (seconds: number) =>
    new Response(body, {
      headers: { 'content-type': 'application/json', 'cache-control': `max-age=${seconds}` },
    });
  ctx.waitUntil(
    Promise.all([cache.put(FRESH, keep(FRESH_SECONDS)), cache.put(STALE, keep(STALE_SECONDS))]),
  );
  return keep(FRESH_SECONDS);
}

interface Published {
  tag_name: string;
  html_url: string;
  draft: boolean;
  prerelease: boolean;
}
