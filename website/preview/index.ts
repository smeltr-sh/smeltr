// The demo's preview service at smeltr.sh/preview: `smeltr-demo` in a container, behind a per-visitor rate limit.
// The page at /demo/try, which this Worker serves from its assets, asks it what only the engine can answer, on the
// site's own origin, so it needs no cross-origin headers. Each request carries all it is about, so any instance can answer any visitor.
import { Container, getRandom } from '@cloudflare/containers';

interface Env {
  PREVIEW: DurableObjectNamespace<Preview>;
  LIMIT: RateLimit;
}

/** Must not exceed `max_instances` in wrangler.jsonc. */
const INSTANCES = 2;

/** What a visitor past the rate limit reads, in place of the answer it asked for. */
const SLOW_DOWN = 'The demo is busy. Wait a minute and try again.';

export class Preview extends Container<Env> {
  defaultPort = 8080;
  sleepAfter = '5m';
  // It reads nothing but the request, so nothing it runs may reach anything else.
  enableInternet = false;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    if (!url.pathname.startsWith('/preview/') || request.method !== 'POST') {
      return new Response(null, { status: 404 });
    }
    const visitor = request.headers.get('cf-connecting-ip') ?? 'unknown';
    if (!(await env.LIMIT.limit({ key: visitor })).success) {
      return new Response(SLOW_DOWN, { status: 429 });
    }
    url.pathname = url.pathname.slice('/preview'.length);
    const container = await getRandom(env.PREVIEW, INSTANCES);
    return container.fetch(new Request(url, request));
  },
};
