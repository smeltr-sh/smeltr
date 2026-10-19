// Every route is prerendered to static HTML at build time; nothing here
// runs on a server at request time. `adapter-static` enforces this -- a
// route that cannot be prerendered fails the build rather than silently
// needing a runtime Cloudflare Pages will not provide.
export const prerender = true;
