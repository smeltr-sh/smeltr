import { gzipSync } from 'node:zlib';
import { defineConfig, mergeConfig, type Plugin } from 'vite';
import base from './vite.config.ts';

// The website demo (`demo.html`): the GUI over the leader its page plays, built into `dist-demo/` for the website to
// serve. VITE_SMELTR_PREVIEW_URL names the preview service it asks what only the engine can answer (`src/demo`).
// Without it the page asks `/preview` on its own origin, which `vite --config vite.demo.config.ts` and `vite preview`
// pass to a `smeltr-demo` at SMELTR_DEMO_SERVICE.
const service = {
  '/preview': {
    target: process.env.SMELTR_DEMO_SERVICE ?? 'http://127.0.0.1:8790',
    changeOrigin: true,
    rewrite: (path: string) => path.replace(/^\/preview/, ''),
  },
};

// The recording outgrows the 25 MiB a Cloudflare Workers asset may be, and gzips to a fraction of it. `main.ts`
// unpacks it.
const RECORDING = /^assets\/recorded-[^/]+\.json$/;
const gzipRecording: Plugin = {
  name: 'gzip-recording',
  apply: 'build',
  generateBundle(_, bundle) {
    for (const [name, asset] of Object.entries(bundle)) {
      if (asset.type === 'asset' && RECORDING.test(name)) {
        delete bundle[name];
        this.emitFile({
          type: 'asset',
          fileName: `${name}.gz`,
          source: gzipSync(asset.source, { level: 9 }),
        });
      }
    }
  },
};

export default mergeConfig(
  base,
  defineConfig({
    // The shell draws the demo's strip in this mode alone (`DemoBar.svelte`).
    mode: 'demo',
    plugins: [gzipRecording],
    experimental: {
      // Beside the entry chunk, in `assets/`, wherever the site serves the demo.
      renderBuiltUrl: (file) =>
        RECORDING.test(file)
          ? {
              runtime: `new URL(${JSON.stringify(`${file.slice('assets/'.length)}.gz`)}, import.meta.url).href`,
            }
          : { relative: true },
    },
    build: {
      outDir: 'dist-demo',
      rollupOptions: { input: 'demo.html' },
    },
    server: { proxy: service },
    preview: { proxy: service },
  }),
);
