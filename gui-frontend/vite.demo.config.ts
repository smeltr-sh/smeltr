import { defineConfig, mergeConfig } from 'vite';
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

export default mergeConfig(
  base,
  defineConfig({
    // The shell draws the demo's strip in this mode alone (`DemoBar.svelte`).
    mode: 'demo',
    build: {
      outDir: 'dist-demo',
      rollupOptions: { input: 'demo.html' },
    },
    server: { proxy: service },
    preview: { proxy: service },
  }),
);
