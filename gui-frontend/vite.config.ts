import { svelte } from '@sveltejs/vite-plugin-svelte';
import { defineConfig } from 'vite';

// `npm run dev` proxies API calls to the axum backend (src/bin/smeltr) so
// the browser only ever talks to one origin -- no CORS setup needed on
// either side. The production build has no dev server: build.rs bakes
// dist/ into the binary, which serves it from memory (src/bin/smeltr/
// assets.rs), so a GUI change needs `npm run build` then `cargo build`.
//
// The target is configurable because the backend's scheme isn't fixed: a
// `--features leader` build serves this same port over HTTPS with a
// certificate its own authority signed (SMELTR_LEADER_TLS), while a plain
// `--features gui` build serves plain HTTP. Hardcoding `http://` silently
// broke every /api call against a leader build.
//
//   SMELTR_DEV_API=https://127.0.0.1:4000 npm run dev
//
// `secure: false` accepts a certificate signed by an authority this
// machine does not trust -- this is a local dev proxy talking to a
// loopback backend, never a production path (nothing here ships in the
// built dist/).
//
// The port here, 4000, is not the backend's own default (443, privileged) -- start it with
// SMELTR_GUI_PORT=4000 so this proxy's default target is right without SMELTR_DEV_API.
const target = process.env.SMELTR_DEV_API ?? 'http://127.0.0.1:4000';

export default defineConfig({
  plugins: [svelte()],
  resolve: { conditions: process.env.VITEST ? ['browser'] : undefined },
  server: {
    // Read from the website: its code tokenizer, and the palette and controls both share.
    fs: {
      allow: [
        '.',
        '../website/src/lib/highlight',
        '../website/src/lib/foundry',
        '../website/src/lib/reference.json',
        '../website/src/lib/code-language.json',
      ],
    },
    proxy: {
      '/api': {
        target,
        changeOrigin: true,
        secure: false,
      },
    },
  },
});
