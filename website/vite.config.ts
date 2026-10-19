import adapter from '@sveltejs/adapter-static';
import { sveltekit } from '@sveltejs/kit/vite';
import { vitePreprocess } from '@sveltejs/vite-plugin-svelte';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [
    sveltekit({
      preprocess: vitePreprocess(),
      // Every route is prerendered (see `src/routes/+layout.ts`), so the
      // build is a directory of HTML that Cloudflare Pages serves directly:
      // no server runtime, no Worker, no adapter-specific config.
      adapter: adapter({
        pages: 'build',
        assets: 'build',
        // Cloudflare Pages serves `404.html` for unmatched paths.
        fallback: '404.html',
        strict: true,
      }),
    }),
  ],
});
