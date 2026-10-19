# gui-frontend

The smeltr GUI: Svelte 5 + TypeScript, built with Vite. The leader serves
the built `dist/` from inside the binary; `just build` at the repo root
regenerates it.

```
npm ci
npm run dev            # http://localhost:5173, /api proxied to the backend
npm run check          # svelte-check + tsc
npm test               # vitest
npm run build          # -> dist/
npm run format         # prettier; `just check` runs format:check
```

`npm run dev` proxies `/api` to `http://127.0.0.1:4000`, so start the backend with
`SMELTR_GUI_PORT=4000` -- its own default, 443, is privileged and a dev loop has no reason to need it. A leader
serving TLS needs `SMELTR_DEV_API=https://127.0.0.1:4000 npm run dev` on top of that.

## Layout

- `App.svelte`: nav, hash router, theme tokens.
- One page component per section: `SourcesPage`, `DestinationsPage`,
  `RoutesPage`, `LookupTablesPage`, `PipelinesListPage`, `PipelineEditor`,
  `ClusterPage`. Each config page is sugar over one JSON file in the
  bundle and has a raw-JSON view (`JsonEditPage`).
- `api.ts`: the REST client and the wire types, mirroring
  `src/config_file/dto.rs` and `src/bin/smeltr/config_api.rs`.
- `pipeline.ts`: the builder's step model and its `StepDto` codec.
- `workers.ts`, `diff.ts`, `autocomplete.ts`, `sparkline.ts`: pure logic
  behind a component, each with a test file beside it.

`STYLE_GUIDE.md` is the visual system.
