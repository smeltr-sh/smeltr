import { mount } from 'svelte';
// Bundled rather than fetched: a leader inside an enclave has no route to a font CDN, and
// a stylesheet that never resolves blocks this script from running at all. Every face is
// SIL Open Font License 1.1; the notices are in THIRD-PARTY-NOTICES.md. Only the weights
// STYLE_GUIDE.md's "Type" names, so none is synthesized.
import '@fontsource/geist/600.css';
import '@fontsource/ibm-plex-sans/400.css';
import '@fontsource/ibm-plex-sans/400-italic.css';
import '@fontsource/ibm-plex-sans/500.css';
import '@fontsource/ibm-plex-sans/600.css';
import '@fontsource/ibm-plex-sans/700.css';
import '@fontsource/ibm-plex-mono/400.css';
import '@fontsource/ibm-plex-mono/500.css';
import '../../website/src/lib/foundry/palette.css';
import '../../website/src/lib/foundry/controls.css';
import './app.css';
import App from './App.svelte';

const app = mount(App, {
  target: document.getElementById('app')!,
});

export default app;
