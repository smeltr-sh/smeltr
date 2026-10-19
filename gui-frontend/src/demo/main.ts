// The website demo: the GUI as `main.ts` mounts it, over the leader this page plays (`leader.ts`).
import recordingUrl from './recorded.json?url';
import { DemoLeader, install, type Recording } from './leader';

const recording = (await fetch(recordingUrl).then((r) => r.json())) as Recording;
let store: Storage | null = null;
try {
  store = sessionStorage;
} catch {
  // Blocked storage keeps the visitor's work until the page reloads.
}
install(new DemoLeader(recording, import.meta.env.VITE_SMELTR_PREVIEW_URL ?? '/preview', store));
await import('../main');
