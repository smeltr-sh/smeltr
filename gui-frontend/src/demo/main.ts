// The website demo: the GUI as `main.ts` mounts it, over the leader this page plays (`leader.ts`).
import recordingUrl from './recorded.json?url';
import { DemoLeader, WAKING_EVENT, install, waking, type Recording } from './leader';

const recording = (await fetch(recordingUrl).then((r) => r.json())) as Recording;
let store: Storage | null = null;
try {
  store = sessionStorage;
} catch {
  // Blocked storage keeps the visitor's work until the page reloads.
}
const leader = new DemoLeader(
  recording,
  import.meta.env.VITE_SMELTR_PREVIEW_URL ?? '/preview',
  store,
);
install(leader);
leader.wake((now) => {
  waking.now = now;
  window.dispatchEvent(new Event(WAKING_EVENT));
});
await import('../main');
