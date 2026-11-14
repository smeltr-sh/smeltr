// The website demo: the GUI as `main.ts` mounts it, over the leader this page plays (`leader.ts`).
import recordingUrl from './recorded.json?url';
import { DemoLeader, WAKING_EVENT, install, waking, type Recording } from './leader';
import { partFile } from './parts';

/** Each part's URL, by its file's name (`parts.js`). */
const PARTS: Record<string, string> = import.meta.glob('./recorded/*.json', {
  query: '?url',
  import: 'default',
  eager: true,
});

// The build ships the recording gzipped (`vite.demo.config.ts`). A host may still undo that on the way, so the bytes
// say which they are.
async function unpacked(url: string): Promise<unknown> {
  const bytes = new Uint8Array(await (await fetch(url)).arrayBuffer());
  const gzipped = bytes[0] === 0x1f && bytes[1] === 0x8b;
  return new Response(
    gzipped ? new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip')) : bytes,
  ).json();
}

const recording = (await unpacked(recordingUrl)) as Recording;
const BUILD = 'smeltr-demo-build';
let store: Storage | null = null;
try {
  store = sessionStorage;
  // The entry's URL is hashed per build. A tab's work from an older build was made against another recording and GUI.
  if (store.getItem(BUILD) !== import.meta.url) {
    store.clear();
    store.setItem(BUILD, import.meta.url);
  }
} catch {
  // Blocked storage keeps the visitor's work until the page reloads.
}
const leader = new DemoLeader(
  recording,
  import.meta.env.VITE_SMELTR_PREVIEW_URL ?? '/preview',
  store,
  Date.now,
  async (key) => {
    const url = PARTS[`./recorded/${await partFile(key)}`];
    return url === undefined ? undefined : unpacked(url);
  },
);
install(leader);
leader.wake((now) => {
  waking.now = now;
  window.dispatchEvent(new Event(WAKING_EVENT));
});
// The GUI remembers its branch across tabs and builds, and this tab may not hold it: leave it, as for a deleted one.
const { currentBranch, leaveBranch } = await import('../api');
if (!leader.holds(currentBranch())) leaveBranch(currentBranch());
await import('../main');
