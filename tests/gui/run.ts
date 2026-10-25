// One directory per `playwright test` run, holding every test's data directories and a file per process it
// started. A fixture stops what its test started; this is for what a killed worker or an interrupted run left
// behind: the run's own teardown stops it, and the next run's setup stops whatever an earlier run could not.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

/** Where every run directory sits. */
const ROOT = path.join(os.tmpdir(), 'smeltr-gui-runs');

/** The environment variable that hands the run directory to Playwright's workers. Not `SMELTR_*`: a
 * spawned smeltr process is given none of the caller's. */
export const RUN_DIR = 'GUI_TESTS_RUN_DIR';

/** Records `pid` as started by this run, under the binary it runs. */
export function recordPid(runDir: string, pid: number, binary: string) {
  fs.mkdirSync(path.join(runDir, 'pids'), { recursive: true });
  // Resolved, as `/proc/<pid>/exe` is.
  fs.writeFileSync(path.join(runDir, 'pids', String(pid)), fs.realpathSync(binary));
}

function alive(pid: number) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

/** Kills each process `runDir` recorded that still runs the binary it was started from, then removes the
 * directory. A pid the OS has since given to some other program is left alone. */
function sweep(runDir: string) {
  const pids = path.join(runDir, 'pids');
  for (const name of fs.existsSync(pids) ? fs.readdirSync(pids) : []) {
    const pid = Number(name);
    const binary = fs.readFileSync(path.join(pids, name), 'utf8');
    let exe = '';
    try {
      exe = fs.readlinkSync(`/proc/${pid}/exe`);
    } catch {
      continue;
    }
    // A rebuild replaces the binary, and the kernel then names a process still running the old one `… (deleted)`.
    if (exe.replace(/ \(deleted\)$/, '') !== binary || !alive(pid)) continue;
    try {
      // A worker a test froze to make it go silent takes no signal but SIGKILL until it is woken.
      process.kill(pid, 'SIGCONT');
      process.kill(pid, 'SIGKILL');
    } catch {
      // It exited since `alive` looked.
    }
  }
  fs.rmSync(runDir, { recursive: true, force: true });
}

/** Records what one seed of `fuzzer` reached, for its coverage spec to judge once every seed has run. */
export function recordReached(fuzzer: string, seed: number, reached: Iterable<string>) {
  const dir = path.join(process.env[RUN_DIR] ?? '', fuzzer);
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, `${seed}.json`), JSON.stringify([...reached]));
}

/** What each seed of `fuzzer` that ran recorded with `recordReached`. */
export function reached(fuzzer: string): string[][] {
  const dir = path.join(process.env[RUN_DIR] ?? '', fuzzer);
  return (fs.existsSync(dir) ? fs.readdirSync(dir) : []).map(
    (f) => JSON.parse(fs.readFileSync(path.join(dir, f), 'utf8')) as string[],
  );
}

export async function globalSetup() {
  fs.mkdirSync(ROOT, { recursive: true });
  // A run whose Playwright process is gone left these; one still running is another run's.
  for (const name of fs.readdirSync(ROOT)) {
    const owner = Number(name.split('-')[0]);
    if (!alive(owner)) sweep(path.join(ROOT, name));
  }
  const runDir = fs.mkdtempSync(path.join(ROOT, `${process.pid}-`));
  process.env[RUN_DIR] = runDir;
  return async () => sweep(runDir);
}

export default globalSetup;
