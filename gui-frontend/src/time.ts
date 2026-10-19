// Times from the leader arrive as unix seconds. Everything that shows one
// shows how long ago it was, because "40m ago" is the question being asked
// and a timestamp is not.

/** How long ago `unixSeconds` was, at the coarsest unit that still says
 * something: seconds under a minute, then minutes, hours and days. */
export function ago(unixSeconds: number, now: number = Date.now()): string {
  const s = Math.max(0, Math.floor(now / 1000 - unixSeconds));
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  return `${Math.floor(h / 24)}d ago`;
}

/** When `unixMillis` runs out, as a whole phrase, at the coarsest unit that
 * still says something. For a deadline somebody is deciding whether to act
 * on: a certificate to replace, an enrollment code to spend before it lapses.
 * Says "in 12 days" rather than a date, because the date is not the question. */
export function expiry(unixMillis: number, now: number = Date.now()): string {
  const left = unixMillis - now;
  if (left <= 0) return 'expired';
  const minutes = Math.floor(left / 60_000);
  if (minutes < 60) return `expires in ${count(minutes, 'minute')}`;
  const hours = Math.floor(minutes / 60);
  if (hours < 48) return `expires in ${count(hours, 'hour')}`;
  const days = Math.floor(hours / 24);
  if (days < 90) return `expires in ${count(days, 'day')}`;
  const months = Math.round(days / 30);
  if (months < 24) return `expires in ${count(months, 'month')}`;
  return `expires in ${count(Math.round(days / 365), 'year')}`;
}

/** Wall-clock `HH:MM`, local time -- the axis under a chart spanning real time, where "how
 * long ago" is the wrong question because both ends are on screen at once. */
export function clockTime(unixMillis: number): string {
  const d = new Date(unixMillis);
  return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
}

/** A month, day and local `HH:MM`, such as `Oct 17, 22:40` -- for a list sorted by when its rows were made,
 * where the rows are read against each other rather than against now. */
export function dateTime(unixSeconds: number): string {
  const d = new Date(unixSeconds * 1000);
  const day = d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  return `${day}, ${clockTime(unixSeconds * 1000)}`;
}

/** `n` of something, pluralized by adding an s. */
export function count(n: number, what: string): string {
  return `${n} ${what}${n === 1 ? '' : 's'}`;
}

/** A commit, short enough to read and long enough to name one. */
export function shortCommit(commit: string): string {
  return commit.slice(0, 7);
}

/** A bundle path as the object it holds: `groups/west/sources.json` is
 * `sources`. The prefix says which group, which the page showing it already
 * said. */
export function shortPath(path: string): string {
  return path
    .replace(/^groups\/[^/]+\//, '')
    .replace(/^shared\//, '')
    .replace(/\.json$/, '');
}

/** Whether `path` holds one pipeline, in a group or in `shared/`. Its stem is the pipeline's id, a key no
 * reader chose, so it is never shown as a name. */
export function isPipelinePath(path: string): boolean {
  return /^(?:groups\/[^/]+\/|shared\/)?pipelines\/[^/]+\.json$/.test(path);
}

/** The first `limit` of `paths`, named as objects, with a count of the rest. `label` names a path when it
 * can -- a pipeline file by its pipeline's name, on a page that holds the names -- and a path it does not
 * name is its `shortPath`, except that every unnamed pipeline file is one entry, `pipeline` or `2 pipelines`,
 * where the first of them stood. */
export function pathSummary(
  paths: string[],
  label: (path: string) => string | undefined = () => undefined,
  limit = 3,
): string {
  if (paths.length === 0) return '—';
  const entries: { text: string; files: number }[] = [];
  let pipelines: { text: string; files: number } | null = null;
  for (const path of paths) {
    const named = label(path);
    if (named !== undefined) {
      entries.push({ text: named, files: 1 });
    } else if (isPipelinePath(path)) {
      if (!pipelines) entries.push((pipelines = { text: '', files: 0 }));
      pipelines.files += 1;
      pipelines.text = pipelines.files === 1 ? 'pipeline' : count(pipelines.files, 'pipeline');
    } else {
      entries.push({ text: shortPath(path), files: 1 });
    }
  }
  const shown = entries.slice(0, limit);
  const rest = entries.slice(limit).reduce((n, e) => n + e.files, 0);
  const text = shown.map((e) => e.text).join(' · ');
  return rest > 0 ? `${text} · ${rest} more` : text;
}
