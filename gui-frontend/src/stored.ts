// What the GUI remembers in the browser. Each of these is a reader's convenience,
// so a browser with no storage, or one that refuses it, only forgets on a reload.

type Area = 'local' | 'session';

function area(where: Area): Storage | null {
  const g = globalThis as { localStorage?: Storage; sessionStorage?: Storage };
  return (where === 'local' ? g.localStorage : g.sessionStorage) ?? null;
}

/** What this browser keeps under `key`, or null when it keeps nothing there. */
export function stored(key: string, where: Area = 'local'): string | null {
  try {
    return area(where)?.getItem(key) ?? null;
  } catch {
    return null;
  }
}

/** What this browser keeps under `key` as JSON, or `fallback` when nothing usable is there. */
export function storedJson<T>(key: string, fallback: T, where: Area = 'local'): T {
  const saved = stored(key, where);
  if (saved === null) return fallback;
  try {
    return JSON.parse(saved) as T;
  } catch {
    return fallback;
  }
}

/** Keeps `value` under `key`, or forgets what is there when it is null. */
export function store(key: string, value: string | null, where: Area = 'local'): void {
  try {
    if (value === null) area(where)?.removeItem(key);
    else area(where)?.setItem(key, value);
  } catch {
    // Refused or full: the value lasts as long as the page.
  }
}
