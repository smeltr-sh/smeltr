import notes from './releases.json';
import { source } from './release.ts';
import type { Group, Link } from './nav.ts';

/** A release's notes, as `releases.json` holds them. `date` is missing until the release ships. */
export type Release = {
  version: string;
  date?: string | null;
  summary: string;
  highlights?: { title: string; text: string; href: string }[];
  sections: { title: string; items: string[] }[];
};

function parts(version: string): number[] {
  return version.split('.').map(Number);
}

function newerFirst(a: Release, b: Release): number {
  const [x, y] = [parts(a.version), parts(b.version)];
  for (let i = 0; i < Math.max(x.length, y.length); i++) {
    if ((x[i] ?? 0) !== (y[i] ?? 0)) return (y[i] ?? 0) - (x[i] ?? 0);
  }
  return 0;
}

/** Every release, newest first, whatever order the file holds them in. */
export const releases: Release[] = [...(notes as Release[])].sort(newerFirst);

/** The newest release that has shipped: the one the download links serve. */
export const latest: Release | undefined = releases.find((r) => r.date);

/** Where to download `release`: the download table for the newest, its GitHub release for an older one. */
export function downloadHref(release: Release): string {
  return release === latest ? '/get#download' : `${source}/releases/tag/v${release.version}`;
}

/** The rail's groups: one per major, newest open and older ones closed, a heading per minor over its releases. */
export function railGroups(): Group[] {
  const majors = new Map<string, Map<string, Link[]>>();
  for (const r of releases) {
    const [major, minor] = r.version.split('.');
    const minors = majors.get(major) ?? new Map<string, Link[]>();
    majors.set(major, minors);
    const key = `${major}.${minor}`;
    minors.set(key, [
      ...(minors.get(key) ?? []),
      { href: `/releases/${r.version}`, title: r.version },
    ]);
  }
  return [...majors].map(([major, minors], i) => ({
    title: `Smeltr ${major}`,
    closed: i > 0,
    links: [...minors].map(([title, links]) => ({ title, links })),
  }));
}
