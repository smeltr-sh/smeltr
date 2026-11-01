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

/** A release's major, minor and patch: `just cut-release` names every release so. */
function parts(release: Release): number[] {
  return release.version.split('.').map(Number);
}

/** Every release, newest first, whatever order the file holds them in. */
export const releases: Release[] = [...(notes as Release[])].sort((a, b) => {
  const [x, y] = [parts(a), parts(b)];
  return y[0] - x[0] || y[1] - x[1] || y[2] - x[2];
});

/** The newest release that has shipped: the one the download links serve. */
export const latest: Release | undefined = releases.find((r) => r.date);

/** Where to download `release`: the download table for the newest, its GitHub release for an older one. */
export function downloadHref(release: Release): string {
  return release === latest ? '/get#download' : `${source}/releases/tag/v${release.version}`;
}

/** The rail's groups: one per major, with a heading per minor over its releases. */
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
  return [...majors].map(([major, minors]) => ({
    title: `Smeltr ${major}`,
    links: [...minors].map(([title, links]) => ({ title, links })),
  }));
}
