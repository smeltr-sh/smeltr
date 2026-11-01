// A release's notes as Markdown, from `src/lib/releases.json`: the text the release pages show, for the GitHub
// release. `node release-notes.js <version>` prints them, and fails when the release has no dated entry, so a
// release cannot be cut or published without its notes.

import { readFileSync } from 'node:fs';

const SITE = 'https://smeltr.sh';
const version = process.argv[2];
const releases = JSON.parse(
  readFileSync(new URL('../src/lib/releases.json', import.meta.url), 'utf8'),
);
const release = releases.find((r) => r.version === version);
if (!release?.date) {
  console.error(
    `website/src/lib/releases.json has no dated entry for ${version}: write its notes and date there first`,
  );
  process.exit(1);
}

const absolute = (href) => (href.startsWith('/') ? SITE + href : href);
const lines = [release.summary, ''];
if (release.highlights?.length) {
  lines.push('## Highlights', '');
  for (const h of release.highlights)
    lines.push(`- **[${h.title}](${absolute(h.href)})** ${h.text}`);
  lines.push('');
}
for (const s of release.sections) {
  lines.push(`## ${s.title}`, '', ...s.items.map((i) => `- ${i}`), '');
}
lines.push(`[Smeltr ${version} on smeltr.sh](${SITE}/releases/${version})`);
console.log(lines.join('\n'));
