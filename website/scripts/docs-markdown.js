// The docs as Markdown, for language models: the site's and the leader's AI agent's one text.
//
// Converts each docs page `vite build` prerendered. Writes each page's twin beside it (`docs/x.html` gets
// `docs/x.md`) and `llms.txt`, which lists them. With `--leader`, also writes `src/leader/docs.json`, each page cut
// into its sections at its headings; with `--check`, fails when that file is not what this build makes.

import { readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { Window } from 'happy-dom';
import TurndownService from 'turndown';

const BUILD = new URL('../build/', import.meta.url);
const LEADER = new URL('../../src/leader/docs.json', import.meta.url);

const turndown = new TurndownService({
  headingStyle: 'atx',
  codeBlockStyle: 'fenced',
  bulletListMarker: '-',
});
// Tables, GFM's way. A table with no heading row gets an empty one: GFM has no table without.
const cells = (row) => [...row.children].filter((c) => c.nodeName === 'TD' || c.nodeName === 'TH');
turndown.addRule('table cell', {
  filter: ['th', 'td'],
  replacement: (content) =>
    ` ${content
      .trim()
      .replace(/\s*\n+\s*/g, '<br>')
      .replace(/\|/g, '\\|')} |`,
});
turndown.addRule('table row', {
  filter: 'tr',
  replacement: (content) => `\n|${content}`,
});
turndown.addRule('table section', {
  filter: ['thead', 'tbody', 'tfoot'],
  replacement: (content) => content,
});
turndown.addRule('table caption', {
  filter: 'caption',
  replacement: () => '',
});
turndown.addRule('table', {
  filter: 'table',
  replacement: (content, node) => {
    const rows = [...node.rows];
    if (rows.length === 0) return '';
    const width = Math.max(...rows.map((r) => cells(r).length));
    const rule = `\n|${' --- |'.repeat(width)}`;
    const headed = cells(rows[0]).every((c) => c.nodeName === 'TH');
    const lines = content.trim().split('\n');
    const table = headed
      ? [lines[0], rule.trim(), ...lines.slice(1)]
      : [`|${'  |'.repeat(width)}`, rule.trim(), ...lines];
    const caption = node.querySelector('caption')?.textContent.trim();
    return `\n\n${caption ? `${caption}:\n\n` : ''}${table.join('\n')}\n\n`;
  },
});
// A function's signatures, side by side on the page.
turndown.addRule('signatures', {
  filter: (node) => node.nodeName === 'SPAN' && node.parentNode?.classList?.contains('types'),
  replacement: (content, node) => (node.nextElementSibling ? `${content}; ` : content),
});
// A heading's link to itself.
turndown.addRule('heading link', {
  filter: (node) => node.nodeName === 'A' && node.classList.contains('heading-link'),
  replacement: (content) => content.replace(/#$/, ''),
});
// An example's given fields, then the expression it runs on them.
turndown.addRule('given', {
  filter: (node) => node.nodeName === 'CODE' && node.classList.contains('given'),
  replacement: (content, node) =>
    `\`${node.textContent}\`${node.nextElementSibling?.classList.contains('given') ? ';' : ':'}`,
});
// Only for the eye.
turndown.remove(['button', 'nav', 'script', 'style']);
turndown.addRule('page tools', {
  filter: (node) => node.nodeName === 'ASIDE' && node.classList.contains('page-tools'),
  replacement: () => '',
});
// A screenshot is linked to its full size, once per theme: keep one, as its alt text.
turndown.addRule('screenshot', {
  filter: (node) => node.nodeName === 'A' && node.getElementsByTagName('img').length > 0,
  replacement: (_, node) => {
    const img = node.getElementsByTagName('img')[0];
    return /-dark\.\w+$/.test(img.getAttribute('src'))
      ? ''
      : `![${img.alt}](${img.getAttribute('src')})`;
  },
});
// A card is a link around a heading and a line: the heading is the link.
turndown.addRule('card', {
  filter: (node) => node.nodeName === 'A' && node.querySelector('h1, h2, h3, h4, h5, h6') !== null,
  replacement: (content, node) =>
    `\n\n${content
      .trim()
      .replace(
        /^(#{1,6}) (.*)$/m,
        (_, hashes, text) => `${hashes} [${text}](${node.getAttribute('href')})`,
      )}\n\n`,
});
// An icon has nothing to say; a diagram says what its title and description do.
turndown.addRule('svg', {
  filter: (node) => node.nodeName === 'svg' || node.nodeName === 'SVG',
  replacement: (_, node) => {
    const said = ['title', 'desc']
      .map((tag) => node.querySelector(tag)?.textContent.trim())
      .filter(Boolean);
    return said.length ? `\n\n*Diagram: ${said.join('. ')}*\n\n` : '';
  },
});
// A highlighted block draws each line as its own element, with no line break between them.
turndown.addRule('lined code', {
  filter: (node) => node.nodeName === 'PRE' && node.querySelector('span.line') !== null,
  replacement: (_, node) => {
    const lines = [...node.querySelectorAll('span.line')].map((line) => line.textContent);
    return `\n\n\`\`\`\n${lines.join('\n')}\n\`\`\`\n\n`;
  },
});
// A setting's examples are separate values, which side by side would run into one.
turndown.addRule('examples', {
  filter: (node) => node.nodeName === 'DIV' && node.classList.contains('examples'),
  replacement: (_, node) => {
    const label = node.querySelector('span')?.textContent.trim() ?? 'Examples';
    const values = [...node.querySelectorAll('code')].map((c) => `- \`${c.textContent}\``);
    return `\n\n${label}:\n\n${values.join('\n')}\n\n`;
  },
});

/// Every prerendered docs page Pagefind indexes, by its path on the site.
function pages() {
  const html = [{ path: '/docs', file: 'docs.html' }];
  for (const entry of readdirSync(new URL('docs/', BUILD), { recursive: true })) {
    // Windows names a nested file with `\`; a path on the site has `/`.
    const name = entry.replaceAll('\\', '/');
    if (name.endsWith('.html'))
      html.push({ path: `/docs/${name.slice(0, -5)}`, file: `docs/${name}` });
  }
  return html
    .map(({ path, file }) => {
      const doc = new Window().document;
      doc.write(readFileSync(new URL(file, BUILD), 'utf8'));
      const body = doc.querySelector('[data-pagefind-body]');
      if (!body) return null;
      // A diagram drawn twice, once per layout, is said once.
      const markdown =
        turndown
          .turndown(body)
          .trim()
          .replace(/(\*Diagram: [^\n]*\*)(\n\n\1)+/g, '$1') + '\n';
      const description =
        doc.querySelector('meta[name="description"]')?.getAttribute('content') ?? '';
      const headings = [...body.querySelectorAll('h1, h2, h3, h4, h5, h6')].map((h) => ({
        id: h.id,
        level: Number(h.nodeName[1]),
      }));
      return {
        path,
        file,
        title: doc.querySelector('h1').textContent.trim(),
        description,
        markdown,
        headings,
      };
    })
    .filter(Boolean)
    .sort((a, b) => a.path.localeCompare(b.path));
}

/// `page`'s Markdown cut at its headings, each section named by its heading's id on the page.
function sections(page) {
  const cuts = [];
  let fenced = false;
  const lines = page.markdown.split('\n');
  lines.forEach((line, i) => {
    if (line.startsWith('```')) fenced = !fenced;
    const heading = !fenced && /^(#{1,6}) (.*)$/.exec(line);
    if (heading) cuts.push({ line: i, level: heading[1].length, heading: heading[2] });
  });
  if (
    cuts.length !== page.headings.length ||
    cuts.some((c, i) => c.level !== page.headings[i].level)
  ) {
    throw new Error(`${page.path}: its Markdown's headings are not its page's`);
  }
  return cuts.map((cut, i) => ({
    // The page's own heading names no section: it is the page's opening.
    id: i === 0 ? '' : page.headings[i].id,
    level: cut.level,
    heading: cut.heading,
    text: lines
      .slice(i === 0 ? 0 : cut.line, cuts[i + 1]?.line ?? lines.length)
      .join('\n')
      .trim(),
  }));
}

const all = pages();
for (const page of all)
  writeFileSync(new URL(page.file.replace(/\.html$/, '.md'), BUILD), page.markdown);
writeFileSync(
  new URL('llms.txt', BUILD),
  [
    '# Smeltr',
    '',
    '> Smeltr is a log and event pipeline. These are its user docs, one Markdown file per page.',
    '',
    '## Docs',
    '',
    ...all.map((p) => `- [${p.title}](${p.path}.md)${p.description ? `: ${p.description}` : ''}`),
    '',
  ].join('\n'),
);

if (process.argv.includes('--leader') || process.argv.includes('--check')) {
  // A section to a line, so a diff shows the section that changed rather than its whole page.
  const page = (p) => {
    const head = JSON.stringify({ path: p.path, title: p.title });
    const body = sections(p).map((s) => JSON.stringify(s)).join(',\n');
    return `${head.slice(0, -1)},"sections":[\n${body}\n]}`;
  };
  const json = `[\n${all.map(page).join(',\n')}\n]\n`;
  if (process.argv.includes('--check')) {
    if (readFileSync(LEADER, 'utf8') !== json) {
      console.error(
        'src/leader/docs.json is not what this build of the docs makes: run `just docs-markdown`.',
      );
      process.exit(1);
    }
  } else {
    writeFileSync(LEADER, json);
  }
}
