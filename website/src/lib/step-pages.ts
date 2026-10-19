import { parse } from './json-text.ts';
import { destinations, sources } from './connections.ts';

/** The Pipeline reference pages, one per step type, in order of title. */
export const stepPages = [
  { href: '/docs/steps/call', title: 'Call' },
  { href: '/docs/steps/code', title: 'Code' },
  { href: '/docs/steps/comment', title: 'Comment' },
  { href: '/docs/steps/drop', title: 'Drop' },
  { href: '/docs/steps/eval', title: 'Eval' },
  { href: '/docs/steps/if', title: 'If' },
  { href: '/docs/steps/lookup', title: 'Lookup' },
  { href: '/docs/steps/parser', title: 'Parser' },
  { href: '/docs/steps/rename', title: 'Rename' },
  { href: '/docs/steps/unroll', title: 'Unroll' },
];

/** The Reference pages that are not step pages, and the old paths that redirect to Reference pages. */
const otherReferencePaths = [
  '/docs/code-reference',
  '/docs/steps',
  '/docs/sources',
  '/docs/destinations',
  '/docs/reference',
  '/reference',
  '/docs/parsers',
  '/docs/code-steps',
];

/** Whether `path` is a Reference page rather than a guide: the expression reference, a step page, the Sources or
 * Destinations page, the administrator settings, or an old path that redirects to one. These share the Reference
 * sidebar and the top bar's Reference link. */
export function isReferencePage(path: string): boolean {
  return (
    otherReferencePaths.includes(path) ||
    [...stepPages, ...sources, ...destinations].some((p) => p.href === path)
  );
}

/** A step page's examples, `#lib/steps/<page>.json` as text, each given `image`: the name its GUI screenshots
 * share under `/steps/`, which `tools/screenshots/examples.py` writes. */
export function stepExamples(page: string, text: string) {
  const examples = parse(text);
  for (const name in examples) examples[name].image = `steps/${page}-${name}`;
  return examples;
}
