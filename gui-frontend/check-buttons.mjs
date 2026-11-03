// Refuses `disabled` on a bare `<button>`: a button that can be disabled is a `Button`, whose `disabled` is the
// reason it gives (`src/Button.svelte`). Svelte's own parser reads each component, so no layout of a tag slips by.
import { readdirSync, readFileSync } from 'node:fs';
import { join, relative } from 'node:path';
import { parse } from 'svelte/compiler';

process.chdir(import.meta.dirname);

function* components(dir) {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) yield* components(path);
    else if (entry.name.endsWith('.svelte')) yield path;
  }
}

function* bareDisabled(node) {
  if (!node || typeof node !== 'object') return;
  if (Array.isArray(node)) {
    for (const child of node) yield* bareDisabled(child);
    return;
  }
  if (
    node.type === 'RegularElement' &&
    node.name === 'button' &&
    node.attributes.some((a) => a.type === 'Attribute' && a.name === 'disabled')
  )
    yield node.start;
  for (const [key, value] of Object.entries(node)) if (key !== 'parent') yield* bareDisabled(value);
}

const found = [];
for (const path of components('src')) {
  if (path === join('src', 'Button.svelte')) continue;
  const source = readFileSync(path, 'utf8');
  for (const at of bareDisabled(parse(source, { modern: true }).fragment)) {
    const line = source.slice(0, at).split('\n').length;
    found.push(`${relative('.', path)}:${line}`);
  }
}
if (found.length > 0) {
  console.error(
    `A bare <button> is disabled without a reason. Use Button, whose disabled is the reason:\n${found.join('\n')}`,
  );
  process.exit(1);
}
