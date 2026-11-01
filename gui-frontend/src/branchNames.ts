import { slugify } from './slugify';

// The name the branch prompt offers when a first edit starts on `main`: who is editing and what the edit is
// about, as `owner/what-they-are-doing` (`check_branch_name`), so the Branches page answers both at a glance.
// A name already taken gets a number, so two people who both accept it start branches of their own rather
// than landing on one another's.

/** What a write to `url` changes, in a few words: `sources`, `pipeline-fortigate`, `network-routes`.
 * The worker group leads when it is not the shared configuration. */
export function editedBy(url: string): string {
  const path = url.split('?')[0].split('/').filter(Boolean).map(decodeURIComponent);
  // /api/areas/<area>/<kind>[/<id>...]
  if (path[1] === 'areas') {
    const [area, kind = '', ...rest] = path.slice(2);
    const within = kind === 'files' ? (rest.at(-1) ?? 'files').replace(/\.[^.]*$/, '') : kind;
    const what =
      kind !== 'files' && rest.length ? `${kind.replace(/s$/, '')}-${rest.join('-')}` : within;
    return [area === 'shared' ? '' : area, what].filter(Boolean).join('-');
  }
  // /api/<kind>[/<id>]
  const [kind = '', id] = path.slice(1);
  return id ? `${kind.replace(/s$/, '')}-${id}` : kind;
}

/** Something like `dana/routes`, or `dana/routes-2` when `dana/routes` is taken. Just `routes` when nobody is
 * named. */
export function suggestBranchName(who: string, what: string, taken: Iterable<string>): string {
  const base =
    [slugify(who), slugify(what).slice(0, 60).replace(/-+$/, '')].filter(Boolean).join('/') ||
    'edit';
  const names = new Set(taken);
  if (!names.has(base)) return base;
  for (let n = 2; ; n++) if (!names.has(`${base}-${n}`)) return `${base}-${n}`;
}
