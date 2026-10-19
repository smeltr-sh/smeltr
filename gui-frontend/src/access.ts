// What the person signed in may do, from the session the leader last answered.
//
// The leader enforces every level; this is so the GUI does not offer what it would refuse. An
// area is `shared` or one worker group, the scope the scope chip picks. Every page reads it from
// here, and a shared component (`Field`, `ObjectList`) asks `editable()` rather than each page
// deciding for itself.

import { SHARED_SCOPE, currentScope, setScope } from './api';
import { parseRoute, showsScope } from './nav';
import { session, type AccessDto, type AccessLevel } from './session';

/** Every level, lowest first. */
export const LEVELS: AccessLevel[] = ['viewer', 'editor', 'deployer'];

/** What the person signed in holds. */
function mine(): AccessDto {
  const s = session();
  return s.access ?? { administrator: s.administrator, areas: {} };
}

export function isAdministrator(): boolean {
  return mine().administrator;
}

/** The level `access` holds in `area`, or `null` for none, as the leader's `Access::level` decides
 * it. An administrator holds every area. Holding any area reads `shared` too, since a group is read
 * merged with it; more than viewer there must be held by name. */
export function levelOf(access: AccessDto, area: string): AccessLevel | null {
  if (access.administrator) return 'deployer';
  const named = access.areas[area] ?? null;
  if (area === SHARED_SCOPE && Object.keys(access.areas).length > 0) return named ?? 'viewer';
  return named;
}

/** Whether `access` holds `area` at `level` or above. */
export function allows(access: AccessDto, area: string, level: AccessLevel): boolean {
  const held = levelOf(access, area);
  return held !== null && LEVELS.indexOf(held) >= LEVELS.indexOf(level);
}

/** The level held in `area` by the person signed in, or `null` for none. */
export function levelIn(area: string): AccessLevel | null {
  return levelOf(mine(), area);
}

/** Whether the person signed in holds `area` at `level` or above. */
export function holds(area: string, level: AccessLevel): boolean {
  return allows(mine(), area, level);
}

/** The area a repository path is in, as the leader's `policy::area_of_path` reads it: `shared/…` is
 * shared, `groups/<id>/…` is that group. `null` for a path in no area, which only an administrator
 * may change. */
export function areaOfPath(path: string): string | null {
  if (path.startsWith(`${SHARED_SCOPE}/`))
    return path.length > SHARED_SCOPE.length + 1 ? SHARED_SCOPE : null;
  const group = /^groups\/([^/]+)\/./.exec(path);
  return group ? group[1] : null;
}

/** Whether saving, capturing and previewing are open in `area`. */
export function canEdit(area: string = currentScope()): boolean {
  return holds(area, 'editor');
}

/** Whether a capture may be started here: the group's workers are asked for records, and a source's sample
 * is saved where the source is declared. Without a target, whether the scope chip's form may be opened, since
 * its sample is saved there. */
export function mayCapture(at?: {
  kind: 'source' | 'destination';
  scope: string;
  group: string;
}): boolean {
  if (!at) return canEdit();
  return canEdit(at.group) && (at.kind === 'destination' || canEdit(at.scope));
}

/** The areas a person who is not an administrator holds, `shared` first. Empty for an
 * administrator, who is not limited to any. */
export function heldAreas(): string[] {
  if (isAdministrator()) return [];
  const areas = Object.keys(session().access?.areas ?? {}).sort();
  return areas.length === 0 ? [] : [SHARED_SCOPE, ...areas.filter((a) => a !== SHARED_SCOPE)];
}

/** Whether the page on screen may be changed. Only an authoring page names a scope; everywhere
 * else the leader's own refusals are the rule. Read where a component mounts: a scope switch
 * mounts every authoring page afresh. */
export function editable(): boolean {
  if (typeof location === 'undefined') return true;
  return !showsScope(parseRoute(location.hash)) || canEdit();
}

/** Moves a remembered scope the person no longer holds to the first one they do, before any page
 * reads with it. */
export function settleScope(): void {
  const held = heldAreas();
  if (held.length > 0 && !held.includes(currentScope())) setScope(held[0]);
}

/** What a control the website demo does not do says in place of doing it (`demo/leader.ts` refuses it), or `''`
 * outside the demo. */
export const NOT_IN_DEMO = import.meta.env.MODE === 'demo' ? 'Not in the demo' : '';

/** Why approving and deploying a change touching `paths` is not `access`'s to do -- the person
 * signed in, unless another is named -- or `''` when it is. As the leader judges it: a deployer in
 * every area the change touches, and an administrator for a path in no area. */
export function deployBlocker(paths: string[], access: AccessDto = mine()): string {
  if (NOT_IN_DEMO) return NOT_IN_DEMO;
  if (access.administrator) return '';
  const outside = paths.find((p) => areaOfPath(p) === null);
  if (outside !== undefined) return `Deploying this takes an administrator: it changes ${outside}.`;
  const short = [...new Set(paths.map((p) => areaOfPath(p)!))]
    .sort()
    .filter((a) => !allows(access, a, 'deployer'));
  if (short.length === 0) return '';
  return `Deploying this takes a deployer in ${short.join(', ')}.`;
}
