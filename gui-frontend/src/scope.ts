/**
 * The color a worker group is drawn in (STYLE_GUIDE.md, "Group hues").
 *
 * One hue per group, at the accent's own lightness and chroma, so a group is
 * the same color turned rather than a second palette. Only the hue is decided
 * here; the theme picks the lightness and chroma, the way it does for
 * `--accent` itself. `shared` -- what every group draws on, not a group -- is
 * hued the same way, admin-chosen and stored under the reserved id `shared`
 * rather than in a group's own manifest; the caller passes it in as
 * `sharedHue`.
 */
import { SHARED_SCOPE, type WorkerGroupDto } from './api';

/** The last worker groups read, held for the page's life rather than a component's.
 *
 * `SectionShell` mounts a fresh `ScopePicker` on every Build page, so without this the chip and the rail's seam
 * lose their color on each navigation and take it back when the read returns. A hue kept from a read that
 * succeeded is not the guess the uncolored first paint avoids -- it is the last thing the leader said. */
let lastRead: { groups: WorkerGroupDto[]; sharedHue: number | null } = {
  groups: [],
  sharedHue: null,
};

export function lastScopeRead(): { groups: WorkerGroupDto[]; sharedHue: number | null } {
  return lastRead;
}

export function rememberScopeRead(groups: WorkerGroupDto[], sharedHue: number | null): void {
  lastRead = { groups, sharedHue };
}

/** The order a fleet's groups take their hues in. The leader assigns from the
 * same list (`worker_groups_api.rs`), so a hue it hands out and a hue derived
 * here are the same numbers. */
const DEFAULT_HUES = [240, 330, 180, 120, 90, 30, 270, 300, 210, 60, 0, 150];

/** The default at `index`, wrapping once the list runs out. */
export function defaultHue(index: number): number {
  const n = DEFAULT_HUES.length;
  return DEFAULT_HUES[((index % n) + n) % n];
}

/**
 * The hue `scope` is drawn in, or `null` when there is nothing to draw it in
 * yet -- the groups (or `shared`'s hue) have not been read, or `scope` names
 * a group this fleet does not have. No color is the right answer there: a
 * wrong one is the mistake this is meant to catch.
 *
 * `shared` draws `sharedHue` as given, `null` included. A group's own hue
 * when its manifest carries one. That is assigned when the group is created
 * and is the admin's to change, so it never moves under an operator who has
 * learned it. A group made before hues existed takes a default from where it
 * sorts among the others like it, which is at least the same answer on every
 * page.
 */
export function scopeHue(
  scope: string,
  groups: WorkerGroupDto[],
  sharedHue: number | null = null,
): number | null {
  if (scope === SHARED_SCOPE) return sharedHue;
  const group = groups.find((g) => g.id === scope);
  if (!group) return null;
  if (group.hue !== undefined) return group.hue;
  // Around the ones already spoken for, the way the leader picks a default when
  // it assigns one: a group waiting for a hue must not be drawn in a hue that is
  // already somebody's -- `shared`'s included, or a group from before hues
  // existed could be numbered onto the same hue the leader gave `shared`.
  const taken = new Set(groups.map((g) => g.hue).filter((hue) => hue !== undefined));
  if (sharedHue !== null) taken.add(sharedHue);
  const free = DEFAULT_HUES.filter((hue) => !taken.has(hue));
  const unhued = groups
    .filter((g) => g.hue === undefined)
    .map((g) => g.id)
    .sort();
  const index = unhued.indexOf(scope);
  return free.length > 0 ? free[index % free.length] : defaultHue(index);
}
