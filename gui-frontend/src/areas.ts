// The areas a source or destination may be copied or moved into: `shared` and the worker groups the person edits.

import {
  SHARED_SCOPE,
  configAt,
  currentBranch,
  currentScope,
  listWorkerGroups,
  putDestinations,
  putSources,
  type DestinationDto,
  type SourceDto,
} from './api';
import { canEdit } from './access';

/** An area, by the name a person reads for it. */
export type Area = { id: string; name: string };

/** The areas the person may edit, the chip's first. */
export async function editableAreas(): Promise<Area[]> {
  const res = await listWorkerGroups();
  const all: Area[] = [
    { id: SHARED_SCOPE, name: 'Shared configuration' },
    ...(res.ok ? res.groups : []).map((g) => ({ id: g.id, name: g.name })),
  ].filter((a) => canEdit(a.id));
  const here = currentScope();
  return [...all.filter((a) => a.id === here), ...all.filter((a) => a.id !== here)];
}

/** The ids of `section` that a new one in `area` may not take, as far as the person can read: the area's own, and the
 * other side of the merge it runs in, which is `shared` for a group and the chip's group for `shared`. The leader
 * judges the rest on save. */
export async function idsTakenIn(
  area: string,
  section: 'sources' | 'destinations',
): Promise<string[]> {
  const other = area === SHARED_SCOPE ? currentScope() : SHARED_SCOPE;
  const read = [...new Set([area, other])].map(async (a) =>
    ((await configAt(currentBranch(), a)).config?.[section] ?? []).map((o) => o.id),
  );
  return (await Promise.all(read)).flat();
}

/** Saves `made` into `area`, after what is there, against `base`: the commit the calling page read, so a branch that
 * moved since is refused as its own Save would be. The commit it made, or null where the leader refused it, which it
 * has reported. */
export async function addInto(
  area: string,
  base: string | null,
  made:
    { section: 'sources'; object: SourceDto } | { section: 'destinations'; object: DestinationDto },
): Promise<string | null> {
  const there = await configAt(currentBranch(), area);
  if (!there.config) return null;
  const res =
    made.section === 'sources'
      ? await putSources([...there.config.sources, made.object], base, area)
      : await putDestinations([...there.config.destinations, made.object], base, area);
  return res.ok ? (res.commit ?? null) : null;
}
