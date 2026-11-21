// The areas an object may be moved into: `shared` and the worker groups the person edits.

import { SHARED_SCOPE, currentScope, listWorkerGroups } from './api';
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
