// What a branch under review is, shared by the pages that offer review.

import type { BranchDto, ChangeDto } from './api';

/** A branch whose change is open: the only state that can be reviewed,
 * deployed, or closed. */
export type OpenChange = BranchDto & { change: ChangeDto };

/** Open is the one state that counts. Closed and absent are both "not
 * open", so a branch with a closed change offers review again. */
export function isOpen(b: BranchDto): b is OpenChange {
  return b.change?.state === 'open';
}
