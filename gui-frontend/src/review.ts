// Where a change's review stands: its reviewers' verdicts, its tests, and
// what the composer may send.

import type { TestResult, TimelineEvent } from './api';

type VerdictEvent = TimelineEvent & { kind: 'approved' | 'changes-requested' };

function isVerdict(e: TimelineEvent): e is VerdictEvent {
  return e.kind === 'approved' || e.kind === 'changes-requested';
}

/** Each reviewer's last verdict, in the order they gave it. A reviewer is their account where the
 * event names one, and their name otherwise. `events` is oldest first, as the timeline gives it. */
export function latestVerdicts(events: TimelineEvent[]): VerdictEvent[] {
  const byReviewer = new Map<string, VerdictEvent>();
  for (const e of events) {
    if (!isVerdict(e)) continue;
    const who = e.authorId ?? e.author;
    // Deleting first moves a changed mind to the end, where its time puts it.
    byReviewer.delete(who);
    byReviewer.set(who, e);
  }
  return [...byReviewer.values()];
}

/** What the change's reviews come to. One standing request for changes outweighs every approval. */
export type Reviews =
  | { state: 'changes-requested'; names: string[] }
  | { state: 'approved'; names: string[] }
  | { state: 'none' };

export function reviews(events: TimelineEvent[]): Reviews {
  const latest = latestVerdicts(events);
  const requested = latest.filter((e) => e.kind === 'changes-requested');
  if (requested.length > 0) {
    return { state: 'changes-requested', names: requested.map((e) => e.author) };
  }
  const approved = latest.filter((e) => e.kind === 'approved');
  if (approved.length > 0) return { state: 'approved', names: approved.map((e) => e.author) };
  return { state: 'none' };
}

/** How a change's tests came out. A test that could not run is neither passed nor failed. */
export interface TestsTally {
  total: number;
  passed: number;
  failed: number;
  couldNotRun: number;
}

export function testsTally(results: TestResult[]): TestsTally {
  const passed = results.filter((t) => t.verdict?.passed).length;
  const failed = results.filter((t) => t.verdict && !t.verdict.passed).length;
  return { total: results.length, passed, failed, couldNotRun: results.length - passed - failed };
}

/** What the composer sends: a comment, or a review verdict with the text as its body. */
export type ComposerChoice = 'comment' | 'approve' | 'request-changes';

/** Whether the composer has something the leader would take: a comment and a request for changes
 * each need words; an approval may go without. */
export function canSubmit(choice: ComposerChoice, text: string): boolean {
  return choice === 'approve' || text.trim() !== '';
}
