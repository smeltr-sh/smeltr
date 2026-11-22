import { editable } from './access';
import { runTests } from './api';
import { standingOf, type Standing } from './objectChanges';

/**
 * Where each test it is asked to run stands on the branch, by test id, as the run comes back: the sample menu's
 * marks, the Pipelines list's counts. `of` is absent where nothing is run (a viewer, who may not run tests, or no
 * tests) or the run failed, and empty while it is in flight. Only the last run asked for lands.
 */
export class Standings {
  of: Record<string, Standing> | undefined = $state();
  #runs = 0;

  async run(ids: string[]): Promise<void> {
    const run = ++this.#runs;
    this.of = undefined;
    // An empty `ids` would run every test.
    if (!editable() || ids.length === 0) return;
    this.of = {};
    let found: Record<string, Standing> | undefined;
    try {
      const res = await runTests({ ids });
      if (res.ok || res.results.length > 0) {
        found = Object.fromEntries(res.results.map((r) => [r.id, standingOf(r)]));
      }
    } catch {
      // A failed run leaves `of` absent.
    }
    if (run === this.#runs) this.of = found;
  }

  /** How many of `ids` stand as `standing`. */
  count(ids: string[], standing: Standing): number {
    return ids.filter((id) => this.of?.[id] === standing).length;
  }
}
