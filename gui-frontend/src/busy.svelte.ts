import { reported } from './errors.svelte';

/** A page's busy flag over the call it runs: `on` while the call is out, and a failure the call throws `reported`
 * rather than thrown on. */
export class Busy {
  on = $state(false);

  async run(call: () => Promise<void>): Promise<void> {
    this.on = true;
    try {
      await call();
    } catch (e) {
      reported(e);
    } finally {
      this.on = false;
    }
  }
}
