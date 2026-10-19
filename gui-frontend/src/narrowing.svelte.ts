/**
 * A list narrowed by `?only=` (`connections.ts`), which widens for good once a
 * row it hides has to be shown: a save error's, or one just added. The hash
 * drops `?only=` with it, so the notice and a reload agree.
 */
export class Narrowing {
  #only: () => string[] | undefined;
  #widenedFrom = $state<string[] | undefined>();

  constructor(only: () => string[] | undefined) {
    this.#only = only;
  }

  /** The ids the list shows, or nothing where it shows them all. */
  get ids(): string[] | undefined {
    const only = this.#only();
    return only !== this.#widenedFrom ? only : undefined;
  }

  keeps(id: string): boolean {
    const ids = this.ids;
    return !ids || ids.includes(id);
  }

  /** The same list, with its `?only=` taken off. */
  get all(): string {
    return location.hash.split('?')[0];
  }

  widen(): void {
    if (!this.ids) return;
    this.#widenedFrom = this.#only();
    location.hash = this.all;
  }
}
