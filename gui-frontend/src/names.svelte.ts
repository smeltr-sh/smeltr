import { namesAt } from './pipelineNames';

/** Each scope's pipeline and route names on each branch, read when first asked for, and once more the first time
 * they lack an id, which was added since. A person reads an object by its name: its id is a key nobody chose. */
class Names {
  #read = $state<Record<string, Awaited<ReturnType<typeof namesAt>>>>({});
  #asked = new Set<string>();

  /** What a person calls `kind` `id` of `scope` on `branch`: its name, or `id` until one is read. */
  of(branch: string, scope: string, kind: 'pipelines' | 'routes', id: string): string {
    const key = `${branch}\u0000${scope}`;
    const name = this.#read[key]?.[kind][id];
    const ask = this.#read[key] && !name ? `${key}\u0000again` : key;
    if (!this.#asked.has(ask)) {
      this.#asked.add(ask);
      namesAt(branch, scope).then((n) => (this.#read[key] = n));
    }
    return name || id;
  }
}

export const names = new Names();
