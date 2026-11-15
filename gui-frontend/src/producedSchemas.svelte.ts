/**
 * The schemas the pipeline being edited produces, and the fields its record holds, for whatever below the editor
 * offers the schemas' fields: the name boxes (`ValidatedField`) and the Fields menu.
 */

import { getContext, setContext } from 'svelte';
import { SchemaTree } from './schemaTree';
import type { SchemaDto } from './api';

const CONTEXT = Symbol('produced schemas');

export class ProducedSchemas {
  /** The produced schemas' fields, or null where the pipeline produces none. */
  tree = $state.raw<SchemaTree | null>(null);
  /** Each path the record holds: what the pipeline declares and the records carry. */
  held = $state.raw<ReadonlySet<string>>(new Set());

  #schemas: SchemaDto[] = [];

  /** Rebuilds the tree only when the schemas themselves change: a standard's runs to tens of thousands of fields. */
  setSchemas(schemas: SchemaDto[]) {
    if (schemas.length === this.#schemas.length && schemas.every((s, i) => s === this.#schemas[i]))
      return;
    this.#schemas = schemas;
    this.tree = schemas.length > 0 ? new SchemaTree(schemas) : null;
  }

  /** The type every produced schema that names `path` gives it, where they agree and it is one a parse reads text
   * as; the compiler's `schema_type` says the same. Undefined where none names it, or they disagree. */
  typeOf(path: string): string | undefined {
    const types = this.#schemas.flatMap((s) =>
      s.fields.filter((f) => f.field === path).map((f) => f.type),
    );
    const ty = types[0];
    return ty !== undefined && READ_AS.includes(ty) && types.every((t) => t === ty)
      ? ty
      : undefined;
  }
}

/** The types a parse reads a field's text as. */
const READ_AS = ['str', 'i64', 'f64', 'bool', 'datetime'];

/** Makes one `ProducedSchemas` the one every field below reads. */
export function provideProducedSchemas(): ProducedSchemas {
  return setContext(CONTEXT, new ProducedSchemas());
}

/** The editor's `ProducedSchemas`, where there is one. */
export function producedSchemas(): ProducedSchemas | undefined {
  return getContext<ProducedSchemas | undefined>(CONTEXT);
}
