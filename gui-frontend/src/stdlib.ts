// The names `ValidatedField`'s autocomplete suggests, read from the
// website's language reference (`reference.json`, `code-language.json`),
// which `tests/website-reference-tests.rs` keeps in step with the engine.

import reference from '../../website/src/lib/reference.json';
import codeLanguage from '../../website/src/lib/code-language.json';

export interface StdFuncEntry {
  name: string;
  signature: string;
  description: string;
  /** Where the website's language reference documents it. */
  docs?: string;
  /** A field of the object before the dot, not a function: it is written as it is named, with no call. */
  field?: boolean;
  /** Why it does not fit where it would be written: the function does not take the type before the dot. */
  warning?: string;
}

const REFERENCE = 'https://smeltr.sh/docs/code-reference';

/** Functions an Eval assignment or a Code body can call and a condition
 * cannot, since only those expand them (`expr::inline::PRELUDE_FUNCTIONS`;
 * `ops::stdfuncs` holds this to it). */
export const NOT_IN_CONDITIONS = ['map', 'filter'];

/** An entry for a function the reference documents under `anchor`. */
function entry(f: { name: string; signature: string; summary: string }, anchor: string) {
  return {
    name: f.name,
    signature: f.signature,
    description: f.summary,
    docs: `${REFERENCE}#${anchor}`,
  };
}

const documented: StdFuncEntry[] = reference.groups.flatMap((g) =>
  (g.functions ?? []).map((f) => entry(f, f.name)),
);

const parseFunctions: StdFuncEntry[] = codeLanguage.parseFunctions.map((f) =>
  entry(f, `parse-${f.name}`),
);

/** What a condition can call: a step's filter, a Drop's or an If's condition. */
export const CONDITION_FUNCTIONS: StdFuncEntry[] = [
  ...documented.filter((f) => !NOT_IN_CONDITIONS.includes(f.name)),
];

/** What an Eval assignment can call: every function, and a parse function as
 * the whole value it writes. */
export const ASSIGNMENT_FUNCTIONS: StdFuncEntry[] = [...documented, ...parseFunctions];

/** What a Code body can call besides: the statements written as a call (`drop()`). */
export const CODE_FUNCTIONS: StdFuncEntry[] = [
  ...ASSIGNMENT_FUNCTIONS,
  ...codeLanguage.statements.flatMap((s) => {
    const name = /^([a-z]\w*)\(/.exec(s.form)?.[1];
    return name
      ? [{ name, signature: s.form, description: s.rule, docs: `${REFERENCE}#statements` }]
      : [];
  }),
];
