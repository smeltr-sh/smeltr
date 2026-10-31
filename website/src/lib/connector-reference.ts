import data from './connector-reference.json';
import { RECORD_FIELDS, type RecordField } from './reference.ts';

/** One setting of a connector. Leaving it out is refused when `required`, takes `default` when there is one, and
 * otherwise means what `absent` says. `values` lists each value of a setting that takes a fixed set of them. */
export type Setting = {
  name: string;
  type: string;
  required?: boolean;
  default?: unknown;
  absent?: string;
  examples: unknown[];
  what: string;
  values?: SettingValue[];
};

/** One value a setting takes, and what it does. */
export type SettingValue = { value: unknown; what: string };

export type ConnectorKind = {
  type: string;
  name: string;
  summary: string;
  /** Whether it also takes the group's `shared` settings. */
  shared?: boolean;
  settings: Setting[];
  /** The record fields a source gives. */
  fields?: string[];
  /** The record fields a destination reads. */
  reads?: string[];
  /** Its settings, `shared` ones included, in the order its GUI form asks for them. A group with a `section` is
   * behind that Advanced section of the form. */
  form?: { section?: string; settings: string[] }[];
};

/** `common` is every kind's settings; `shared` those of the kinds marked `shared`. */
export type ConnectorGroup = { common: Setting[]; shared?: Setting[]; kinds: ConnectorKind[] };

export const SOURCES: ConnectorGroup = data.sources;
export const DESTINATIONS: ConnectorGroup = data.destinations;

/** The kind `type` of `group`. */
export function kind(group: ConnectorGroup, type: string): ConnectorKind {
  const k = group.kinds.find((k) => k.type === type);
  if (!k) throw new Error(`no kind ${type}`);
  return k;
}

/** `type`'s settings as its GUI form groups them: the form's own, then each Advanced section by its label. */
export function formGroups(
  group: ConnectorGroup,
  type: string,
): { section?: string; settings: Setting[] }[] {
  const k = kind(group, type);
  const all = [...k.settings, ...(k.shared ? (group.shared ?? []) : [])];
  const named = (name: string) => {
    const s = all.find((s) => s.name === name);
    if (!s) throw new Error(`${type}'s form names ${name}, which it does not take`);
    return s;
  };
  if (!k.form) throw new Error(`${type} has no form`);
  return k.form.map((g) => ({
    section: g.section,
    settings: g.settings.map(named),
  }));
}

/** The record fields table's row for `name`. */
export function recordField(name: string): RecordField {
  const row = RECORD_FIELDS.find((f) => f.name === name);
  if (!row) throw new Error(`${name} is not in the record fields table`);
  return row;
}

/** Prose split into plain text, inline code spans, bold spans and links: the Markdown the reference's prose and
 * the commercial license use. */
export function inline(
  text: string,
): { kind: 'text' | 'code' | 'strong' | 'link'; text: string; href?: string; code?: boolean }[] {
  return text
    .split(/(`[^`]+`|\*\*[^*]+\*\*|\[[^\]]+\]\([^)]+\))/)
    .filter((piece) => piece.length > 0)
    .map((piece) => {
      if (piece.startsWith('`')) return { kind: 'code', text: piece.slice(1, -1) };
      if (piece.startsWith('**')) return { kind: 'strong', text: piece.slice(2, -2) };
      const link = /^\[([^\]]+)\]\(([^)]+)\)$/.exec(piece);
      // [`name`](href) links a code span: its text is the name, set as code.
      const code = !!link && /^`[^`]+`$/.test(link[1]);
      if (link)
        return { kind: 'link', text: code ? link[1].slice(1, -1) : link[1], href: link[2], code };
      return { kind: 'text', text: piece };
    });
}

/** The record fields every record has: those every source gives, and those no source gives and no destination
 * reads. */
export const GLOBAL_FIELDS: RecordField[] = RECORD_FIELDS.filter((f) => {
  const sources = SOURCES.kinds.filter((k) => k.fields?.includes(f.name)).length;
  const named = sources > 0 || DESTINATIONS.kinds.some((k) => k.reads?.includes(f.name));
  return sources === SOURCES.kinds.length || !named;
});
