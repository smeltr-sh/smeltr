export type Link = { href: string; title: string };
/** A sidebar entry: a page, or a heading over its pages. */
export type Entry = Link | { title: string; links: Link[] };
/** A sidebar group. Each folds on a click. A closed one starts folded, except on its own pages. */
export type Group = { title: string; closed?: boolean; links: Entry[] };
