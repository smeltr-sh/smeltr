export type Link = { href: string; title: string };
/** A sidebar entry: a page, or a heading over its pages. */
export type Entry = Link | { title: string; links: Link[] };
/** A sidebar group. It starts folded, except on its own pages or when `open`, and folds or opens on a click. */
export type Group = { title: string; open?: boolean; links: Entry[] };
