/** A name made into a path segment -- a group's id, a part of a branch name -- so it is
 * filesystem- and git-safe: lowercase `[a-z0-9-]`, single hyphens, no
 * leading or trailing hyphen.
 *
 * Accents are folded to the letter they sit on rather than dropped, so
 * `Café Süd` is `cafe-sud` and not `caf-s-d`. A name with no Latin
 * letters at all slugs to nothing, which the form refuses -- an id is
 * typed by hand there. */
export function slugify(name: string): string {
  return name
    .normalize('NFD')
    .replace(/\p{Diacritic}/gu, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}
