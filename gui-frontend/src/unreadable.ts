// The files of a bundle that do not load, and the load errors they account for. A load error
// names its file relative to its bundle (`config_file::bundle::load_lenient`) or by its full
// path, while the list always names it by its full path in the repository.

import type { UnreadableFileDto } from './api';

/** Whether a load error is one of `files` failing to load: its message is `<path>: <why>`,
 * the path relative to the bundle or the repository. */
export function aboutFile(message: string, files: UnreadableFileDto[]): boolean {
  const at = message.indexOf(': ');
  if (at <= 0) return false;
  const named = message.slice(0, at);
  return files.some((f) => f.path === named || withinBundle(f.path) === named);
}

/** A repository path relative to the bundle holding it: `shared/` or `groups/<id>/`. */
function withinBundle(path: string): string {
  return path.replace(/^(shared|groups\/[^/]+)\//, '');
}

/** Where one file's raw text is edited, and where its editor goes back to. */
export function fileHash(path: string, back?: string): string {
  const q = new URLSearchParams({ path });
  if (back) q.set('back', back);
  return `#/build/file?${q}`;
}
