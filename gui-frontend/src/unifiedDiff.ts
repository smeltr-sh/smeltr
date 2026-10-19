// A unified diff read into files and numbered lines, so a line can be named
// by its number in the new file -- what a comment on a line sits on.

export interface DiffRow {
  /** `meta` is a file's own header line; `hunk` an `@@` header. */
  kind: 'meta' | 'hunk' | 'added' | 'removed' | 'same';
  /** The line as the diff wrote it, its leading mark included. */
  text: string;
  /** Its number in the new file: absent on a removed line and on headers. */
  newLine?: number;
}

export interface DiffFile {
  path: string;
  rows: DiffRow[];
}

/** The path a `diff --git a/<path> b/<path>` line names. */
function pathOf(header: string): string {
  const rest = header.slice('diff --git '.length);
  const same = /^a\/(.*) b\/\1$/.exec(rest);
  if (same) return same[1];
  const at = rest.lastIndexOf(' b/');
  return at >= 0 ? rest.slice(at + 3) : rest.replace(/^a\//, '');
}

/** Each file of a unified diff, in order. A new file starts at its
 * `diff --git` line; text before the first one goes to a file named `''`. */
export function parseUnifiedDiff(text: string): DiffFile[] {
  const files: DiffFile[] = [];
  let file: DiffFile | null = null;
  let next = 0;
  let inHunk = false;
  const lines = text.split('\n');
  if (lines[lines.length - 1] === '') lines.pop();
  for (const line of lines) {
    if (line.startsWith('diff --git ')) {
      file = { path: pathOf(line), rows: [{ kind: 'meta', text: line }] };
      files.push(file);
      inHunk = false;
      continue;
    }
    if (!file) {
      file = { path: '', rows: [] };
      files.push(file);
    }
    const hunk = /^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@/.exec(line);
    if (hunk) {
      next = Number(hunk[1]);
      inHunk = true;
      file.rows.push({ kind: 'hunk', text: line });
    } else if (!inHunk || line.startsWith('\\')) {
      file.rows.push({ kind: 'meta', text: line });
    } else if (line.startsWith('+')) {
      file.rows.push({ kind: 'added', text: line, newLine: next++ });
    } else if (line.startsWith('-')) {
      file.rows.push({ kind: 'removed', text: line });
    } else {
      file.rows.push({ kind: 'same', text: line, newLine: next++ });
    }
  }
  return files;
}

/** Each file's new-side lines the diff shows, by number, without their mark. */
export function newSideLines(files: DiffFile[]): Map<string, Map<number, string>> {
  const out = new Map<string, Map<number, string>>();
  for (const f of files) {
    const lines = out.get(f.path) ?? new Map<number, string>();
    for (const row of f.rows) {
      if (row.newLine !== undefined) lines.set(row.newLine, row.text.slice(1));
    }
    out.set(f.path, lines);
  }
  return out;
}
