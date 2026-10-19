// A lookup table's rows as the editor holds them: a CSV file read into a grid and written back,
// what each column holds, and which pipeline steps read a table.

/** Records of `text`, split on `delimiter` with RFC 4180 quoting: a quoted field may hold the
 * delimiter, a line break, or a doubled `""` for one quote; a record ends at LF or CRLF. A
 * blank line is skipped rather than read as a record, as the leader's own loader does. */
export function parseCsv(text: string, delimiter: string): string[][] {
  const records: string[][] = [];
  let record: string[] = [];
  let field = '';
  let quoted = false;
  // The line has a delimiter or a quote, so it is a record even when every field is empty.
  let seen = false;
  const endRecord = () => {
    if (seen || field.trim() !== '') records.push([...record, field]);
    record = [];
    field = '';
    seen = false;
  };
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (quoted) {
      if (c !== '"') field += c;
      else if (text[i + 1] === '"') field += text[++i];
      else quoted = false;
    } else if (c === '"' && field === '') {
      quoted = seen = true;
    } else if (c === delimiter) {
      record.push(field);
      field = '';
      seen = true;
    } else if (c === '\n' || c === '\r') {
      if (c === '\r' && text[i + 1] === '\n') i++;
      endRecord();
    } else field += c;
  }
  endRecord();
  return records;
}

/** `records` as CSV text, quoting a field only where it holds the delimiter, a quote or a line
 * break. A record of one empty field is quoted, or it would read back as a blank line. */
export function serializeCsv(records: string[][], delimiter: string): string {
  const quote = (f: string) =>
    f.includes(delimiter) || /["\r\n]/.test(f) ? `"${f.replaceAll('"', '""')}"` : f;
  return records
    .map((r) => (r.length === 1 && r[0] === '' ? '""' : r.map(quote).join(delimiter)))
    .map((line) => line + '\n')
    .join('');
}

/** Per column: its name, how many distinct values it holds and how many cells are empty. */
export function columnStats(headers: string[], rows: string[][]): [string, number, number][] {
  return headers.map((name, c) => {
    const values = rows.map((r) => r[c] ?? '');
    return [name, new Set(values).size, values.filter((v) => v === '').length];
  });
}

/** Every `lookup` step, at any depth, that reads table `id`: `pipeline › step` by the pipeline's
 * name, its input, its key column and its `column → field` outputs. Walks the raw step JSON, which a pipeline the builder
 * cannot represent still has. */
export function stepsReading(pipelines: { name: string; steps: unknown[] }[], id: string) {
  const found: string[][] = [];
  const visit = (pipeline: string, steps: unknown) => {
    if (!Array.isArray(steps)) return;
    for (const s of steps as Record<string, unknown>[]) {
      if (s?.type === 'if') {
        visit(pipeline, s.then);
        visit(pipeline, s.else);
      } else if (s?.type === 'lookup' && s.table === id) {
        const outputs = (Array.isArray(s.outputs) ? s.outputs : []) as Record<string, unknown>[];
        const reads = outputs.map((o) => `${o.source} → ${o.field}`).join(', ');
        found.push([`${pipeline} › ${s.id}`, String(s.input), String(s.keyColumn ?? ''), reads]);
      }
    }
  };
  for (const p of pipelines) visit(p.name, p.steps);
  return found;
}
