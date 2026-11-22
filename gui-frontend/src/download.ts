/** Hands `text` to the browser as a file named `name`, which it saves where downloads go. */
export function downloadFile(name: string, text: string, type: string) {
  const url = URL.createObjectURL(new Blob([text], { type }));
  const a = document.createElement('a');
  a.href = url;
  a.download = name;
  a.click();
  URL.revokeObjectURL(url);
}

/** Hands `records` to the browser as `<stem>.ndjson`, one record per line, the file a capture downloads as. */
export function downloadRecords(stem: string, records: unknown[]) {
  const text = records.map((r) => JSON.stringify(r) + '\n').join('');
  downloadFile(`${fileStem(stem)}.ndjson`, text, 'application/x-ndjson');
}

/** `name` as a file name's stem: each run of characters a file name cannot hold becomes one dash, and no extension
 * is added. */
export function fileStem(name: string): string {
  return name.replace(/[^A-Za-z0-9._-]+/g, '-');
}
