// A sample as the specs write and read one.
import { pastedTable, tableRecords } from '../../gui-frontend/src/capture';

/** The table of `records` pasted, as a sample holds it. */
export const pasted = pastedTable;

/** The records a sample's table holds, as the GUI shows them. */
export function recordsOf(sample: Parameters<typeof tableRecords>[0]): Record<string, unknown>[] {
  return tableRecords(sample).map((held) => held.record);
}
