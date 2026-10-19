// The Changes pages, as a person reads them.
import type { Page } from '@playwright/test';

/** The History table's row for the commit whose subject is `subject`. */
export function historyRow(page: Page, subject: string) {
  return page.getByRole('row').filter({ has: page.getByText(subject, { exact: true }) });
}
