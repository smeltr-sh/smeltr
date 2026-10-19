import data from './step-reference.json';
import type { ConnectorGroup } from './connector-reference.ts';
export { kind } from './connector-reference.ts';

/** The pipeline steps: `common` is every step's settings, `shared` those of the steps marked `shared`. */
export const STEPS: ConnectorGroup = data.steps;
/** The Parser step's formats, each the `kind` it takes. */
export const PARSER_FORMATS: ConnectorGroup = data.parserFormats;
