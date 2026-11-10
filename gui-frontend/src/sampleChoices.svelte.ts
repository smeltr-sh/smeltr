// The records a person may run over, as `SampleMenu` offers them: the saved samples of this branch and scope, and
// the finished capture the leader holds for the person signed in, not yet saved. Every screen that runs a sample
// reads them through this, so each offers the same choices.

import { heldCount } from './capture';
import {
  capturedRecords,
  getSample,
  heldChoice,
  heldSession,
  listSamples,
  myCapture,
  type CaptureStatus,
  type SampleRecordDto,
  type SampleSummary,
} from './api';

export class SampleChoices {
  samples: SampleSummary[] = $state([]);
  /** Whether the samples and the held capture have been read once. */
  loaded = $state(false);
  /** The finished capture the leader holds for the person signed in. */
  held: CaptureStatus | null = $state(null);
  #heldRead: Promise<unknown>;

  /** Reads the samples and the held capture. A failed read of the samples goes to `failed`; a held capture that
   * cannot be read is left off. */
  constructor(failed: (e: unknown) => void) {
    this.#heldRead = myCapture()
      .then((res) => (this.held = res.capture?.done ? res.capture : null))
      .catch(() => {});
    this.load().catch(failed);
  }

  /** Reads the samples again, as after one is saved. */
  async load() {
    const [listed] = await Promise.all([listSamples(), this.#heldRead]);
    this.samples = listed.samples;
    this.loaded = true;
  }

  /** The held capture as `SampleMenu` lists it. */
  get menuHeld(): { choice: string; records: number; source?: string } | undefined {
    const held = this.held;
    return held
      ? { choice: heldChoice(held.session), records: heldCount(held), source: held.source }
      : undefined;
  }

  /** Once the samples and the held capture are read, the choice a screen starts on: the held capture, else the first
   * sample. */
  get first(): string {
    return this.loaded ? (this.menuHeld?.choice ?? this.samples[0]?.id ?? '') : '';
  }

  /** The source a choice's records stand for, where it names one. */
  sourceOf(choice: string): string | undefined {
    if (heldSession(choice)) return this.held?.source;
    return this.samples.find((s) => s.id === choice)?.source;
  }

  /** The name `SampleMenu` shows for a choice. */
  name(choice: string): string {
    if (heldSession(choice)) return HELD_NAME;
    return this.samples.find((s) => s.id === choice)?.name ?? choice;
  }
}

/** What `SampleMenu` calls the held capture. */
export const HELD_NAME = 'Your capture';

/** The records a choice holds: the held capture's, or the saved sample's. */
export async function recordsOf(choice: string): Promise<SampleRecordDto[]> {
  const session = heldSession(choice);
  if (session) return capturedRecords(session);
  return (await getSample(choice)).sample?.records ?? [];
}
