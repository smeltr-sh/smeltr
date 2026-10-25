<!--
  A capture, from its form to its end. The form asks the same of every target: the group whose
  workers take it, a filter, and its bounds. Start hands it to the leader, which gives every worker in
  the group the same invitation on its next heartbeat and merges what comes back
  (`leader::capture`); `CaptureDialog` then holds it until it ends.

  The target is a source or a destination, picked here from the configuration; a page that opens
  this at one starts the form on it. Only the target and what the filter reads differ. Either is
  saved as a sample, which is where it and a paste become the same thing. A source's names its
  source, so it can be route-previewed; a destination's names none, since what a pipeline finished
  no route takes in.

  Closing this discards whatever session it still holds.
-->
<script lang="ts">
  import { reported, unreported } from './errors.svelte';
  import InlineError from './InlineError.svelte';
  import { onDestroy, onMount, untrack } from 'svelte';
  import { SvelteMap } from 'svelte/reactivity';
  import Dialog from './Dialog.svelte';
  import CaptureDialog from './CaptureDialog.svelte';
  import Field from './Field.svelte';
  import ValidatedField from './ValidatedField.svelte';
  import Advanced from './Advanced.svelte';
  import StartAt from './StartAt.svelte';
  import type { StartPosition } from './kafka';
  import { CaptureSession } from './captureSession.svelte';
  import {
    captureRequest,
    filterHelp,
    filterPlaceholder,
    type CaptureAt,
    type CaptureTarget,
  } from './capture';
  import { mintSampleId } from './ids';
  import { canEdit } from './access';
  import {
    sharedConfig,
    getConfig,
    configAt,
    currentBranch,
    currentScope,
    SHARED_SCOPE,
    getClusterStatus,
    saveCapture,
    errorText,
    MAX_SAMPLE_RECORDS,
    type CaptureStatus,
    type DestinationDto,
    type IdentityDto,
    type SourceDto,
    type WorkerGroupDto,
    type FileConfigDto,
    type AskAt,
    type CaptureFilterCheck,
    afterDot,
    checkCaptureFilter,
  } from './api';

  let {
    target,
    editing,
    use,
    oncancel,
    onback,
    onsaved,
  }: {
    /** The source or destination the form starts on, the scope that declares it, the group that
     * runs it, and the filter it starts with. Without it the form starts on the scope chip's first
     * target. */
    target?: { kind: CaptureAt; id: string; scope: string; group: string; filter?: string };
    /** A source as a form edits it, saved or not, in place of a target picked from the configuration: the
     * only one the form offers. */
    editing?: { source: SourceDto; identity?: IdentityDto; maxMatches: number };
    /** What a finished capture is for, in place of saving it as a sample: `take` is handed it and answers why it
     * could not use it, or nothing once it has, which closes the flow. */
    use?: { title: string; label: string; take: (status: CaptureStatus) => string | undefined };
    /** Cancel or Escape. */
    oncancel: () => void;
    /** A step back to what came before, in the form's header. */
    onback?: () => void;
    /** A capture, saved: the new sample's id. Without it, saving closes the flow. */
    onsaved?: (id: string) => void;
  } = $props();

  // Fixed for the life of this flow: the page opens a new one for another target.
  // svelte-ignore state_referenced_locally
  const given = target;
  // svelte-ignore state_referenced_locally
  const edited = editing;
  /** The scope whose sources and destinations the form offers. */
  const scope = given?.scope ?? currentScope();

  let error = $state('');
  let busy = $state(false);

  let sources: SourceDto[] = $state(edited ? [edited.source] : []);
  let destinations: DestinationDto[] = $state([]);
  let identities: IdentityDto[] = $state(edited?.identity ? [edited.identity] : []);
  /** What `shared/` declares: a source in a group scope may legally name one of these. Never saved from here. */
  let sharedIdentities: IdentityDto[] = $state([]);
  /** The picked target, as `targetKey` writes it. */
  let selected = $state(
    edited ? targetKey('source', edited.source.id) : given ? targetKey(given.kind, given.id) : '',
  );
  /** The commit `given`'s scope was read at: a save is made against it, as one through the scope chip is. */
  let readAt: string | null = null;
  let groups: WorkerGroupDto[] = $state([]);
  let group = $state(given?.group ?? (scope === SHARED_SCOPE ? '' : scope));
  let filter = $state(given?.filter ?? '');
  let maxMatches = $state(edited?.maxMatches ?? 100);
  let deadlineSecs = $state(30);
  /** Where a stream source's capture starts; absent is the latest. */
  let start: StartPosition | undefined = $state();

  // A lost session closes the capture dialog, and the form says why.
  const capture = new CaptureSession((e) => (error = e));
  /** The target the running session was started at, so a change to the form
   * while the dialog is up cannot rename what it saves. */
  let sessionTarget = $state<{ kind: CaptureAt; id: string } | null>(null);

  onDestroy(() => capture.destroy());

  // Apart, so a cluster that does not answer still leaves the sources to pick from.
  onMount(() => {
    if (!edited) void loadTargets();
    void loadGroups();
  });

  /** One value per target in the picker: a source and a destination may share an id. */
  function targetKey(kind: CaptureAt, id: string): string {
    return `${kind}:${id}`;
  }

  async function loadTargets() {
    try {
      const config = given ? await configAt(currentBranch(), given.scope) : await getConfig();
      if (given) readAt = config.commit ?? null;
      if (config.config) {
        loaded = config.config;
        // A source's sample is saved in its own scope, so only one its editor may save to is offered.
        sources = scope !== SHARED_SCOPE || canEdit(SHARED_SCOPE) ? config.config.sources : [];
        destinations = config.config.destinations;
        identities = config.config.identities;
        sharedIdentities = (await sharedConfig())?.identities ?? [];
        const keys = [
          ...sources.map((s) => targetKey('source', s.id)),
          ...destinations.map((d) => targetKey('destination', d.id)),
        ];
        if (given && !keys.includes(selected)) {
          // The Map draws what it read; a target since removed or renamed is not another one to capture.
          selected = '';
          error = `Can't find ${given.id} in ${given.scope}; it may have been renamed or removed`;
        } else if (!keys.includes(selected)) selected = keys[0] ?? '';
      }
    } catch (e) {
      reported(e);
    }
  }

  async function loadGroups() {
    try {
      const cluster = await getClusterStatus();
      // A group's own sources and destinations run only in that group.
      groups = (cluster.workerGroups ?? []).filter((g) => scope === SHARED_SCOPE || g.id === scope);
      if (!groups.some((g) => g.id === group)) group = groups[0]?.id ?? '';
    } catch (e) {
      reported(e);
    }
  }

  let captureSource = $derived(sources.find((s) => targetKey('source', s.id) === selected) ?? null);
  let captureDestination = $derived(
    destinations.find((d) => targetKey('destination', d.id) === selected) ?? null,
  );
  /** The bundle the targets were read from. Absent for a source a form is editing. */
  let loaded: FileConfigDto | null = $state(null);
  /** Asks the engine what may follow a dot in the filter: over the bundle the targets came from, or over the edited
   * source alone. */
  const askAt: AskAt = (at) => (text, dot) =>
    afterDot({
      config: JSON.stringify(
        loaded ?? {
          version: 1,
          sources,
          destinations: [],
          routes: [],
          pipelines: [],
          identities: [...identities, ...sharedIdentities],
        },
      ),
      at,
      text,
      dot,
    });
  // What the filter makes of each route delivering to the destination, by destination and text: a route it does not
  // compile against is passed over, as a worker passes it over. Asked once the text has sat still a moment, and kept
  // under what it was asked for, so an answer arriving after a later keystroke cannot land on the wrong text.
  let checks = new SvelteMap<string, CaptureFilterCheck | null>();
  let checkKey = $derived(captureDestination ? `${captureDestination.id}\n${filter.trim()}` : '');
  $effect(() => {
    const key = checkKey;
    const dest = captureDestination?.id;
    const text = filter.trim();
    if (!dest || !loaded || text === '' || untrack(() => checks.has(key))) return;
    const config = JSON.stringify(loaded);
    const timer = setTimeout(() => {
      checkCaptureFilter(config, text, dest)
        .then((c) => checks.set(key, c))
        .catch(() => {});
    }, 400);
    return () => clearTimeout(timer);
  });
  let check = $derived(checks.get(checkKey) ?? null);
  let skipErrors = $derived(check?.skips.map((s) => s.error) ?? []);
  let readsNone = $derived(!!check && check.reads === 0 && check.skips.length > 0);
  /** One line for each of `skips`, `line` spelling it, to the first few; past them, how many more. */
  function skipLines(
    skips: CaptureFilterCheck['skips'],
    line: (route: string, error: string) => string,
  ): string {
    const shown = skips.slice(0, SKIPS_SHOWN).map((s) => line(s.route, s.error));
    const more = skips.length - shown.length;
    return [
      ...shown,
      ...(more > 0 ? [`…and ${more} more ${more === 1 ? 'route' : 'routes'}.`] : []),
    ].join('\n');
  }
  const SKIPS_SHOWN = 3;
  let filterError = $derived(
    readsNone
      ? `No route can use this filter, so nothing would be captured.\n${skipLines(check!.skips, (r, e) => `Route ${r}: ${e}.`)}`
      : null,
  );
  let filterWarning = $derived(
    !readsNone && check && check.skips.length > 0
      ? skipLines(check.skips, (r, e) => `Records from route ${r} are not captured: ${e}.`)
      : null,
  );
  /** What the filter reads: a source's record, or a destination's row. */
  let at: CaptureAt = $derived(captureDestination ? 'destination' : 'source');
  // Sent with the source rather than named: a worker follows main, and this
  // branch is where the identity was written.
  let identity = $derived(
    captureSource && 'identity' in captureSource && captureSource.identity !== undefined
      ? (identities.find((i) => i.id === captureSource.identity) ??
          sharedIdentities.find((i) => i.id === captureSource.identity) ??
          null)
      : null,
  );

  /** Only a stream has a position to start from. */
  let streams = $derived(captureSource?.type === 'kafka' || captureSource?.type === 'eventHubs');

  let captureTarget = $derived<CaptureTarget | null>(
    captureDestination
      ? { destination: captureDestination.id }
      : captureSource
        ? { source: captureSource, identity: identity ?? undefined }
        : null,
  );
  let canStart = $derived(!!captureTarget && !!group);

  async function begin() {
    if (busy || !captureTarget || !group) return;
    busy = true;
    error = '';
    // Before the start: the capture dialog reads it for its default name as it opens.
    sessionTarget = captureDestination
      ? { kind: 'destination', id: captureDestination.id }
      : captureSource && { kind: 'source', id: captureSource.id };
    await capture.start(
      captureRequest(captureTarget, {
        group,
        filter,
        maxMatches,
        deadlineSecs,
        start: streams ? start : undefined,
      }),
    );
    busy = false;
  }

  /** End the capture where it stands. The dialog then offers what it holds. */
  async function stop() {
    busy = true;
    error = '';
    await capture.stop();
    busy = false;
  }

  async function save(sampleName: string) {
    const session = capture.session;
    if (!session || !sampleName) return;
    busy = true;
    error = '';
    try {
      const id = mintSampleId();
      // A source's sample goes where the source is declared, so a route preview can find it; a
      // destination's, which no route takes in, goes to the scope chip's.
      const source = sessionTarget?.kind === 'source' ? sessionTarget.id : undefined;
      const res = await saveCapture(
        session,
        id,
        sampleName,
        source,
        source && given ? readAt : undefined,
        source ? given?.scope : undefined,
      );
      if (!res.ok) {
        error = errorText(res.errors) || 'could not save';
        return;
      }
      capture.release();
      if (onsaved) onsaved(id);
      else oncancel();
    } catch (e) {
      error = unreported(e);
    } finally {
      busy = false;
    }
  }

  /** Hand the finished capture to `use`, which closes the flow once it has taken it. */
  async function take() {
    if (!use || !capture.status) return;
    error = use.take(capture.status) ?? '';
    if (error) return;
    await capture.discard();
    oncancel();
  }

  /** Back to the form, to change it and start again or to cancel. */
  async function discard() {
    error = '';
    await capture.discard();
  }
</script>

<Dialog
  open={!capture.session}
  title={use?.title ??
    (captureDestination ? `capture at ${captureDestination.id}` : 'capture a sample')}
  wide
  oncancel={() => busy || oncancel()}
  {onback}
  onsubmit={() => void begin()}
>
  <div class="form-body">
    {#if sources.length === 0 && destinations.length === 0}
      <p class="dim">No sources are configured yet.</p>
    {:else}
      <Field
        label="Group"
        error={groups.length === 0
          ? "No worker groups yet — a capture is asked of a group's workers."
          : null}
      >
        <select bind:value={group}>
          {#each groups as g (g.id)}
            <option value={g.id}>{g.name}</option>
          {/each}
        </select>
      </Field>
      <Field label="Capture target">
        <!-- A start was chosen for the source it was chosen on. -->
        <select bind:value={selected} onchange={() => (start = undefined)}>
          {#if sources.length > 0}
            <optgroup label="Sources">
              {#each sources as s (s.id)}
                <option value={targetKey('source', s.id)}>{s.id}</option>
              {/each}
            </optgroup>
          {/if}
          {#if destinations.length > 0}
            <optgroup label="Destinations">
              {#each destinations as d (d.id)}
                <option value={targetKey('destination', d.id)}>{d.id}</option>
              {/each}
            </optgroup>
          {/if}
        </select>
      </Field>
      <Field optional label="Keep records where" error={filterError} warning={filterWarning}>
        {#snippet help()}
          {#each filterHelp(at) as part, i (i)}{#if part.code}<code>{part.text}</code
              >{:else}{part.text}{/if}{/each} Leave it empty to keep everything.
        {/snippet}
        <ValidatedField
          bind:value={filter}
          errors={readsNone ? skipErrors : []}
          warnings={readsNone ? [] : skipErrors}
          placeholder={filterPlaceholder(at)}
          autocomplete="condition"
          ask={captureSource
            ? askAt({ source: captureSource.id })
            : captureDestination
              ? askAt({ destination: captureDestination.id })
              : undefined}
          wrap
        />
      </Field>
      <Field label="At most" group>
        <div class="limits">
          <input
            type="number"
            min="1"
            max={MAX_SAMPLE_RECORDS}
            bind:value={maxMatches}
            class="narrow"
            aria-label="records"
          />
          <span class="dim">records, for at most</span>
          <input
            type="number"
            min="1"
            bind:value={deadlineSecs}
            class="narrow"
            aria-label="seconds"
          />
          <span class="dim">seconds</span>
        </div>
      </Field>
      {#if streams}
        <Advanced modified={start !== undefined}>
          <StartAt bind:start label="Start at" fallback="latest">
            {#snippet help()}
              Where each partition starts. Every worker in the group reads the source for the
              capture, whether or not it runs it, and nothing it reads is committed.
            {/snippet}
          </StartAt>
        </Advanced>
      {/if}
    {/if}
    <InlineError message={error} />
  </div>
  {#snippet actions()}
    <button type="button" onclick={oncancel} disabled={busy}>Cancel</button>
    <button type="submit" class="primary" data-guide="capture-start" disabled={busy || !canStart}>
      {busy ? 'Starting…' : 'Start'}
    </button>
  {/snippet}
</Dialog>

{#if capture.session}
  <CaptureDialog
    status={capture.status}
    source={sessionTarget?.kind === 'source' ? sessionTarget.id : undefined}
    destination={sessionTarget?.kind === 'destination' ? sessionTarget.id : undefined}
    {busy}
    {error}
    onstop={stop}
    onsave={save}
    use={use && { title: use.title, label: use.label, take }}
    ondiscard={discard}
  />
{/if}

<style>
  .form-body {
    display: flex;
    flex-direction: column;
    gap: 0.7rem;
  }
  .limits {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.5rem;
  }
  input,
  select {
    width: 100%;
    box-sizing: border-box;
  }
  input.narrow {
    width: 5rem;
  }
</style>
