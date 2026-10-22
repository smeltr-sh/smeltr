<!--
  One conversation with the person's own agent, or a new one: what it asked the leader and was answered, and the
  box to say more in, with the model and effort the next answer is made with. The Agent page and the agent panel
  both show it. A turn runs on the leader; this reads the conversation until the turn ends.
-->
<script module lang="ts">
  import { listAgentModels, refused as refusedCall, type AgentModelDto } from './api';

  /** The models are asked of the provider once a page load, by whichever view opens first. */
  let models: Promise<{ models: AgentModelDto[]; default: string } | null> | null = null;
  function modelsOnce() {
    models ??= listAgentModels()
      .then((got) => (refusedCall(got) ? null : got))
      .catch(() => null);
    return models;
  }
</script>

<script lang="ts">
  import ConversationEntry from './ConversationEntry.svelte';
  import ConversationEvent from './ConversationEvent.svelte';
  import Markdown from './Markdown.svelte';
  import Collapsible from './Collapsible.svelte';
  import ModelPicker from './ModelPicker.svelte';
  import ContextGauge from './ContextGauge.svelte';
  import { ask } from './Confirm.svelte';
  import { reported } from './errors.svelte';
  import InlineError from './InlineError.svelte';
  import {
    deleteConversation,
    getConversation,
    refused,
    restartConversation,
    sendToConversation,
    startConversation,
    stopConversation,
    type ConversationDto,
    type ConversationItem,
    type Effort,
  } from './api';
  import { session } from './session';

  let {
    open,
    onstarted,
    onchanged,
    ondeleted,
    compact = false,
  }: {
    /** The conversation shown; none starts a new one. */
    open?: string;
    /** A new conversation was started by the first message. */
    onstarted: (id: string) => void;
    /** A turn began or ended, so a list of conversations is out of date. */
    onchanged?: () => void;
    ondeleted?: () => void;
    /** Fewer words, for the panel. */
    compact?: boolean;
  } = $props();

  /** How often a running turn is read again, to show its answer as it is written. */
  const POLL_MS = 250;

  let shown = $state<ConversationDto | null>(null);
  let message = $state('');
  let busy = $state(false);
  let available = $state<AgentModelDto[]>([]);
  let fallback = $state('');
  let model = $state('');
  let effort = $state<Effort | ''>('');

  const me = $derived(session().displayName ?? '');

  $effect(() => {
    void modelsOnce().then((got) => {
      if (!got) return;
      available = got.models;
      fallback = got.default;
      model ||= got.default;
    });
  });

  async function read(id: string) {
    try {
      const got = await getConversation(id);
      if (refused(got)) return;
      shown = got.conversation;
    } catch (e) {
      reported(e);
    }
  }

  // A conversation's next answer starts from what made its last one.
  $effect(() => {
    shown = null;
    model = fallback;
    effort = '';
    if (open)
      void read(open).then(() => {
        if (shown?.model) model = shown.model;
        effort = shown?.effort ?? '';
      });
  });

  $effect(() => {
    if (!open || !shown?.running) return;
    const id = open;
    const timer = setInterval(() => {
      void read(id).then(() => {
        if (!shown?.running) onchanged?.();
      });
    }, POLL_MS);
    return () => clearInterval(timer);
  });

  async function send(e: SubmitEvent) {
    e.preventDefault();
    const text = message.trim();
    if (busy || !text) return;
    busy = true;
    const asked = { model: model || undefined, effort: effort || undefined };
    try {
      if (open) {
        const sent = await sendToConversation(open, text, asked);
        if (refused(sent)) return;
        message = '';
        await read(open);
      } else {
        const started = await startConversation(text, asked);
        if (refused(started)) return;
        message = '';
        onstarted(started.id);
      }
      onchanged?.();
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }

  async function stop() {
    if (!open) return;
    await stopConversation(open);
  }

  async function remove() {
    if (!open || !shown) return;
    const sure = await ask({
      title: 'Delete this conversation?',
      body: `“${shown.title}” and everything said in it are deleted for good.`,
      action: 'Delete',
      danger: true,
    });
    if (!sure) return;
    const deleted = await deleteConversation(open);
    if (refused(deleted)) return;
    ondeleted?.();
  }

  /** Summarize the conversation for the agent to carry on from, or clear what it reads. Clearing asks first: the
   * agent forgets it all. */
  async function restart(summarize: boolean) {
    if (!open) return;
    if (!summarize) {
      const sure = await ask({
        title: 'Clear the context?',
        body: 'Your agent forgets everything said so far.',
        action: 'Clear',
      });
      if (!sure) return;
    }
    try {
      if (refused(await restartConversation(open, summarize))) return;
      await read(open);
    } catch (e) {
      reported(e);
    }
  }

  /** Each call with the result it came back with, beside what was said. */
  type Shown =
    | { kind: 'message' | 'answer'; text: string }
    | { kind: 'call'; label: string; input: unknown; result?: { output: string; isError: boolean } }
    | { kind: 'unreadable'; error: string }
    | { kind: 'fresh'; summary: string | null };

  function arrange(items: ConversationItem[]): Shown[] {
    const results = new Map(
      items.flatMap((i) => (i.kind === 'result' ? [[i.callId, i] as const] : [])),
    );
    return items.flatMap((i): Shown[] => {
      switch (i.kind) {
        case 'message':
        case 'answer':
          return [{ kind: i.kind, text: i.text }];
        case 'call': {
          const query = new URLSearchParams(i.input.query ?? {}).toString();
          const label = `${i.input.method ?? '?'} ${i.input.path ?? '?'}${query ? `?${query}` : ''}`;
          return [{ kind: 'call', label, input: i.input.body, result: results.get(i.id) }];
        }
        case 'result':
          return [];
        case 'unreadable':
          return [{ kind: 'unreadable', error: i.error }];
        case 'fresh':
          return [{ kind: 'fresh', summary: i.summary }];
      }
    });
  }

  /** The status a result's output opens with, the answer's body after it. */
  function split(output: string): { status: string; body: string } {
    const cut = output.indexOf('\n');
    return cut < 0
      ? { status: '', body: output }
      : { status: output.slice(0, cut), body: output.slice(cut + 1) };
  }

  function pretty(text: string): string {
    try {
      return JSON.stringify(JSON.parse(text), null, 2);
    } catch {
      return text;
    }
  }
</script>

<section class="conversation" class:compact aria-label="Conversation">
  {#if open && shown}
    {#if !compact}
      <div class="head">
        <h2>{shown.title}</h2>
        <button type="button" class="compact" onclick={remove}>Delete conversation</button>
      </div>
    {/if}
    {#each arrange(shown.writing ? [...shown.items, { kind: 'answer', text: shown.writing }] : shown.items) as item, n (n)}
      {#if compact && (item.kind === 'message' || item.kind === 'answer')}
        <!-- A terminal's shape: what was said is the line after the prompt, and the answer the lines under it. -->
        <div class="line" class:said={item.kind === 'message'}>
          {#if item.kind === 'message'}<span class="prompt" aria-label={me}>›</span><span
              class="text">{item.text}</span
            >{:else}<Markdown text={item.text} />{/if}
        </div>
      {:else if item.kind === 'message' || item.kind === 'answer'}
        {@const who = item.kind === 'message' ? me : 'Agent'}
        {#if item.kind === 'message'}
          <ConversationEntry author={who} text={item.text}>
            {#snippet head()}<span class="who">{who}</span>{/snippet}
          </ConversationEntry>
        {:else}
          <ConversationEntry author={who}>
            {#snippet head()}<span class="who">{who}</span>{/snippet}
            <Markdown text={item.text} />
          </ConversationEntry>
        {/if}
      {:else if item.kind === 'call'}
        {@const answered = item.result ? split(item.result.output) : null}
        <div class="call">
          <Collapsible variant="row">
            {#snippet summary()}
              <span class="mono">{item.label}</span>
              {#if answered}
                <span class="status" class:failed={item.result?.isError}
                  >{answered.status || 'refused'}</span
                >
              {:else}
                <span class="dim later">{shown?.running ? 'running…' : 'not run'}</span>
              {/if}
            {/snippet}
            {#if item.input !== undefined}
              <h3>Sent</h3>
              <pre>{JSON.stringify(item.input, null, 2)}</pre>
            {/if}
            {#if answered}
              <h3>Answered</h3>
              <pre>{pretty(answered.body)}</pre>
            {/if}
          </Collapsible>
        </div>
      {:else if item.kind === 'fresh'}
        <ConversationEvent>
          {#if item.summary === null}
            Cleared. Your agent reads nothing above this.
          {:else}
            <Collapsible variant="row">
              {#snippet summary()}Summarized. Your agent reads this summary in place of what is
                above.{/snippet}
              <div class="summary"><Markdown text={item.summary} /></div>
            </Collapsible>
          {/if}
        </ConversationEvent>
      {:else if item.kind === 'unreadable'}
        <InlineError message={item.error} />
      {/if}
    {/each}
    {#if shown.running}
      <ConversationEvent>
        Your agent is working…
        <button type="button" class="compact" onclick={stop}>Stop</button>
      </ConversationEvent>
    {:else if shown.error}
      <InlineError message={shown.error} />
    {:else if shown.ended === 'rounds'}
      <ConversationEvent>
        Your agent stopped after 25 rounds of requests. Say how to go on.
      </ConversationEvent>
    {:else if shown.ended === 'stopped'}
      <ConversationEvent>Stopped.</ConversationEvent>
    {/if}
  {:else if !open}
    <p class="empty">Ask your agent about your pipelines, or to change something on a branch.</p>
  {/if}

  {#if (!open || shown) && compact}
    <form class="prompt-line" onsubmit={send}>
      <span class="prompt" aria-hidden="true">›</span>
      <textarea
        rows="1"
        aria-label="Message"
        placeholder="Ask your agent"
        bind:value={message}
        disabled={shown?.running}
        onkeydown={(e) => {
          // Enter sends, as a terminal's does; Shift+Enter starts another line.
          if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            e.currentTarget.form?.requestSubmit();
          }
        }}></textarea>
      {#if available.length > 0}
        <ModelPicker models={available} bind:model bind:effort />
      {/if}
      {#if shown?.context}
        <ContextGauge
          used={shown.context.used}
          window={shown.context.window}
          busy={shown.running}
          onrestart={restart}
        />
      {/if}
    </form>
  {:else if !open || shown}
    <ConversationEntry author={me || undefined}>
      <form class="composer" onsubmit={send}>
        <textarea
          rows={compact ? 2 : 3}
          aria-label="Message"
          placeholder="Ask your agent"
          bind:value={message}
          disabled={shown?.running}></textarea>
        <div class="actions">
          <span class="spacer"></span>
          {#if available.length > 0}
            <ModelPicker models={available} bind:model bind:effort />
          {/if}
          {#if shown?.context}
            <ContextGauge
              used={shown.context.used}
              window={shown.context.window}
              busy={shown.running}
              onrestart={restart}
            />
          {/if}
          <button
            type="submit"
            class="primary"
            disabled={busy || !message.trim() || shown?.running}
          >
            {busy ? 'Sending…' : 'Send'}
          </button>
        </div>
      </form>
    </ConversationEntry>
  {/if}
</section>

<style>
  .conversation {
    display: grid;
    gap: 0.75rem;
  }
  .conversation.compact {
    gap: 0.35rem;
    font-family: var(--mono);
    font-size: var(--text-note);
  }
  .compact .call {
    margin-left: 1.2rem;
  }
  .compact .empty {
    margin: 0;
  }
  .line {
    display: flex;
    gap: 0.5rem;
    white-space: pre-wrap;
    overflow-wrap: anywhere;
  }
  .line .text {
    min-width: 0;
  }
  .line.said {
    color: var(--ink);
    font-weight: 600;
  }
  .prompt {
    color: var(--accent);
    font-weight: 700;
    user-select: none;
  }
  .prompt-line {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    border-top: 1px dashed var(--border);
    padding-top: 0.35rem;
  }
  .prompt-line textarea {
    flex: 1 1 auto;
    min-width: 0;
    resize: none;
    border: 0;
    background: none;
    padding: 0.2rem 0;
    font-family: var(--mono);
    font-size: var(--text-note);
    color: var(--ink);
    field-sizing: content;
    max-height: 8rem;
  }
  .prompt-line textarea:focus {
    outline: none;
  }
  .head {
    display: flex;
    align-items: baseline;
    gap: 0.75rem;
  }
  .head h2 {
    flex: 1 1 auto;
    margin: 0;
    font-size: var(--text-title, 1.1rem);
  }
  .who {
    font-weight: 600;
    color: var(--ink);
  }
  .dim {
    color: var(--dim);
    font-size: var(--text-note);
    font-weight: 400;
  }
  .empty {
    color: var(--dim);
  }
  .summary {
    margin: 0.4rem 0 0;
  }
  .mono {
    font-family: var(--mono);
    overflow-wrap: anywhere;
  }
  .call {
    margin-left: 2.2rem;
  }
  .call h3 {
    font-size: var(--text-note);
    margin: 0.5rem 0 0.25rem;
    color: var(--dim);
  }
  .call pre {
    margin: 0;
    max-height: 20rem;
    overflow: auto;
    font-size: var(--text-note);
  }
  .later {
    margin-left: 0.5rem;
  }
  .status {
    margin-left: 0.5rem;
    font-family: var(--mono);
    font-size: var(--text-micro);
    border: 1px solid var(--border);
    border-radius: 999px;
    padding: 0 0.45rem;
    color: var(--dim);
  }
  .status.failed {
    color: var(--error);
  }
  .composer textarea {
    width: 100%;
    box-sizing: border-box;
    resize: vertical;
    margin-bottom: 0.5rem;
  }
  .composer .actions {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.5rem;
  }
  .spacer {
    flex: 1 1 auto;
  }
</style>
