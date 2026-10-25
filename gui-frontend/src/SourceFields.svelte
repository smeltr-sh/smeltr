<!--
  One source's form, by its type: every field it takes but its id. The Sources page draws it in each row and in its
  add dialog.
-->
<script lang="ts">
  import Advanced from './Advanced.svelte';
  import IdentityPicker from './IdentityPicker.svelte';
  import Field from './Field.svelte';
  import Nested from './Nested.svelte';
  import StartAt from './StartAt.svelte';
  import UrlField from './UrlField.svelte';
  import JsonField from './JsonField.svelte';
  import KafkaPropsEditor from './KafkaPropsEditor.svelte';
  import KafkaSecurity from './KafkaSecurity.svelte';
  import SecretRefInput from './SecretRefInput.svelte';
  import ConnectorAccess, { accessModified } from './ConnectorAccess.svelte';
  import ListingFields, { listingModified } from './ListingFields.svelte';
  import TokenListInput from './TokenListInput.svelte';
  import PropsEditor from './PropsEditor.svelte';
  import ReceiverTokens from './ReceiverTokens.svelte';
  import ReceiverSourcetypes from './ReceiverSourcetypes.svelte';
  import LineBreakerField from './LineBreakerField.svelte';
  import SqlSourceFields from './SqlSourceFields.svelte';
  import AvroRead from './AvroRead.svelte';
  import BytesInput from './BytesInput.svelte';
  import { headerValueRefusal } from './http';
  import {
    currentScope,
    SHARED_SCOPE,
    LIMITS,
    type SourceDto,
    type IdentityDto,
    type SecretDto,
    type ApiRecords,
    type OtlpSignal,
    type ListStart,
    type Framing,
  } from './api';
  import { brokersWarning, SECTION_PROPS } from './kafka';
  import { fromLocalInput, toLocalInput } from './time';
  import { numberOrUnset } from './page';
  import { byteCount, exactBytes } from './bytes';
  import {
    DEFAULT_LEVEL,
    LEVELS,
    LISTED,
    LISTING_STORE,
    OBJECT_STORE,
    isListing,
    isObjectSource,
    isReceiver,
    isSql,
    isStream,
    setFraming,
    type ApiSource,
    type ListingSource,
    type ObjectSource,
    type ReceiverSource,
    type StreamSource,
  } from './sources';

  let {
    source,
    identities,
    secrets,
    mechanisms,
    startMoves = () => false,
    readAt,
    onsaved,
  }: {
    source: SourceDto;
    /** Every identity the source may name. */
    identities: IdentityDto[];
    /** The secret catalog, so a credential can be pointed at one the leader holds. */
    secrets: SecretDto[];
    /** The SASL mechanisms this leader's build can select. */
    mechanisms: string[];
    /** Whether a stream source's start differs from the saved one, so deploying it moves the source. */
    startMoves?: (s: StreamSource) => boolean;
    /** The commit the sources were read at, for a database source's sample. */
    readAt?: string | null;
    /** Told the commit a database source's sample was saved at. */
    onsaved?: (commit: string) => void;
  } = $props();

  /** Properties set by hand: the security section's own are its business,
   * and Advanced's marker speaks only for what Advanced hides. */
  function hasProps(s: SourceDto): boolean {
    if (s.type !== 'kafka') return false;
    const byHand = (m: Record<string, unknown> | undefined) =>
      Object.keys(m ?? {}).filter((k) => !SECTION_PROPS.includes(k)).length > 0;
    return byHand(s.properties) || byHand(s.credentials);
  }

  /** Drives the Advanced header's marker (STYLE_GUIDE, "Advanced
   * settings"): a collapsed section must say when it hides a live
   * setting. */
  function streamModified(s: StreamSource): boolean {
    return (
      hasProps(s) ||
      (s.type === 'kafka' && (s.groupId ?? '') !== '') ||
      (s.type === 'eventHubs' && accessModified('eventHubs', s)) ||
      byteCount(s.fetchBytes, LIMITS.fetchBytes.default) !== LIMITS.fetchBytes.default ||
      (s.encoding ?? 'auto') !== 'auto'
    );
  }

  /** As `streamModified`, for a listing source. */
  function listingSourceModified(s: ListingSource): boolean {
    return (
      accessModified(LISTING_STORE[s.type], s) ||
      listingModified(s) ||
      (s.encoding ?? 'auto') !== 'auto'
    );
  }

  /** As `streamModified`, for an API source. */
  function apiModified(s: ApiSource): boolean {
    return (
      s.identity !== undefined ||
      s.start !== undefined ||
      (s.pollIntervalSecs ?? LIMITS.pollIntervalSecs.default) !== LIMITS.pollIntervalSecs.default ||
      (s.type === 'httpApi' && (s.sinceOverlapSecs ?? 0) !== 0) ||
      ((s.type === 'oktaLogs' || s.type === 'httpApi') && s.keyId !== undefined) ||
      (s.type === 'httpApi' && s.scope !== undefined)
    );
  }

  /** As `streamModified`, for a receiver. */
  function receiverModified(s: ReceiverSource): boolean {
    return (
      s.identity !== undefined ||
      (s.maxRequestBytes ?? LIMITS.receiverRequestBytes.default) !==
        LIMITS.receiverRequestBytes.default ||
      (s.type === 'syslogReceiver' && (s.encoding ?? 'auto') !== 'auto') ||
      (s.type === 'syslogReceiver' && s.lineBreaker !== undefined) ||
      (s.type === 'hecReceiver' && s.sourcetypes !== undefined)
    );
  }

  /** How an Okta source signs in: as a service app or with an API token. */
  type OktaSignIn = 'serviceApp' | 'token';
  const oktaSignIn = (s: Extract<SourceDto, { type: 'oktaLogs' }>): OktaSignIn =>
    s.token !== undefined ? 'token' : 'serviceApp';
  /** Switching keeps nothing of the other way, which the load refuses beside it. */
  function setOktaSignIn(s: SourceDto, how: OktaSignIn) {
    if (s.type !== 'oktaLogs') return;
    if (how === 'token') {
      delete s.clientId;
      delete s.privateKey;
      delete s.keyId;
      s.token = '';
    } else {
      delete s.token;
      s.clientId = '';
      s.privateKey = '';
    }
  }

  /** How an HTTP API source signs in: with only its headers, or as an OAuth app proven by a secret or a key. */
  type HttpSignIn = 'none' | 'secret' | 'key';
  type HttpApiSource = Extract<SourceDto, { type: 'httpApi' }>;
  const httpSignIn = (s: HttpApiSource): HttpSignIn =>
    s.privateKey !== undefined
      ? 'key'
      : s.clientId !== undefined || s.clientSecret !== undefined
        ? 'secret'
        : 'none';
  /** Switching keeps only what both ways take, so the load never sees a secret beside a key. */
  function setHttpSignIn(s: SourceDto, how: HttpSignIn) {
    if (s.type !== 'httpApi') return;
    if (how === 'none') {
      for (const k of [
        'clientId',
        'clientSecret',
        'privateKey',
        'keyId',
        'tokenUrl',
        'issuer',
        'scope',
      ] as const)
        delete s[k];
      return;
    }
    s.clientId ??= '';
    if (s.tokenUrl === undefined && s.issuer === undefined) s.tokenUrl = '';
    if (how === 'secret') {
      delete s.privateKey;
      delete s.keyId;
      s.clientSecret ??= '';
    } else {
      delete s.clientSecret;
      s.privateKey ??= '';
    }
  }
  /** Where an HTTP API source's token comes from: a token URL given, or the issuer's discovery. */
  type TokenFrom = 'url' | 'issuer';
  const tokenFrom = (s: HttpApiSource): TokenFrom => (s.issuer !== undefined ? 'issuer' : 'url');
  function setTokenFrom(s: SourceDto, from: TokenFrom) {
    if (s.type !== 'httpApi') return;
    if (from === 'issuer') {
      delete s.tokenUrl;
      s.issuer ??= '';
    } else {
      delete s.issuer;
      s.tokenUrl ??= '';
    }
  }

  /** As `streamModified`, for an object source. */
  function objectsModified(s: ObjectSource): boolean {
    return (
      accessModified(OBJECT_STORE[s.type], s) ||
      (s.type === 'gcsPubSub' && s.pubsubEndpoint !== undefined) ||
      (s.type === 'azureBlobQueue' &&
        (s.maxDeliveries ?? LIMITS.maxDeliveries.default) !== LIMITS.maxDeliveries.default) ||
      (s.encoding ?? 'auto') !== 'auto' ||
      (s.visibilitySecs ?? LIMITS.visibilitySecs.default) !== LIMITS.visibilitySecs.default
    );
  }

  /** A whole number within `limit`'s bounds, or unset when the input is empty. */
  function bounded(
    input: HTMLInputElement,
    limit: { min: number; max: number },
  ): number | undefined {
    const n = numberOrUnset(input);
    return n === undefined ? undefined : Math.min(limit.max, Math.max(limit.min, Math.round(n)));
  }

  /** The consumer group a Kafka source with a blank group id reads as. */
  function defaultGroupId(): string {
    const scope = currentScope();
    return scope === SHARED_SCOPE ? 'smeltr.<worker group>' : `smeltr.${scope}`;
  }

  const stopKind = (s: SourceDto): string => (isStream(s) ? (s.stop?.type ?? 'never') : 'never');

  /** `never` is the default, so choosing it writes nothing. A time stops at
   * now. */
  function setStop(source: SourceDto, kind: string) {
    if (!isStream(source)) return;
    if (kind === 'never') source.stop = undefined;
    else source.stop = { type: 'timestamp', millis: Date.now() };
  }
</script>

<!-- A stream, object or syslog source's encoding: each names a label the same way. -->
{#snippet encodingField(
  source:
    StreamSource | ObjectSource | ListingSource | Extract<SourceDto, { type: 'syslogReceiver' }>,
)}
  <!-- Avro's strings are UTF-8: the load refuses an encoding beside it. -->
  {#if !('format' in source && source.format === 'avro')}
    <Field field="encoding" label="encoding">
      {#snippet help()}
        How each record's bytes become <code>_raw</code>'s text. <code>auto</code> guesses, and
        marks the row when it guessed. <code>none</code> decodes nothing, and the record is
        <code>__source.bytes</code> alone.
      {/snippet}
      <input
        type="text"
        placeholder="auto"
        value={source.encoding ?? ''}
        oninput={(e) => (source.encoding = e.currentTarget.value.trim() || undefined)}
      />
    </Field>
  {/if}
{/snippet}

<!-- How an object or listing source reads what it reads, and cuts text into records: each names it the same way. -->
{#snippet framingField(source: ObjectSource | ListingSource)}
  <AvroRead value={source} stream={false} {secrets} {identities} />
  <!-- An Avro file is its own records: the load refuses a framing beside it. -->
  {#if source.format !== 'avro'}
    <Field field="framing" label="framing">
      {#snippet help()}
        <code>lines</code>: one record per line, or per event where a line breaker cuts them.
        <code>object</code>: the whole object is one record, for a JSON document a pipeline unrolls.
        <code>escapedLines</code>: one record per line, unescaped as a <code>bytes</code> object destination
        wrote it.
      {/snippet}
      <select
        value={source.framing ?? 'lines'}
        onchange={(e) => setFraming(source, e.currentTarget.value as Framing)}
      >
        <option value="lines">lines</option>
        <option value="object">object</option>
        <option value="escapedLines">escapedLines</option>
      </select>
    </Field>
    {#if (source.framing ?? 'lines') === 'lines'}
      <Nested>
        <LineBreakerField
          value={source.lineBreaker}
          placeholder={'([\\r\\n]+)\\d{4}-'}
          onchange={(v) => (source.lineBreaker = v)}
        />
      </Nested>
    {/if}
  {/if}
{/snippet}

<!-- A listing source's own fields: where it lists, how it signs in, and what it does with what it reads. -->
{#snippet listingFields(source: ListingSource)}
  {@const store = LISTING_STORE[source.type]}
  {@const listed = LISTED[source.type]}
  {#if source.type === 'azureBlobList'}
    <Field field="account" label="account">
      {#snippet help()}
        The storage account holding the container.
      {/snippet}
      <input type="text" bind:value={source.account} />
    </Field>
    <Field field="container" label="container">
      {#snippet help()}
        The container's name, alone.
      {/snippet}
      <input type="text" bind:value={source.container} />
    </Field>
  {:else if source.type === 'sftp'}
    <ConnectorAccess {store} value={source} show="credential" {secrets} />
    <Field field="directory" label="directory">
      {#snippet help()}
        The directory it reads: absolute, or under the user's home directory. Empty is the home
        directory.
      {/snippet}
      <input
        type="text"
        value={source.directory ?? ''}
        oninput={(e) => {
          if (source.type === 'sftp') source.directory = e.currentTarget.value || undefined;
        }}
        placeholder="the user's home"
      />
    </Field>
  {:else}
    <Field field="bucket" label="bucket">
      {#snippet help()}
        The bucket's name, alone.
      {/snippet}
      <input type="text" bind:value={source.bucket} />
    </Field>
  {/if}
  {#if source.type !== 'sftp'}
    <ConnectorAccess {store} value={source} show="credential" {secrets} />
  {/if}
  <ListingFields value={source} noun={listed.noun} within={listed.within} show="main" />
  {@render framingField(source)}
  {#if source.type !== 'sftp'}
    <IdentityPicker
      value={source.identity}
      {identities}
      farEnd="source"
      onchange={(v) => (source.identity = v)}
    />
  {/if}
  <Advanced label="Advanced" modified={listingSourceModified(source)}>
    <ConnectorAccess {store} value={source} show="advanced" />
    <ListingFields value={source} noun={listed.noun} within={listed.within} show="advanced" />
    {@render encodingField(source)}
  </Advanced>
{/snippet}

<!-- What every API source takes under Advanced: who it connects as, what its first poll asks for, and how often. -->
{#snippet apiAdvanced(source: ApiSource)}
  <IdentityPicker
    value={source.identity}
    {identities}
    farEnd="source"
    onchange={(v) => (source.identity = v)}
  />
  <!-- Without a since parameter every poll reads everything, so there is no first poll to start from. -->
  {#if source.type === 'oktaLogs' || source.sinceParam !== undefined}
    <Field field="start" label="start at">
      {#snippet help()}
        What its first poll asks for: <code>everything</code> the API lists, or only what happens
        from the first poll on, <code>new</code>. Later polls resume where the last one every
        destination took left off.
      {/snippet}
      <select
        value={source.start ?? 'everything'}
        onchange={(e) => {
          const v = e.currentTarget.value as ListStart;
          source.start = v === 'everything' ? undefined : v;
        }}
      >
        <option value="everything">everything</option>
        <option value="new">new</option>
      </select>
    </Field>
  {/if}
  <Field field="pollIntervalSecs" label="poll interval seconds">
    {#snippet help()}
      Seconds from the start of one poll to the start of the next.
    {/snippet}
    <input
      type="number"
      min={LIMITS.pollIntervalSecs.min}
      value={source.pollIntervalSecs ?? LIMITS.pollIntervalSecs.default}
      oninput={(e) => (source.pollIntervalSecs = bounded(e.currentTarget, LIMITS.pollIntervalSecs))}
    />
  </Field>
{/snippet}

<!-- An HTTP API source's own fields: what it asks, how it signs in, and where the answer holds its records. -->
{#snippet httpApiFields(source: Extract<SourceDto, { type: 'httpApi' }>)}
  <UrlField
    field="url"
    label="url"
    bind:value={source.url}
    sends={Object.keys(source.credentials ?? {}).length > 0 || httpSignIn(source) !== 'none'}
    placeholder="https://api.example.com/v1/events"
  >
    {#snippet help()}
      The whole address it asks, with any fixed query parameters.
    {/snippet}
  </UrlField>
  <Field field="method" label="method">
    {#snippet help()}
      How it asks: a GET, or a POST of a fixed JSON body, for an API that is asked by what it is
      sent.
    {/snippet}
    <select
      value={source.method ?? 'GET'}
      onchange={(e) => {
        const post = e.currentTarget.value === 'POST';
        source.method = post ? 'POST' : undefined;
        if (!post) source.body = undefined;
      }}
    >
      <option value="GET">GET</option>
      <option value="POST">POST</option>
    </select>
  </Field>
  {#if source.method === 'POST'}
    <Nested>
      <JsonField
        field="body"
        label="body"
        value={source.body}
        set={(v) => {
          if (source.type === 'httpApi') source.body = v;
        }}
        placeholder={'{"batchSize": 1000}'}
      >
        {#snippet help()}
          The JSON every request sends. Empty sends nothing.
        {/snippet}
      </JsonField>
    </Nested>
  {/if}
  <PropsEditor
    value={source}
    literalKey="headers"
    labels={{
      literal: 'headers',
      credentials: 'credentials',
      noun: 'header',
      literalName: 'Accept',
      literalValue: 'application/json',
      credentialName: 'Authorization',
    }}
    {secrets}
    refusal={(_key, _credential, val) => (val === undefined ? undefined : headerValueRefusal(val))}
  >
    {#snippet literalHelp()}
      Sent with every request, by name and value, committed to the config repo.
    {/snippet}
    {#snippet credentialsHelp()}
      A header whose value is secret, such as <code>Authorization</code>: the secret holds its whole
      value, <code>Bearer &lt;token&gt;</code> included. The value never reaches git.
    {/snippet}
  </PropsEditor>
  {@render httpSignInFields(source)}
  <Field field="records" label="records">
    {#snippet help()}
      Where an answer's records are: the whole answer is one record, each line is one, or each
      element of a JSON array is one.
    {/snippet}
    <select
      value={source.records ?? 'body'}
      onchange={(e) => {
        const v = e.currentTarget.value as ApiRecords;
        source.records = v === 'body' ? undefined : v;
        if (v !== 'array') source.recordsAt = undefined;
      }}
    >
      <option value="body">body &mdash; the whole answer</option>
      <option value="lines">lines &mdash; one per line</option>
      <option value="array">array &mdash; one per element</option>
    </select>
  </Field>
  {#if source.records === 'array'}
    <Nested>
      <Field field="recordsAt" label="records at">
        {#snippet help()}
          The JSON pointer to the array, such as <code>/data</code>. Empty is an answer that is the
          array.
        {/snippet}
        <input
          type="text"
          value={source.recordsAt ?? ''}
          oninput={(e) => {
            if (source.type === 'httpApi')
              source.recordsAt = e.currentTarget.value.trim() || undefined;
          }}
          placeholder="/data"
        />
      </Field>
    </Nested>
  {/if}
  <Field field="followLinks" label="pages">
    {#snippet help()}
      Whether a poll follows each answer's <code>Link: rel="next"</code> to the next page, on the same
      host only.
    {/snippet}
    <select
      value={source.followLinks ? 'follow' : 'one'}
      onchange={(e) => (source.followLinks = e.currentTarget.value === 'follow' || undefined)}
    >
      <option value="one">one page per poll</option>
      <option value="follow">follow next links</option>
    </select>
  </Field>
  <Field field="sinceParam" label="since parameter">
    {#snippet help()}
      The query parameter that asks for what happened since a time. Each poll sends it the start of
      the last poll every destination took. Empty reads everything every poll.
    {/snippet}
    <input
      type="text"
      value={source.sinceParam ?? ''}
      oninput={(e) => {
        if (source.type !== 'httpApi') return;
        source.sinceParam = e.currentTarget.value.trim() || undefined;
        if (source.sinceParam === undefined) {
          source.sinceOverlapSecs = undefined;
          source.start = undefined;
        }
      }}
      placeholder="since"
    />
  </Field>
  <Advanced label="Advanced" modified={apiModified(source)}>
    {#if httpSignIn(source) !== 'none'}
      {#if httpSignIn(source) === 'key'}
        <Field field="keyId" label="key id">
          {#snippet help()}
            The <code>kid</code> naming the private key, for a client with several keys.
          {/snippet}
          <input
            type="text"
            value={source.keyId ?? ''}
            oninput={(e) => {
              if (source.type === 'httpApi')
                source.keyId = e.currentTarget.value.trim() || undefined;
            }}
          />
        </Field>
      {/if}
      <Field field="scope" label="scope">
        {#snippet help()}
          The scope the client's token is asked for.
        {/snippet}
        <input
          type="text"
          value={source.scope ?? ''}
          oninput={(e) => {
            if (source.type === 'httpApi') source.scope = e.currentTarget.value.trim() || undefined;
          }}
          placeholder="read:events"
        />
      </Field>
    {/if}
    {#if source.sinceParam !== undefined}
      <Field field="sinceOverlapSecs" label="overlap seconds">
        {#snippet help()}
          How far before the last poll's start the since parameter asks from, for an API that
          records events late. What falls in the overlap is read twice.
        {/snippet}
        <input
          type="number"
          min="0"
          value={source.sinceOverlapSecs ?? 0}
          oninput={(e) => {
            if (source.type !== 'httpApi') return;
            const n = numberOrUnset(e.currentTarget);
            source.sinceOverlapSecs = n === undefined || n <= 0 ? undefined : Math.round(n);
          }}
        />
      </Field>
    {/if}
    {@render apiAdvanced(source)}
  </Advanced>
{/snippet}

<!-- How an HTTP API source signs in as an OAuth app: which client, its proof, and where its token comes from. -->
{#snippet httpSignInFields(source: HttpApiSource)}
  {@const how = httpSignIn(source)}
  <Field label="sign in">
    {#snippet help()}
      Signs in as an OAuth app for a token sent on every request. A private key never leaves smeltr;
      a secret is sent to the provider.
    {/snippet}
    <select
      value={how}
      onchange={(e) => setHttpSignIn(source, e.currentTarget.value as HttpSignIn)}
    >
      <option value="none">none</option>
      <option value="secret">client secret</option>
      <option value="key">private key</option>
    </select>
  </Field>
  {#if how !== 'none'}
    {@const from = tokenFrom(source)}
    <Nested>
      <Field field="clientId" label="client id">
        {#snippet help()}
          The OAuth client it signs in as.
        {/snippet}
        <input
          type="text"
          value={source.clientId ?? ''}
          oninput={(e) => {
            if (source.type === 'httpApi') source.clientId = e.currentTarget.value.trim();
          }}
          placeholder="0oa1b2c3d4"
        />
      </Field>
      {#if how === 'secret'}
        <Field field="clientSecret" label="client secret">
          {#snippet help()}
            Where the OAuth client's secret lives. The value never reaches git.
          {/snippet}
          <SecretRefInput
            value={source.clientSecret}
            {secrets}
            label="client secret"
            onchange={(v) => {
              if (source.type === 'httpApi') source.clientSecret = v ?? '';
            }}
          />
        </Field>
      {:else}
        <Field field="privateKey" label="private key">
          {#snippet help()}
            Where the client's RSA private key lives, as PEM PKCS#8, which begins
            <code>BEGIN PRIVATE KEY</code>.
          {/snippet}
          <SecretRefInput
            value={source.privateKey}
            {secrets}
            label="private key"
            onchange={(v) => {
              if (source.type === 'httpApi') source.privateKey = v ?? '';
            }}
          />
        </Field>
      {/if}
      <Field label="token from">
        {#snippet help()}
          A token URL is used as given. An issuer is asked where its token endpoint is, and how it
          takes a secret.
        {/snippet}
        <select
          value={from}
          onchange={(e) => setTokenFrom(source, e.currentTarget.value as TokenFrom)}
        >
          <option value="url">token URL</option>
          <option value="issuer">issuer's discovery</option>
        </select>
      </Field>
      <Nested>
        {#if from === 'url'}
          <UrlField
            field="tokenUrl"
            label="token URL"
            bind:value={source.tokenUrl}
            sends={true}
            placeholder="https://auth.example.com/oauth2/token"
          >
            {#snippet help()}
              Where the client's token is obtained.
            {/snippet}
          </UrlField>
        {:else}
          <UrlField
            field="issuer"
            label="issuer"
            bind:value={source.issuer}
            sends={false}
            placeholder="https://login.example.com/realms/acme"
          >
            {#snippet help()}
              The OpenID issuer, exactly as the provider writes it.
            {/snippet}
          </UrlField>
        {/if}
      </Nested>
    </Nested>
  {/if}
{/snippet}

<!-- An Okta source's own fields: which org, and how it signs in. -->
{#snippet oktaFields(source: Extract<SourceDto, { type: 'oktaLogs' }>)}
  {@const how = oktaSignIn(source)}
  <UrlField
    field="org"
    label="org"
    bind:value={source.org}
    sends
    placeholder="https://acme.okta.com"
  >
    {#snippet help()}
      The address your users sign in at, or the org's custom domain.
    {/snippet}
  </UrlField>
  <Field label="sign in">
    {#snippet help()}
      Use a service app. Recommended because it keeps working after the person who set it up leaves.
      Use an API token for quick tests.
    {/snippet}
    <select
      value={how}
      onchange={(e) => setOktaSignIn(source, e.currentTarget.value as OktaSignIn)}
    >
      <option value="serviceApp">as a service app</option>
      <option value="token">with an API token</option>
    </select>
  </Field>
  <Nested>
    {#if how === 'serviceApp'}
      <Field field="clientId" label="client id">
        {#snippet help()}
          The client id of an API Services app granted the <code>okta.logs.read</code> scope and the Read-only
          Administrator role.
        {/snippet}
        <input
          type="text"
          value={source.clientId ?? ''}
          oninput={(e) => {
            if (source.type === 'oktaLogs') source.clientId = e.currentTarget.value.trim();
          }}
          placeholder="0oa1b2c3d4e5f6g7h8i9"
        />
      </Field>
      <Field field="privateKey" label="private key">
        {#snippet help()}
          Where the app's private key lives: an RSA key as PEM PKCS#8, which begins
          <code>BEGIN PRIVATE KEY</code>.
        {/snippet}
        <SecretRefInput
          value={source.privateKey}
          {secrets}
          label="private key"
          onchange={(v) => {
            if (source.type === 'oktaLogs') source.privateKey = v ?? '';
          }}
        />
      </Field>
    {:else}
      <Field field="token" label="token">
        {#snippet help()}
          Where an API token lives, created by an admin who can read the System Log.
        {/snippet}
        <SecretRefInput
          value={source.token}
          {secrets}
          label="token"
          onchange={(v) => {
            if (source.type === 'oktaLogs') source.token = v ?? '';
          }}
        />
      </Field>
    {/if}
  </Nested>
  <Advanced label="Advanced" modified={apiModified(source)}>
    {#if how === 'serviceApp'}
      <Field field="keyId" label="key id">
        {#snippet help()}
          The key's KID in Okta. Needed only when the app has more than one active key.
        {/snippet}
        <input
          type="text"
          value={source.keyId ?? ''}
          oninput={(e) => {
            if (source.type === 'oktaLogs')
              source.keyId = e.currentTarget.value.trim() || undefined;
          }}
        />
      </Field>
    {/if}
    {@render apiAdvanced(source)}
  </Advanced>
{/snippet}

<!-- A Kafka source's own fields: where its brokers are, what it reads and as which group. -->
{#snippet kafkaFields(source: Extract<SourceDto, { type: 'kafka' }>)}
  <Field field="brokers" label="brokers" warning={brokersWarning(source.brokers, source) ?? null}>
    <input type="text" bind:value={source.brokers} placeholder="host:9092,host2:9092" />
  </Field>
  <Field field="topics" label="topics">
    {#snippet help()}
      Every topic this consumer group reads. Add a second source only when the brokers, group id or
      start position differ.
    {/snippet}
    <TokenListInput bind:values={source.topics} placeholder="topic-name" />
  </Field>
{/snippet}

<!-- An Event Hubs source's own fields, which loading turns into a Kafka source's. -->
{#snippet eventHubsFields(source: Extract<SourceDto, { type: 'eventHubs' }>)}
  <Field field="namespace" label="namespace">
    {#snippet help()}
      The Event Hubs namespace: its name in Azure's public cloud, or its host name in another cloud.
    {/snippet}
    <input type="text" bind:value={source.namespace} placeholder="namespace" />
  </Field>
  <Field field="eventHubs" label="event hubs">
    {#snippet help()}
      Every event hub this consumer group reads.
    {/snippet}
    <TokenListInput bind:values={source.eventHubs} placeholder="event-hub" />
  </Field>
  <Field field="consumerGroup" label="consumer group">
    {#snippet help()}
      The consumer group this source reads as. Its committed offsets are kept under this name, so a
      new group reads again from <em>start at</em>.
    {/snippet}
    <input type="text" bind:value={source.consumerGroup} />
  </Field>
  <ConnectorAccess
    store="eventHubs"
    value={source}
    show="credential"
    {secrets}
    help={{
      connectionString:
        "Where the namespace's connection string lives, with the Listen right. Use the namespace's, not one that ends in EntityPath=.",
    }}
  />
{/snippet}

{#snippet moves()}Deploying this moves the source to the new position, so records are skipped or
  read again.{/snippet}

<!-- Where a stream source starts and stops reading. -->
{#snippet positions(source: StreamSource)}
  <div class="start">
    <StartAt
      bind:start={source.start}
      label="start at"
      fallback="earliest"
      note={startMoves(source) ? moves : undefined}
    >
      {#snippet help()}
        Where this source reads from.
      {/snippet}
    </StartAt>
  </div>

  <div class="pair">
    <Field label="stop at">
      {#snippet help()}
        Each partition stops reading at this point and goes idle.
      {/snippet}
      <select value={stopKind(source)} onchange={(e) => setStop(source, e.currentTarget.value)}>
        <option value="never">never &mdash; keep reading</option>
        <option value="timestamp">at a time</option>
      </select>
    </Field>
    {#if source.stop?.type === 'timestamp'}
      <Field label="until" help="Reading stops just before this time.">
        <input
          type="datetime-local"
          value={toLocalInput(source.stop.millis)}
          oninput={(e) => {
            const millis = fromLocalInput(e.currentTarget.value);
            if (millis !== undefined && isStream(source))
              source.stop = { type: 'timestamp', millis };
          }}
        />
      </Field>
    {/if}
  </div>
{/snippet}

<!-- A Kafka source's identity and security. -->
{#snippet kafkaSecurity(source: Extract<SourceDto, { type: 'kafka' }>)}
  <IdentityPicker
    value={source.identity}
    {identities}
    farEnd="source"
    onchange={(v) => {
      if (source.type === 'kafka') source.identity = v;
    }}
  />

  <div class="section">
    <div class="section-head">security</div>
    <KafkaSecurity
      value={source}
      {secrets}
      {mechanisms}
      hasIdentity={source.identity !== undefined}
    />
  </div>
{/snippet}

<!-- A receiver's own fields: where it listens, who it admits, and what each kind reads from a request. -->
{#snippet receiverFields(source: ReceiverSource)}
  <Field field="port" label="port">
    {#snippet help()}
      The port it listens on. Every worker of the group binds it, so no two receivers share one, but
      OTLP receivers of different signals may, and a UDP port is not the TCP port of that number.
    {/snippet}
    <input
      type="number"
      min="1"
      max="65535"
      value={source.port}
      oninput={(e) => {
        const n = bounded(e.currentTarget, { min: 1, max: 65535 });
        if (n !== undefined) source.port = n;
      }}
    />
  </Field>
  {#if source.type === 'otlpReceiver'}
    <Field field="signal" label="signal">
      {#snippet help()}
        What it takes: log records, metric data points or spans, each a record. To take more than
        one, add a receiver for each on the same port.
      {/snippet}
      <select
        value={source.signal ?? 'logs'}
        onchange={(e) => {
          if (source.type !== 'otlpReceiver') return;
          const v = e.currentTarget.value as OtlpSignal;
          source.signal = v === 'logs' ? undefined : v;
        }}
      >
        <option value="logs">logs</option>
        <option value="metrics">metrics</option>
        <option value="traces">traces</option>
      </select>
    </Field>
  {/if}
  {#if source.type === 'syslogReceiver'}
    <Field field="transport" label="transport">
      {#snippet help()}
        What senders send over. Over UDP each datagram is a message, served without TLS. A UDP
        receiver can't slow a sender, so what arrives while it is full is dropped.
      {/snippet}
      <select
        value={source.transport ?? 'tcp'}
        onchange={(e) => {
          if (source.type !== 'syslogReceiver') return;
          const v = e.currentTarget.value as 'tcp' | 'udp';
          source.transport = v === 'tcp' ? undefined : v;
          // The load refuses an identity and a line breaker under UDP, which serves no TLS and frames nothing.
          if (v === 'udp') {
            source.identity = undefined;
            delete source.lineBreaker;
          }
        }}
      >
        <option value="tcp">tcp</option>
        <option value="udp">udp &mdash; one message per datagram</option>
      </select>
    </Field>
  {/if}
  {#if source.type === 'httpReceiver'}
    <Field field="path" label="path">
      {#snippet help()}
        The path senders POST to. Empty is <code>/</code>.
      {/snippet}
      <input
        type="text"
        value={source.path ?? ''}
        oninput={(e) => {
          if (source.type !== 'httpReceiver') return;
          const v = e.currentTarget.value.trim();
          source.path = v === '' ? undefined : v;
        }}
        placeholder="/"
      />
    </Field>
  {/if}
  {#if source.type === 'syslogReceiver'}
    <Field field="allowFrom" label="allow from">
      {#snippet help()}
        {#if source.transport === 'udp'}
          The networks a sender may send from, each a CIDR block. Over UDP only these networks admit
          a sender, and a UDP sender's address can be spoofed.
        {:else}
          The networks a sender may connect from, each a CIDR block. Syslog carries no token, so the
          receiver needs these networks, an identity with authorities, or both. With both, a sender
          must connect from a listed network and present a certificate.
        {/if}
      {/snippet}
      <TokenListInput
        bind:values={
          () => (source.type === 'syslogReceiver' && source.allowFrom) || [],
          (cidrs) => {
            if (source.type !== 'syslogReceiver') return;
            if (cidrs.length > 0) source.allowFrom = cidrs;
            else delete source.allowFrom;
          }
        }
        placeholder="10.0.0.0/8"
      />
    </Field>
  {:else}
    <ReceiverTokens value={source} acknowledges={source.type === 'hecReceiver'} {secrets}>
      {#snippet help()}
        {#if source.type === 'hecReceiver'}
          Each token a sender may present as <code>Authorization: Splunk &lt;token&gt;</code>, by
          name. Tick acknowledgement for a sender that asks after its requests by ackId.
        {:else if source.type === 's2sReceiver'}
          Each token a forwarder may present as its outputs.conf <code>token</code>, by name.
        {:else if source.type === 'elasticReceiver'}
          Each token a sender may present as <code>Authorization: Bearer &lt;token&gt;</code> or
          <code>ApiKey &lt;token&gt;</code>, or as a Basic password, by name.
        {:else}
          Each token a sender may present as <code>Authorization: Bearer &lt;token&gt;</code>, by
          name.
        {/if}
        With no token, the receiver needs an identity with authorities.
      {/snippet}
    </ReceiverTokens>
  {/if}
  {#if source.type === 's2sReceiver'}
    <ReceiverSourcetypes value={source}>
      {#snippet help()}
        Where each sourcetype's records end. The first capture group is required and will be
        dropped. A sourcetype not named here is a record per line.
      {/snippet}
    </ReceiverSourcetypes>
  {/if}
  {#if source.type === 'httpReceiver'}
    <Field field="records" label="records">
      {#snippet help()}
        Where a request's records are: the whole body is one record, each line is one, or each
        element of a JSON array is one.
      {/snippet}
      <select
        value={source.records ?? 'body'}
        onchange={(e) => {
          if (source.type !== 'httpReceiver') return;
          const v = e.currentTarget.value as ApiRecords;
          source.records = v === 'body' ? undefined : v;
          if (v !== 'array') source.recordsAt = undefined;
        }}
      >
        <option value="body">body &mdash; the whole body</option>
        <option value="lines">lines &mdash; one per line</option>
        <option value="array">array &mdash; one per element</option>
      </select>
    </Field>
    {#if source.records === 'array'}
      <Nested>
        <Field field="recordsAt" label="records at">
          {#snippet help()}
            The JSON pointer to the array, such as <code>/events</code>. Empty is a body that is the
            array.
          {/snippet}
          <input
            type="text"
            value={source.recordsAt ?? ''}
            oninput={(e) => {
              if (source.type === 'httpReceiver')
                source.recordsAt = e.currentTarget.value.trim() || undefined;
            }}
            placeholder="/events"
          />
        </Field>
      </Nested>
    {/if}
  {/if}
  <Advanced label="Advanced" modified={receiverModified(source)}>
    {#if !(source.type === 'syslogReceiver' && source.transport === 'udp')}
      <IdentityPicker
        value={source.identity}
        {identities}
        farEnd="sender"
        onchange={(v) => (source.identity = v)}
      />
    {/if}
    {#if source.type === 'hecReceiver'}
      <ReceiverSourcetypes value={source}>
        {#snippet help()}
          Where each raw request's records end, by its <code>sourcetype</code>. The first capture
          group is required and will be dropped. A sourcetype not named here is a record per line.
        {/snippet}
      </ReceiverSourcetypes>
    {/if}
    <Field field="maxRequestBytes" label="request size">
      {#snippet help()}
        {#if source.type === 's2sReceiver'}
          The largest frame it reads, and the most one record holds, at most
          {exactBytes(LIMITS.receiverRequestBytes.max)}.
        {:else if source.type === 'syslogReceiver'}
          The largest message it reads, at most {exactBytes(LIMITS.receiverRequestBytes.max)}.
          {#if source.transport === 'udp'}A larger datagram is dropped.{:else}A larger one closes
            the connection.{/if}
        {:else}
          The largest request body it reads, at most {exactBytes(LIMITS.receiverRequestBytes.max)}.
          A larger one is refused, and a body sent as gzip is held to it as it inflates.
        {/if}
      {/snippet}
      <BytesInput
        value={source.maxRequestBytes}
        fallback={LIMITS.receiverRequestBytes.default}
        onchange={(n) => (source.maxRequestBytes = n)}
      />
    </Field>
    {#if source.type === 'syslogReceiver'}
      {@render encodingField(source)}
      {#if source.transport !== 'udp'}
        <LineBreakerField
          value={source.lineBreaker}
          placeholder={'(\\x00)'}
          onchange={(v) => {
            if (source.type === 'syslogReceiver') source.lineBreaker = v;
          }}
        />
      {/if}
    {/if}
  </Advanced>
{/snippet}

<!-- A source's own fields, below its id: the same in its row and in the add dialog. -->
{#snippet fields(source: SourceDto)}
  {#if isStream(source)}
    {#if source.type === 'kafka'}
      {@render kafkaFields(source)}
    {:else}
      {@render eventHubsFields(source)}
    {/if}
    {@render positions(source)}
    <AvroRead value={source} stream {secrets} {identities} />
    {#if source.type === 'kafka'}
      {@render kafkaSecurity(source)}
    {/if}
    <Advanced label="Advanced" modified={streamModified(source)}>
      {#if source.type === 'eventHubs'}
        <ConnectorAccess store="eventHubs" value={source} show="advanced" />
      {/if}
      {#if source.type === 'kafka'}
        <Field field="groupId" label="group id">
          {#snippet help()}
            Leave it blank unless this source needs offsets of its own. Blank reads as its worker
            group's consumer group, <code>{defaultGroupId()}</code>, so every worker group reads
            every record and rebalances only among its own workers.
          {/snippet}
          <input type="text" bind:value={source.groupId} placeholder={defaultGroupId()} />
        </Field>
      {/if}
      <Field field="fetchBytes" label="fetch buffer">
        {#snippet help()}
          The most fetched but not yet processed data this source holds on a worker, across all its
          partitions.
        {/snippet}
        <BytesInput
          value={source.fetchBytes}
          fallback={LIMITS.fetchBytes.default}
          onchange={(n) => {
            if (isStream(source)) source.fetchBytes = n;
          }}
        />
      </Field>
      {@render encodingField(source)}
      {#if source.type === 'kafka'}
        <KafkaPropsEditor value={source} {secrets} hasIdentity={source.identity !== undefined} />
      {/if}
    </Advanced>
  {:else if isObjectSource(source)}
    {@const store = OBJECT_STORE[source.type]}
    {@const limit = source.type === 'gcsPubSub' ? LIMITS.ackDeadlineSecs : LIMITS.visibilitySecs}
    {#if source.type === 'gcsPubSub'}
      <Field field="subscription" label="subscription">
        {#snippet help()}
          The Pub/Sub subscription on the topic a bucket's notifications go to, named in full. Give
          it a dead-letter policy: a message whose object cannot be read ends only through it.
        {/snippet}
        <input
          type="text"
          bind:value={source.subscription}
          placeholder="projects/project/subscriptions/subscription"
        />
      </Field>
    {:else}
      <UrlField
        field="queueUrl"
        label="queue URL"
        bind:value={source.queueUrl}
        sends
        placeholder={source.type === 's3Sqs'
          ? 'https://sqs.region.amazonaws.com/account/queue'
          : 'https://account.queue.core.windows.net/queue'}
      >
        {#snippet help()}
          {#if source.type === 's3Sqs'}
            The SQS queue S3 announces new objects on, from any bucket.
          {:else}
            The Storage queue Event Grid sends <code>BlobCreated</code> events to.
          {/if}
        {/snippet}
      </UrlField>
    {/if}
    {#if source.type === 'azureBlobQueue'}
      <Field field="account" label="account">
        {#snippet help()}
          The storage account holding the blobs. A blob in another account is refused.
        {/snippet}
        <input type="text" bind:value={source.account} />
      </Field>
    {/if}
    <ConnectorAccess {store} value={source} show="credential" {secrets} />
    {@render framingField(source)}
    <IdentityPicker
      value={source.identity}
      {identities}
      farEnd="source"
      onchange={(v) => (source.identity = v)}
    />
    <Advanced label="Advanced" modified={objectsModified(source)}>
      {#if source.type === 'gcsPubSub'}
        <UrlField
          field="pubsubEndpoint"
          label="Pub/Sub endpoint"
          bind:value={
            () => source.pubsubEndpoint,
            (v) => {
              if (source.type === 'gcsPubSub') source.pubsubEndpoint = v?.trim() || undefined;
            }
          }
          sends
          optional
          placeholder="https://pubsub.googleapis.com"
        >
          {#snippet help()}
            Another Pub/Sub address, such as an emulator's. Left empty, Google's.
          {/snippet}
        </UrlField>
      {/if}
      <ConnectorAccess {store} value={source} show="advanced" />
      {@render encodingField(source)}
      <Field field="visibilitySecs" label="visibility seconds">
        {#snippet help()}
          How long a received message stays hidden from every other worker. A worker that dies
          holding one hands it on after this long. At most {limit.max}.
        {/snippet}
        <input
          type="number"
          min={limit.min}
          max={limit.max}
          value={source.visibilitySecs ?? limit.default}
          oninput={(e) => (source.visibilitySecs = bounded(e.currentTarget, limit))}
        />
      </Field>
      {#if source.type === 'azureBlobQueue'}
        <Field field="maxDeliveries" label="max deliveries">
          {#snippet help()}
            Times a message is received before it is moved to the queue named after this one with
            <code>-poison</code> added.
          {/snippet}
          <input
            type="number"
            min={LIMITS.maxDeliveries.min}
            max={LIMITS.maxDeliveries.max}
            value={source.maxDeliveries ?? LIMITS.maxDeliveries.default}
            oninput={(e) => {
              if (source.type === 'azureBlobQueue')
                source.maxDeliveries = bounded(e.currentTarget, LIMITS.maxDeliveries);
            }}
          />
        </Field>
      {/if}
    </Advanced>
  {:else if isListing(source)}
    {@render listingFields(source)}
  {:else if source.type === 'httpApi'}
    {@render httpApiFields(source)}
  {:else if source.type === 'oktaLogs'}
    {@render oktaFields(source)}
  {:else if isSql(source)}
    <SqlSourceFields {source} {identities} {secrets} {readAt} {onsaved} />
  {:else if isReceiver(source)}
    {@render receiverFields(source)}
  {:else if source.type === 'metrics'}
    <Field field="bucketSeconds" label="bucket seconds">
      {#snippet help()}
        How many seconds of each worker's own metrics go into one row.
      {/snippet}
      <input type="number" min="1" bind:value={source.bucketSeconds} />
    </Field>
  {:else}
    <Field field="minLevel" label="min level">
      {#snippet help()}
        The lowest severity this source carries. Below the worker's own <code>SMELTR_LOG_LEVEL</code
        >, <code>info</code> unless set, it gets nothing more.
      {/snippet}
      <select
        value={source.minLevel ?? DEFAULT_LEVEL}
        onchange={(e) => {
          // Absent is the default, so choosing it writes nothing: a file states what an author
          // decided, and `info` here is what they left alone.
          const picked = e.currentTarget.value;
          if (source.type === 'logs')
            source.minLevel = picked === DEFAULT_LEVEL ? undefined : picked;
        }}
      >
        {#each LEVELS as level (level)}
          <option value={level}>{level}</option>
        {/each}
      </select>
    </Field>
  {/if}
{/snippet}

{@render fields(source)}

<style>
  /* A choice and the value it asks for, side by side: the value has no
     meaning without the choice above it. */
  .pair {
    display: flex;
    gap: 0.8rem;
    align-items: flex-start;
  }

  .pair > :global(*) {
    flex: 1;
  }

  /* A group of fields that answer one question. */
  .section {
    border-top: 1px solid var(--border);
    padding-top: 0.6rem;
  }

  .section-head {
    font-size: var(--text-micro);
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--dim);
    margin-bottom: 0.5rem;
  }

  input,
  select,
  .start :global(:is(input, select)) {
    font-family: var(--mono);
  }

  /* Only so the rule above reaches the shared editor's controls: it lays out as if it were not there. */
  .start {
    display: contents;
  }
</style>
