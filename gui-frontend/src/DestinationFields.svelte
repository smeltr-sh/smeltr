<!--
  One destination's form, by its type: every field it takes but its id. The Destinations page draws it in each row
  and in its add dialog (`NewDestination`), which the Routes page opens too.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';
  import KafkaSecurity from './KafkaSecurity.svelte';
  import { brokersWarning, SECTION_PROPS } from './kafka';
  import SecretRefInput from './SecretRefInput.svelte';
  import ObjectStoreAccess, { accessModified } from './ObjectStoreAccess.svelte';
  import Advanced from './Advanced.svelte';
  import Help from './Help.svelte';
  import Field from './Field.svelte';
  import UrlField from './UrlField.svelte';
  import KafkaPropsEditor from './KafkaPropsEditor.svelte';
  import PropsEditor from './PropsEditor.svelte';
  import ZoneUrls from './ZoneUrls.svelte';
  import RequestDelivery, { requestDeliveryModified } from './RequestDelivery.svelte';
  import Toggle from './Toggle.svelte';
  import {
    batchedFormat,
    headerRefusal,
    headerValueRefusal,
    setBatched,
    setSingle,
    identityRefusal,
    type HttpBatched,
  } from './http';
  import IdentityPicker from './IdentityPicker.svelte';
  import IcebergFields from './IcebergFields.svelte';
  import DeltaFields from './DeltaFields.svelte';
  import SchemaRegistryFields from './SchemaRegistryFields.svelte';
  import {
    LIMITS,
    OBJECT_PREFIX,
    type DestinationDto,
    type IdentityDto,
    type SecretDto,
    type DocIdPolicy,
    type Compression,
    type SchemaRegistry,
    type HttpCompression,
    COMPRESSIONS,
    STORAGE_CLASSES,
    SFTP_MIN_STALL_AFTER,
  } from './api';
  import { numberOrUnset } from './page';
  import BytesInput from './BytesInput.svelte';
  import LinkEstimate from './LinkEstimate.svelte';
  import { byteCount, exactBytes, type ByteCount } from './bytes';
  import {
    httpModified,
    isCloud,
    logAnalyticsModified,
    isObjectStore,
    objectModified,
    prefixOf,
    sendsCredential,
  } from './destinations';

  let {
    destination,
    identities,
    secrets,
    mechanisms,
  }: {
    destination: DestinationDto;
    /** Every identity the destination may name. */
    identities: IdentityDto[];
    /** The secret catalog, so a credential can be pointed at one the leader holds. */
    secrets: SecretDto[];
    /** The SASL mechanisms this leader's build can select. */
    mechanisms: string[];
  } = $props();

  /** The buffer a Kafka card is showing, default filled in: what the derived block splits. */
  const kafkaBuffer = (d: { bufferBytes?: ByteCount }) =>
    byteCount(d.bufferBytes, LIMITS.bufferBytes.default);

  /** Properties set by hand: the security section's own are its business,
   * and Advanced's marker speaks only for what Advanced hides. */
  function hasProps(d: DestinationDto): boolean {
    if (d.type !== 'kafka') return false;
    const byHand = (m: Record<string, unknown> | undefined) =>
      Object.keys(m ?? {}).filter((k) => !SECTION_PROPS.includes(k)).length > 0;
    return byHand(d.properties) || byHand(d.credentials);
  }

  /** Drives the Advanced header's marker (STYLE_GUIDE, "Advanced
   * settings"): a collapsed section must say when it hides a live
   * setting. */
  function kafkaModified(d: DestinationDto): boolean {
    if (d.type !== 'kafka') return false;
    return (
      hasProps(d) ||
      (d.compression ?? 'none') !== 'none' ||
      (d.stallAfterSecs ?? LIMITS.stallAfterSecs.default) !== LIMITS.stallAfterSecs.default ||
      byteCount(d.bufferBytes, LIMITS.bufferBytes.default) !== LIMITS.bufferBytes.default
    );
  }

  function eventHubsModified(d: DestinationDto): boolean {
    if (d.type !== 'eventHubs') return false;
    return (
      accessModified('eventHubs', d) ||
      (d.compression ?? 'none') !== 'none' ||
      (d.stallAfterSecs ?? LIMITS.stallAfterSecs.default) !== LIMITS.stallAfterSecs.default ||
      byteCount(d.bufferBytes, LIMITS.bufferBytes.default) !== LIMITS.bufferBytes.default
    );
  }

  function elasticModified(d: DestinationDto): boolean {
    if (d.type !== 'elasticsearch') return false;
    return (
      d.zoneUrls !== undefined ||
      d.sniff === true ||
      requestDeliveryModified(d, 'maxBulkBytes', LIMITS.maxBulkBytes.default)
    );
  }

  function splunkModified(d: DestinationDto): boolean {
    if (d.type !== 'splunkHec') return false;
    return (
      d.zoneUrls !== undefined ||
      requestDeliveryModified(d, 'maxRequestBytes', LIMITS.maxRequestBytes.default)
    );
  }

  /** What the shared format select edits: a destination's `format`, the `field` only `text` and `bytes` write,
   * the `newlines` only `bytes` reads, the `schemaRegistry` only a stream's `avro` registers in, and the
   * `compression` an Avro file has no `lz4` of. */
  type Formatted = {
    format?: string;
    field?: string;
    newlines?: string;
    schemaRegistry?: SchemaRegistry;
    compression?: Compression;
  };

  const STREAM_FORMATS = ['json', 'text', 'bytes'] as const;
  /** A Kafka or Event Hubs destination's: Avro registers its schema in a schema registry. */
  const AVRO_STREAM_FORMATS = [...STREAM_FORMATS, 'avro'] as const;
  const OBJECT_FORMATS = ['ndjson', 'json', 'parquet', 'avro', 'csv', 'text', 'bytes'] as const;
  /** A format's option, where its name alone does not say what each record becomes. */
  const STREAM_LABELS = { avro: 'avro — one Avro message per record' };
  const OBJECT_LABELS = { avro: 'avro — one Avro file' };

  /** An empty text box unsets the field rather than naming `""`. */
  function optional(value: string): string | undefined {
    return value.trim() === '' ? undefined : value;
  }
</script>

{#snippet inOrder()}
  A Prometheus destination sends one request at a time, so each series lands in order.
{/snippet}

{#snippet zoneHelp()}
  A worker in one of these zones sends here instead of to <code>url</code>. Any other worker sends
  to <code>url</code>.
{/snippet}

{#snippet fields(destination: DestinationDto)}
  {#if destination.type === 'kafka'}
    <Field
      field="brokers"
      label="brokers"
      warning={brokersWarning(destination.brokers, destination) ?? null}
    >
      <input type="text" bind:value={destination.brokers} placeholder="host:9092,host2:9092" />
    </Field>
    <Field field="defaultTopic" label="default topic">
      <input type="text" bind:value={destination.defaultTopic} />
    </Field>
    {@render formatFields(
      destination,
      AVRO_STREAM_FORMATS,
      'json',
      streamFormatHelp,
      STREAM_LABELS,
    )}
    <IdentityPicker
      value={destination.identity}
      {identities}
      farEnd="destination"
      onchange={(v) => {
        if (destination.type === 'kafka') destination.identity = v;
      }}
    />
    <div class="section">
      <div class="section-head">security</div>
      <KafkaSecurity
        value={destination}
        {secrets}
        {mechanisms}
        hasIdentity={destination.identity !== undefined}
      />
    </div>
    <Advanced label="Delivery and client properties" modified={kafkaModified(destination)}>
      <Field field="compression" label="compression">
        {#snippet help()}
          Compresses each batch sent to the brokers, saving broker bandwidth for some CPU on the
          worker.
        {/snippet}
        {#snippet note()}Off by default.{/snippet}
        <select
          value={destination.compression ?? 'none'}
          onchange={(e) => {
            if (destination.type === 'kafka')
              destination.compression = e.currentTarget.value as Compression;
          }}
        >
          {#each COMPRESSIONS as codec (codec)}
            <option value={codec}>{codec}</option>
          {/each}
        </select>
      </Field>
      <Field field="stallAfterSecs" label="stall after (seconds)">
        {#snippet help()}
          How long one batch may take to deliver before this destination counts as stalled.
        {/snippet}
        {#snippet note()}Default {LIMITS.stallAfterSecs.default}.{/snippet}
        <input
          type="number"
          min={LIMITS.stallAfterSecs.min}
          max={LIMITS.stallAfterSecs.max}
          value={destination.stallAfterSecs ?? LIMITS.stallAfterSecs.default}
          oninput={(e) => {
            if (destination.type === 'kafka')
              destination.stallAfterSecs = numberOrUnset(e.currentTarget);
          }}
        />
      </Field>
      <Field field="bufferBytes" label="buffer">
        {#snippet help()}
          The most one worker holds for this destination, accepted but not yet acknowledged by the
          brokers. Over a slow link it caps throughput.
        {/snippet}
        {#snippet note()}<LinkEstimate bufferBytes={kafkaBuffer(destination)} /> Default
          {exactBytes(LIMITS.bufferBytes.default)}.{/snippet}
        <BytesInput
          value={destination.bufferBytes}
          fallback={LIMITS.bufferBytes.default}
          onchange={(n) => {
            if (destination.type === 'kafka') destination.bufferBytes = n;
          }}
        />
      </Field>
      <KafkaPropsEditor
        value={destination}
        {secrets}
        hasIdentity={destination.identity !== undefined}
      />
    </Advanced>
  {:else if destination.type === 'eventHubs'}
    <Field field="namespace" label="namespace">
      {#snippet help()}
        The Event Hubs namespace: its name in Azure's public cloud, or its host name in another
        cloud.
      {/snippet}
      <input type="text" bind:value={destination.namespace} placeholder="namespace" />
    </Field>
    <Field field="defaultEventHub" label="default event hub">
      {#snippet help()}
        The event hub a record goes to unless its pipeline sets <code>__destination.topic</code>.
      {/snippet}
      <input type="text" bind:value={destination.defaultEventHub} />
    </Field>
    <ObjectStoreAccess
      store="eventHubs"
      value={destination}
      show="credential"
      {secrets}
      help={{
        connectionString: "Where the namespace's connection string lives, with the Send right.",
      }}
    />
    {@render formatFields(
      destination,
      AVRO_STREAM_FORMATS,
      'json',
      streamFormatHelp,
      STREAM_LABELS,
    )}
    <Advanced label="Delivery" modified={eventHubsModified(destination)}>
      <ObjectStoreAccess store="eventHubs" value={destination} show="advanced" />
      <Field field="compression" label="compression">
        {#snippet help()}
          Compresses each batch sent to Event Hubs, when the namespace takes gzip.
        {/snippet}
        {#snippet note()}Off by default.{/snippet}
        <select
          value={destination.compression ?? 'none'}
          onchange={(e) => {
            if (destination.type === 'eventHubs')
              destination.compression = e.currentTarget.value as 'none' | 'gzip';
          }}
        >
          <option value="none">none</option>
          <option value="gzip">gzip</option>
        </select>
      </Field>
      <Field field="stallAfterSecs" label="stall after (seconds)">
        {#snippet help()}
          How long one batch may take to deliver before this destination counts as stalled.
        {/snippet}
        {#snippet note()}Default {LIMITS.stallAfterSecs.default}.{/snippet}
        <input
          type="number"
          min={LIMITS.stallAfterSecs.min}
          max={LIMITS.stallAfterSecs.max}
          value={destination.stallAfterSecs ?? LIMITS.stallAfterSecs.default}
          oninput={(e) => {
            if (destination.type === 'eventHubs')
              destination.stallAfterSecs = numberOrUnset(e.currentTarget);
          }}
        />
      </Field>
      <Field field="bufferBytes" label="buffer">
        {#snippet help()}
          The most one worker holds for this destination, accepted but not yet acknowledged by Event
          Hubs. Over a slow link it caps throughput.
        {/snippet}
        {#snippet note()}<LinkEstimate bufferBytes={kafkaBuffer(destination)} /> Default
          {exactBytes(LIMITS.bufferBytes.default)}.{/snippet}
        <BytesInput
          value={destination.bufferBytes}
          fallback={LIMITS.bufferBytes.default}
          onchange={(n) => {
            if (destination.type === 'eventHubs') destination.bufferBytes = n;
          }}
        />
      </Field>
    </Advanced>
  {:else if destination.type === 'logAnalytics'}
    {@const l = destination}
    <UrlField
      field="endpoint"
      label="endpoint"
      bind:value={l.endpoint}
      sends
      error={identityRefusal(l.endpoint, l.identity) ?? null}
      placeholder={'https://{name}.{region}.ingest.monitor.azure.com'}
    >
      {#snippet help()}
        The rule's logs ingestion endpoint, or the data collection endpoint linked to it.
      {/snippet}
    </UrlField>
    <Field field="ruleId" label="rule id">
      {#snippet help()}
        The rule's immutable id, from its JSON view in the Azure portal.
      {/snippet}
      <input type="text" bind:value={l.ruleId} placeholder="dcr-0123456789abcdef0123456789abcdef" />
    </Field>
    <Field field="stream" label="stream">
      {#snippet help()}
        The rule's stream the rows go to, such as <code>Custom-Events_CL</code>.
      {/snippet}
      <input type="text" bind:value={l.stream} placeholder="Custom-Events_CL" />
    </Field>
    <ObjectStoreAccess store="logAnalytics" value={l} show="credential" {secrets} />
    <Advanced label="Advanced" modified={logAnalyticsModified(l)}>
      <IdentityPicker
        value={l.identity}
        {identities}
        farEnd="destination"
        onchange={(v) => (l.identity = v)}
      />
      <ObjectStoreAccess store="logAnalytics" value={l} show="advanced" />
      <RequestDelivery value={l} sizeDefault={LIMITS.logAnalyticsRequestBytes.default}>
        {#snippet sizeHelp()}
          A request is sent as soon as it reaches this size. Keep it under the largest body the
          service accepts, or it refuses the request.
        {/snippet}
        {#snippet sizeNote()}Default and at most {exactBytes(
            LIMITS.logAnalyticsRequestBytes.default,
          )}.{/snippet}
      </RequestDelivery>
    </Advanced>
  {:else if destination.type === 'elasticsearch'}
    <UrlField
      field="url"
      label="url"
      bind:value={destination.url}
      sends={sendsCredential(destination)}
      placeholder="https://es.internal:9200"
    />
    <Field field="defaultIndex" label="default index">
      <input type="text" bind:value={destination.defaultIndex} />
    </Field>
    <IdentityPicker
      value={destination.identity}
      {identities}
      farEnd="destination"
      onchange={(v) => {
        if (destination.type === 'elasticsearch') destination.identity = v;
      }}
    />
    <Field field="auth" label="auth">
      {#snippet help()}
        The secret holding the API key.
      {/snippet}
      <SecretRefInput
        value={destination.auth}
        {secrets}
        label="auth"
        onchange={(v) => {
          if (destination.type === 'elasticsearch') destination.auth = v;
        }}
      />
    </Field>
    <Field field="docId" label="duplicate documents">
      {#snippet help()}
        After a crash a document can be sent twice. Under <code>auto</code> that is a second copy;
        <code>dedupe</code>
        and <code>overwrite</code> catch it.
      {/snippet}
      <select
        value={destination.docId ?? 'auto'}
        onchange={(e) => {
          if (destination.type === 'elasticsearch')
            destination.docId = e.currentTarget.value as DocIdPolicy;
        }}
      >
        <option value="auto">auto &mdash; send no id</option>
        <option value="dedupe">dedupe &mdash; keep the first copy, drop re-sends</option>
        <option value="overwrite">overwrite &mdash; replace with the latest copy</option>
      </select>
    </Field>
    {#if (destination.docId ?? 'auto') !== 'auto'}
      <Field field="idField" label="id field">
        {#snippet help()}
          The row field that already holds each document's stable id. Leave it empty and the
          pipeline must set <code>__destination.id</code>.
        {/snippet}
        <input
          type="text"
          value={destination.idField ?? ''}
          oninput={(e) => {
            if (destination.type === 'elasticsearch')
              destination.idField = optional(e.currentTarget.value);
          }}
          placeholder="event.id"
        />
      </Field>
    {/if}
    <Advanced label="Advanced" modified={elasticModified(destination)}>
      <RequestDelivery
        value={destination}
        size="maxBulkBytes"
        sizeLabel="bulk size"
        sizeDefault={LIMITS.maxBulkBytes.default}
        requests="Bulk requests"
      >
        {#snippet sizeHelp()}
          A bulk request is sent as soon as it reaches this size. Keep it under the largest request
          the cluster takes.
        {/snippet}
        {#snippet sizeNote()}<a
            class="link"
            href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-network.html#http-settings"
            target="_blank"
            rel="noreferrer">Elastic's HTTP settings</a
          >{/snippet}
      </RequestDelivery>
      <ZoneUrls
        value={destination}
        sends={sendsCredential(destination)}
        placeholder="https://es-a.internal:9200"
        help={zoneHelp}
      />
      <Field field="sniff" label="sniff nodes" note="Default off.">
        {#snippet help()}
          Learns the cluster's nodes from <code>url</code> and spreads requests across them,
          preferring this worker's zone. Every node must be reachable from the workers, and the
          credential needs the <code>monitor</code> cluster privilege.
        {/snippet}
        <Toggle
          bind:checked={
            () => destination.type === 'elasticsearch' && destination.sniff === true,
            (on) => {
              if (destination.type !== 'elasticsearch') return;
              if (on) destination.sniff = true;
              else {
                delete destination.sniff;
                delete destination.zoneAttribute;
              }
            }
          }
          ariaLabel="sniff nodes"
        />
      </Field>
      {#if destination.sniff}
        <Field field="zoneAttribute" label="zone attribute" note="Default zone.">
          {#snippet help()}
            Optional: The node attribute that names a node's zone, set on each node as
            <code>node.attr.&lt;name&gt;</code>.
          {/snippet}
          <input
            type="text"
            placeholder="zone"
            value={destination.zoneAttribute ?? ''}
            oninput={(e) => {
              if (destination.type === 'elasticsearch')
                destination.zoneAttribute = optional(e.currentTarget.value);
            }}
          />
        </Field>
      {/if}
    </Advanced>
  {:else if destination.type === 'splunkHec'}
    <UrlField
      field="url"
      label="url"
      bind:value={destination.url}
      sends={sendsCredential(destination)}
      placeholder="https://splunk.internal:8088"
    >
      {#snippet help()}
        The collector's base address, without <code>/services/collector</code>.
      {/snippet}
    </UrlField>
    <Field field="token" label="token">
      {#snippet help()}
        The secret holding the HEC token.
      {/snippet}
      <SecretRefInput
        value={destination.token}
        {secrets}
        label="token"
        onchange={(v) => {
          if (destination.type === 'splunkHec') destination.token = v ?? '';
        }}
      />
    </Field>
    <Field field="endpoint" label="endpoint">
      {#snippet help()}
        <code>event</code>: each row is one event, filed under its own top-level
        <code>_time</code>, <code>host</code>, <code>source</code> and
        <code>sourcetype</code>; the fields below fill any it lacks. <code>raw</code>: each row is
        one line of text, filed under the fields below.
      {/snippet}
      <select
        value={destination.endpoint ?? 'event'}
        onchange={(e) => {
          if (destination.type !== 'splunkHec') return;
          destination.endpoint = e.currentTarget.value === 'raw' ? 'raw' : undefined;
          // The event endpoint takes no bytes.
          if (destination.endpoint === undefined && destination.format === 'bytes') {
            delete destination.format;
            delete destination.field;
          }
        }}
      >
        <option value="event">event &mdash; read from the document</option>
        <option value="raw">raw &mdash; one line per row</option>
      </select>
    </Field>
    {@render formatFields(
      destination,
      destination.endpoint === 'raw' ? STREAM_FORMATS : ['json', 'text'],
      'json',
      streamFormatHelp,
    )}
    {#each ['index', 'sourcetype', 'source', 'host'] as const as key (key)}
      {#snippet keyHelp()}
        {#if key === 'index'}
          A row's <code>__destination.index</code> picks its own.
        {:else}
          A document's own <code>{key}</code> wins.
        {/if}
      {/snippet}
      <Field
        field={key}
        label={key}
        help={(destination.endpoint ?? 'event') === 'event' ? keyHelp : undefined}
      >
        <input
          type="text"
          value={destination[key] ?? ''}
          oninput={(e) => {
            if (destination.type === 'splunkHec')
              destination[key] = optional(e.currentTarget.value);
          }}
          placeholder={key === 'index' ? "the token's default" : ''}
        />
      </Field>
    {/each}
    <Field field="deliveredWhen" label="delivered when">
      {#snippet help()}
        <em>indexed</em> needs the token's indexer acknowledgement on; <em>received</em>
        needs it off. Under <em>received</em>, smeltr does not send a batch again once Splunk
        accepts it.
      {/snippet}
      <select
        value={destination.acknowledgement ?? 'indexer'}
        onchange={(e) => {
          if (destination.type === 'splunkHec')
            destination.acknowledgement =
              e.currentTarget.value === 'receipt' ? 'receipt' : undefined;
        }}
      >
        <option value="indexer">indexed &mdash; at-least-once</option>
        <option value="receipt">received &mdash; may lose events</option>
      </select>
    </Field>
    <IdentityPicker
      value={destination.identity}
      {identities}
      farEnd="destination"
      onchange={(v) => {
        if (destination.type === 'splunkHec') destination.identity = v;
      }}
    />
    <Advanced label="Advanced" modified={splunkModified(destination)}>
      <RequestDelivery
        value={destination}
        sizeDefault={LIMITS.maxRequestBytes.default}
        including="its acknowledgement included"
      >
        {#snippet sizeHelp()}
          A request is sent as soon as it reaches this size. Keep it under the largest request the
          collector takes.
        {/snippet}
        {#snippet sizeNote()}<a
            class="link"
            href="https://docs.splunk.com/Documentation/Splunk/latest/Data/UsetheHTTPEventCollector"
            target="_blank"
            rel="noreferrer">Splunk's HTTP Event Collector</a
          >{/snippet}
      </RequestDelivery>
      <ZoneUrls
        value={destination}
        sends={sendsCredential(destination)}
        placeholder="https://splunk-a.internal:8088"
        help={zoneHelp}
      />
    </Advanced>
  {:else if destination.type === 'http' || destination.type === 'prometheus'}
    {@const h = destination}
    <UrlField
      field="url"
      label="url"
      bind:value={h.url}
      sends={sendsCredential(h)}
      error={identityRefusal(h.url, h.identity) ?? null}
      placeholder={h.type === 'prometheus'
        ? 'https://prometheus.internal/api/v1/write'
        : 'https://collector.internal/ingest'}
    />
    {#if h.type === 'prometheus'}
      <Field field="version" label="remote_write version">
        {#snippet help()}
          2.0 also sends each series' metadata and start time. The store must take 2.0; see
          Prometheus's <a
            class="link"
            href="https://prometheus.io/docs/specs/prw/remote_write_spec_2_0/"
            target="_blank"
            rel="noreferrer">remote_write spec</a
          >.
        {/snippet}
        <select
          value={h.version ?? '1.0'}
          onchange={(e) => {
            if (e.currentTarget.value === '2.0') h.version = '2.0';
            else delete h.version;
          }}
        >
          <option value="1.0">1.0</option>
          <option value="2.0">2.0</option>
        </select>
      </Field>
    {/if}
    {#if h.type === 'http'}
      <Field field="body" label="requests">
        <select
          value={h.body === 'single' ? 'single' : 'batched'}
          onchange={(e) => setSingle(h, e.currentTarget.value === 'single')}
        >
          <option value="batched">batched &mdash; many records per request</option>
          <option value="single">single &mdash; one record per request</option>
        </select>
      </Field>
      {#if h.body === 'single'}
        {@render formatFields(h, STREAM_FORMATS, 'json', streamFormatHelp)}
      {:else}
        <Field field="format" label="format" help={batchedHelp}>
          <select
            value={batchedFormat(h)}
            onchange={(e) => setBatched(h, e.currentTarget.value as HttpBatched)}
          >
            <option value="ndjson">ndjson &mdash; one JSON record per line</option>
            <option value="jsonArray">json array &mdash; one array of records</option>
            <option value="text">text &mdash; one field's text per line</option>
            <option value="textArray">text array &mdash; one array of strings</option>
            <option value="bytes">bytes &mdash; one field's bytes per line, as base64</option>
            <option value="bytesArray">bytes array &mdash; one array of base64 strings</option>
          </select>
        </Field>
        {#if h.format === 'text' || h.format === 'bytes'}
          {@const fieldDefault = h.format === 'text' ? '_raw' : '__source.bytes'}
          <Field field="field" label={`${h.format} field`}>
            {#snippet note()}Default <code>{fieldDefault}</code>.{/snippet}
            <input
              type="text"
              value={h.field ?? ''}
              oninput={(e) => (h.field = optional(e.currentTarget.value))}
              placeholder={fieldDefault}
            />
          </Field>
        {/if}
      {/if}
    {/if}
    <PropsEditor
      value={h}
      literalKey="headers"
      labels={{
        literal: 'headers',
        credentials: 'credentials',
        noun: 'header',
        literalName: 'X-Source',
        literalValue: 'smeltr',
        credentialName: 'Authorization',
      }}
      {secrets}
      refusal={(key, _credential, val) =>
        headerRefusal(key, h) ?? (val === undefined ? undefined : headerValueRefusal(val))}
    >
      {#snippet literalHelp()}
        Sent with every request, by name and value, committed to the config repo.
      {/snippet}
      {#snippet credentialsHelp()}
        A header whose value is secret, such as <code>Authorization</code>: the secret holds its
        whole value, <code>Bearer &lt;token&gt;</code> included. The value never reaches git.
      {/snippet}
    </PropsEditor>
    <Advanced label="Advanced" modified={httpModified(h)}>
      <IdentityPicker
        value={h.identity}
        {identities}
        farEnd="destination"
        onchange={(v) => (h.identity = v)}
      />
      {#if h.type === 'http'}
        <Field field="compression" label="compression">
          {#snippet help()}
            Each request body is sent gzipped, with <code>Content-Encoding: gzip</code>; the service
            must accept it.
          {/snippet}
          {#snippet note()}Off by default.{/snippet}
          <select
            value={h.compression ?? 'none'}
            onchange={(e) => {
              const v = e.currentTarget.value as HttpCompression;
              if (v === 'none') delete h.compression;
              else h.compression = v;
            }}
          >
            <option value="none">none</option>
            <option value="gzip">gzip</option>
          </select>
        </Field>
      {/if}
      <RequestDelivery
        value={h}
        sizeDefault={LIMITS.maxRequestBytes.default}
        concurrent={h.type === 'http'}
        bufferHelp={h.type === 'prometheus' ? inOrder : undefined}
      >
        {#snippet sizeHelp()}
          A request is sent as soon as it reaches this size. Keep it under the largest body the
          service accepts, or it refuses the request.
        {/snippet}
        {#snippet sizeNote()}Default {exactBytes(LIMITS.maxRequestBytes.default)}.{/snippet}
      </RequestDelivery>
      {#if h.type === 'http'}
        <ZoneUrls
          value={h}
          sends={sendsCredential(h)}
          placeholder="https://collector-a.internal/ingest"
          help={zoneHelp}
        />
      {/if}
    </Advanced>
  {:else if destination.type === 'iceberg'}
    <IcebergFields value={destination} {secrets} {identities} />
  {:else if destination.type === 'delta'}
    <DeltaFields value={destination} {secrets} {identities} />
  {:else if isObjectStore(destination)}
    {@const o = destination}
    {#if o.type === 'azureBlob'}
      <Field field="account" label="account">
        <input type="text" bind:value={o.account} />
      </Field>
      <Field field="container" label="container">
        <input type="text" bind:value={o.container} />
      </Field>
    {:else if o.type === 'sftp'}
      <ObjectStoreAccess store={o.type} value={o} show="credential" {secrets} />
      <Field field="directory" label="directory">
        {#snippet help()}
          The directory it writes into: absolute, or under the user's home directory. Empty is the
          home directory. It must exist; the folders a prefix names under it are made as needed.
        {/snippet}
        <input
          type="text"
          value={o.directory ?? ''}
          oninput={(e) => {
            if (o.type === 'sftp') o.directory = optional(e.currentTarget.value);
          }}
          placeholder="the user's home"
        />
      </Field>
    {:else}
      <Field field="bucket" label="bucket">
        <input type="text" bind:value={o.bucket} />
      </Field>
    {/if}
    {#if o.type !== 'sftp'}
      <ObjectStoreAccess store={o.type} value={o} show="credential" {secrets} />
    {/if}
    <Field field="prefix" label="prefix">
      {#snippet help()}
        What every {o.type === 'sftp' ? 'file path' : 'object key'} starts with.
        <code>%Y %m %d %H %M</code>
        are the write time, UTC. A row's <code>__destination.prefix</code> picks its own.
      {/snippet}
      {#snippet note()}Default <code>{OBJECT_PREFIX}</code>.{/snippet}
      <input
        type="text"
        value={prefixOf(o)}
        oninput={(e) => {
          const v = e.currentTarget.value;
          if (v === OBJECT_PREFIX) delete o.prefix;
          else o.prefix = v;
        }}
      />
    </Field>
    {@render formatFields(o, OBJECT_FORMATS, 'ndjson', objectFormatHelp, OBJECT_LABELS)}
    {#if o.format === 'bytes'}
      <Field field="newlines" label="newlines">
        {#snippet help()}
          <code>escape</code>: <code>\</code>, newline and carriage return are written
          <code>\\</code>, <code>\n</code> and <code>\r</code>, which an SQS/S3 source with framing
          <code>escapedLines</code> reverses. <code>refuse</code>: a row holding a line break is
          refused.
        {/snippet}
        {#snippet note()}Default <code>escape</code>.{/snippet}
        <select
          value={o.newlines ?? 'escape'}
          onchange={(e) => {
            const v = e.currentTarget.value;
            if (v === 'refuse') o.newlines = v;
            else delete o.newlines;
          }}
        >
          <option value="escape">escape</option>
          <option value="refuse">refuse</option>
        </select>
      </Field>
    {/if}
    {#if isCloud(o)}
      <IdentityPicker
        value={o.identity}
        {identities}
        farEnd="destination"
        onchange={(v) => {
          if (isCloud(o)) o.identity = v;
        }}
      />
    {/if}
    <Advanced
      label={o.type === 'sftp' ? 'Delivery' : 'Endpoint and delivery'}
      modified={objectModified(o)}
    >
      <ObjectStoreAccess store={o.type} value={o} show="advanced" />
      {#if o.type === 's3'}
        <Field field="kmsKeyId" label="KMS key id">
          {#snippet help()}
            The KMS key each object is encrypted under. Left empty, the bucket's default encryption.
          {/snippet}
          <input
            type="text"
            value={o.kmsKeyId ?? ''}
            oninput={(e) => (o.kmsKeyId = optional(e.currentTarget.value))}
            placeholder="the bucket's default"
          />
        </Field>
      {/if}
      <Field field="compression" label="compression">
        {#snippet help()}
          How each object is compressed; the key's extension says which. Parquet and Avro compress
          inside the file.
        {/snippet}
        {#snippet note()}Default gzip.{/snippet}
        <select
          value={o.compression ?? 'gzip'}
          onchange={(e) => {
            const v = e.currentTarget.value as Compression;
            if (v === 'gzip') delete o.compression;
            else o.compression = v;
          }}
        >
          {#each COMPRESSIONS.filter((c) => o.format !== 'avro' || c !== 'lz4') as codec (codec)}
            <option value={codec}
              >{o.format === 'avro' && codec === 'gzip' ? 'gzip (deflate)' : codec}</option
            >
          {/each}
        </select>
      </Field>
      {#if isCloud(o)}
        <Field field="storageClass" label="storage class">
          {#snippet help()}
            What each object is stored as, among the cloud's own classes; Azure calls it the access
            tier. The bucket default is whatever the bucket itself sets.
          {/snippet}
          <select
            value={o.storageClass ?? ''}
            onchange={(e) => {
              if (isCloud(o)) o.storageClass = optional(e.currentTarget.value);
            }}
          >
            <option value="">bucket default</option>
            {#each STORAGE_CLASSES[o.type] as c (c)}
              <option value={c}>{c}</option>
            {/each}
          </select>
        </Field>
      {/if}
      <Field field="inFlight" label="parts uploading at once">
        {#snippet help()}
          How many parts of one file upload at once.
        {/snippet}
        {#snippet note()}Default {LIMITS.inFlight.default}.{/snippet}
        <input
          type="number"
          min={LIMITS.inFlight.min}
          max={LIMITS.inFlight.max}
          value={o.inFlight ?? LIMITS.inFlight.default}
          oninput={(e) => (o.inFlight = numberOrUnset(e.currentTarget))}
        />
      </Field>
      <Field field="maxObjectBytes" label="largest file">
        {#snippet help()}
          The largest file this destination puts in the bucket, compressed. Set it to what the tools
          that read the bucket handle best.
        {/snippet}
        {#snippet note()}Default {exactBytes(LIMITS.maxObjectBytes.default)}.{/snippet}
        <BytesInput
          value={o.maxObjectBytes}
          fallback={LIMITS.maxObjectBytes.default}
          onchange={(n) => (o.maxObjectBytes = n)}
        />
      </Field>
      <Field field="bufferBytes" label="memory">
        {#snippet help()}
          Memory this destination may use on each worker for records not yet in the bucket. Files
          are finished early when it fills, so files reach the largest size only when it holds that
          many records before compression.
        {/snippet}
        {#snippet note()}Default {exactBytes(LIMITS.objectBufferBytes.default)}.{/snippet}
        <BytesInput
          value={o.bufferBytes}
          fallback={LIMITS.objectBufferBytes.default}
          onchange={(n) => (o.bufferBytes = n)}
        />
      </Field>
      <Field field="stallAfterSecs" label="stall after (seconds)">
        {#snippet help()}
          How long one store call, or one finishing of the files, may take before it counts as a
          stall. {#if o.type === 'sftp'}At least {SFTP_MIN_STALL_AFTER} on SFTP, so a sign-in finishes
            within it.{/if}
        {/snippet}
        {#snippet note()}Default {LIMITS.stallAfterSecs.default}.{/snippet}
        <input
          type="number"
          min={o.type === 'sftp' ? SFTP_MIN_STALL_AFTER : LIMITS.stallAfterSecs.min}
          max={LIMITS.stallAfterSecs.max}
          value={o.stallAfterSecs ?? LIMITS.stallAfterSecs.default}
          oninput={(e) => (o.stallAfterSecs = numberOrUnset(e.currentTarget))}
        />
      </Field>
      <Field field="fileIntervalSecs" label="file interval (seconds)">
        {#snippet help()}
          The longest records wait before they land in the bucket. A longer wait makes larger files.
          Records are acknowledged only once they land, so every source feeding this destination is
          held open for up to this long, and after a crash reads up to this long of records again.
        {/snippet}
        {#snippet note()}
          {@const interval = o.fileIntervalSecs ?? LIMITS.fileIntervalSecs.default}
          {#if interval > LIMITS.fileIntervalSecs.default}
            <span class="warn"
              >Sources feeding this destination are held open for up to {interval} seconds.</span
            >
          {:else}
            Default {LIMITS.fileIntervalSecs.default}.
          {/if}
        {/snippet}
        <input
          type="number"
          min={LIMITS.fileIntervalSecs.min}
          max={LIMITS.fileIntervalSecs.max}
          value={o.fileIntervalSecs ?? LIMITS.fileIntervalSecs.default}
          oninput={(e) => (o.fileIntervalSecs = numberOrUnset(e.currentTarget))}
        />
      </Field>
    </Advanced>
  {/if}
{/snippet}

{#snippet formatFields(
  d: Formatted,
  formats: readonly string[],
  fallback: string,
  help: Snippet<[readonly string[]]>,
  labels: Record<string, string> = {},
)}
  {#snippet formatHelp()}{@render help(formats)}{/snippet}
  <Field field="format" label="format" help={formatHelp}>
    <select
      value={d.format ?? fallback}
      onchange={(e) => {
        const v = e.currentTarget.value;
        // text and bytes default their field apart, so one named for either does not carry to the other.
        if (v !== (d.format ?? fallback)) delete d.field;
        if (v === fallback) delete d.format;
        else d.format = v;
        // Only bytes reads newlines; the load refuses it beside any other format.
        if (v !== 'bytes') delete d.newlines;
        // Only a stream's Avro registers in a schema registry, and an Avro file has no lz4.
        if (v !== 'avro') delete d.schemaRegistry;
        else if (formats === AVRO_STREAM_FORMATS) d.schemaRegistry ??= { url: '' };
        if (v === 'avro' && d.compression === 'lz4') delete d.compression;
      }}
    >
      {#each formats as f (f)}<option value={f}>{labels[f] ?? f}</option>{/each}
    </select>
  </Field>
  {#if d.format === 'avro' && d.schemaRegistry}
    <SchemaRegistryFields registry={d.schemaRegistry} {secrets} />
  {/if}
  {#if d.format === 'text' || d.format === 'bytes'}
    {@const fieldDefault = d.format === 'text' ? '_raw' : '__source.bytes'}
    <Field field="field" label={`${d.format} field`}>
      {#snippet note()}Default <code>{fieldDefault}</code>.{/snippet}
      <input
        type="text"
        value={d.field ?? ''}
        oninput={(e) => (d.field = optional(e.currentTarget.value))}
        placeholder={fieldDefault}
      />
    </Field>
  {/if}
{/snippet}

{#snippet streamFormatHelp(formats: readonly string[])}
  <code>json</code>: the record as JSON.<br />
  <code>text</code>: one field's text.
  {#if formats.includes('bytes')}<br /><code>bytes</code>: one field's bytes, once per record; by
    default <code>__source.bytes</code>, each source record's payload as read.{/if}
  {#if formats.includes('avro')}<br /><code>avro</code>: the record as Avro, its schema registered
    in the schema registry.{/if}
{/snippet}

{#snippet batchedHelp()}
  <code>ndjson</code>: one JSON record per line.<br />
  <code>json array</code>: one JSON array of records.<br />
  <code>text</code>: one field's text per line, each line break in it written as
  <code>\n</code>.<br />
  <code>text array</code>: the same text as a JSON array of strings.<br />
  <code>bytes</code>: one field's bytes per line, as base64; by default
  <code>__source.bytes</code>, each source record's payload as read.<br />
  <code>bytes array</code>: the same bytes as a JSON array of base64 strings.
{/snippet}

{#snippet objectFormatHelp()}
  <code>ndjson</code>: one JSON document per line.<br />
  <code>json</code>: one JSON array.<br />
  <code>parquet</code>: one Parquet file, its columns typed as the pipelines write them.<br />
  <code>avro</code>: one Avro file, its fields typed as the pipelines write them.<br />
  <code>csv</code>: one CSV file, a header row of each column declared, then a row for each
  document.<br />
  <code>text</code>: one line per row, the bare text of one field.<br />
  <code>bytes</code>: one line per source record, the bytes of one field; by default
  <code>__source.bytes</code>, each source record's payload as read.
{/snippet}

{@render fields(destination)}

<style>
  .warn {
    color: var(--warn);
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

  code {
    font-family: var(--mono);
    font-size: 0.9em;
  }

  input,
  select {
    font-family: var(--mono);
  }
</style>
