<!--
  How a connector reaches a service and signs in to it, per kind: the
  credential, and the fields that say where and as whom. The object store
  destinations and the object sources (SQS/S3 and S3 as `s3`,
  Queue/Azure Blob and Azure Blob as `azureBlob`, Pub/Sub/GCS and
  GCS as `gcs`, SFTP as `sftp`), the Iceberg destination's storage
  (`iceberg`), the Event Hubs source and destination (`eventHubs`), the
  Log Analytics destination (`logAnalytics`) and the Elasticsearch destination
  (`elasticsearch`) draw the same block, so a config's access reads alike wherever it is written.

  Mutates `value` in place, as `KafkaSecurity` does; the page's Save commits.
  `show` picks the half: the credential, which every form shows, or the rest,
  which sits under the form's Advanced. An SFTP server's address, user and
  host key have no default, so they sit in the credential's half.
-->
<script module lang="ts">
  import { SFTP_PORT, type ObjectStore } from './api';

  /** Whose access the block draws: an object store's, an Iceberg table's S3 storage, which names no credential
   * of its own when the catalog vends one, an Event Hubs namespace's, a data collection rule's, or an
   * Elasticsearch cluster's. */
  export type AccessKind = ObjectStore | 'iceberg' | 'eventHubs' | 'logAnalytics' | 'elasticsearch';

  const fields = (d: object) => d as Record<string, unknown>;

  /** A field that means something only beside another, per kind: shown only once that one is set, and unset with
   * it, so the form never writes one the load refuses alone. */
  const NEEDS: Record<AccessKind, Record<string, string>> = {
    s3: { externalId: 'roleArn', authEndpoint: 'roleArn' },
    gcs: {},
    azureBlob: { authEndpoint: 'clientSecret' },
    sftp: {},
    elasticsearch: {},
    iceberg: { externalId: 'roleArn', authEndpoint: 'roleArn' },
    eventHubs: { authEndpoint: 'clientSecret' },
    logAnalytics: { authEndpoint: 'clientSecret' },
  };

  export function shown(store: AccessKind, d: object, key: string): boolean {
    const need = NEEDS[store][key];
    return need === undefined || fields(d)[need] !== undefined;
  }

  /** Sets one of `d`'s access fields; `undefined` unsets it and every field that needs it. */
  export function setField(store: AccessKind, d: object, key: string, value: unknown) {
    if (value !== undefined) {
      fields(d)[key] = value;
      return;
    }
    delete fields(d)[key];
    for (const [dependent, need] of Object.entries(NEEDS[store])) {
      if (need === key) setField(store, d, dependent, undefined);
    }
  }

  /** One field a credential is made of: a secret reference, or a plain name, with what to know about it wherever
   * it is drawn. */
  export type CredentialField = { key: string; name: string; secret: boolean; help?: string };

  type Credential = { label: string; fields: CredentialField[] };

  /** The Azure credentials beyond a store's own keys, alike wherever Entra signs a client in. */
  const AZURE_HOST: Credential = { label: "the host's managed identity", fields: [] };
  const AZURE_MANAGED_IDENTITY: Credential = {
    label: 'a user-assigned managed identity',
    fields: [
      {
        key: 'managedIdentityClientId',
        name: 'client id',
        secret: false,
        help: "The client id of the managed identity to sign in as. Pick this when the worker's host has more than one managed identity.",
      },
    ],
  };
  const AZURE_SERVICE_PRINCIPAL: Credential = {
    label: 'a service principal',
    fields: [
      { key: 'tenantId', name: 'tenant id', secret: false },
      { key: 'clientId', name: 'client id', secret: false },
      { key: 'clientSecret', name: 'client secret', secret: true },
    ],
  };
  const AZURE_AUTH_ENDPOINT = {
    key: 'authEndpoint',
    name: 'auth endpoint',
    placeholder: "its cloud's own",
    help: "Where the service principal's client secret is exchanged for a token. Left empty, its cloud's own.",
  };

  /** Where a client's credential comes from, per kind: the host's own, or what the config names. One choice
   * per config, so two can never be named at once, and choosing one names all of its fields. The first entry
   * is what a config naming none has: a cloud's is the host's, which names nothing. An SFTP server has no host's
   * credential, so its config always names one. */
  export const CREDENTIALS: Record<AccessKind, Record<string, Credential>> = {
    s3: {
      host: { label: "the host's credentials", fields: [] },
      accessKey: {
        label: 'an access key',
        fields: [
          { key: 'accessKeyId', name: 'access key id', secret: true },
          { key: 'secretAccessKey', name: 'secret access key', secret: true },
        ],
      },
    },
    gcs: {
      host: { label: "the host's Application Default Credentials", fields: [] },
      serviceAccountKey: {
        label: 'a service account key',
        fields: [{ key: 'serviceAccountKey', name: 'service account key', secret: true }],
      },
    },
    azureBlob: {
      host: AZURE_HOST,
      managedIdentity: AZURE_MANAGED_IDENTITY,
      accessKey: {
        label: 'an account key',
        fields: [{ key: 'accessKey', name: 'account key', secret: true }],
      },
      sasToken: {
        label: 'a SAS token',
        fields: [{ key: 'sasToken', name: 'SAS token', secret: true }],
      },
      servicePrincipal: AZURE_SERVICE_PRINCIPAL,
    },
    sftp: {
      password: {
        label: 'a password',
        fields: [{ key: 'password', name: 'password', secret: true }],
      },
      privateKey: {
        label: 'a private key',
        fields: [{ key: 'privateKey', name: 'private key', secret: true }],
      },
    },
    elasticsearch: {
      none: { label: 'none', fields: [] },
      apiKey: {
        label: 'an API key',
        fields: [{ key: 'auth', name: 'API key', secret: true }],
      },
      basic: {
        label: 'a username and password',
        fields: [
          { key: 'username', name: 'username', secret: false },
          { key: 'password', name: 'password', secret: true },
        ],
      },
    },
    eventHubs: {
      host: AZURE_HOST,
      managedIdentity: AZURE_MANAGED_IDENTITY,
      connectionString: {
        label: 'a connection string',
        fields: [{ key: 'connectionString', name: 'connection string', secret: true }],
      },
      servicePrincipal: AZURE_SERVICE_PRINCIPAL,
    },
    logAnalytics: {
      host: AZURE_HOST,
      managedIdentity: AZURE_MANAGED_IDENTITY,
      servicePrincipal: AZURE_SERVICE_PRINCIPAL,
    },
    iceberg: {
      catalog: { label: "what the catalog vends, else the host's", fields: [] },
      accessKey: {
        label: 'an access key',
        fields: [
          { key: 'accessKeyId', name: 'access key id', secret: true },
          { key: 'secretAccessKey', name: 'secret access key', secret: true },
        ],
      },
    },
  };

  /** The credential `d` names: the first whose fields it carries any of, or the first entry when it names none. */
  export function credentialOf(store: AccessKind, d: object): string {
    const choices = Object.entries(CREDENTIALS[store]);
    const named = choices.find(([, c]) => c.fields.some((f) => fields(d)[f.key] !== undefined));
    return (named ?? choices[0])[0];
  }

  /** Unsets every other credential's fields and names `choice`'s, empty until they are filled in. */
  export function setCredential(store: AccessKind, d: object, choice: string) {
    for (const [name, c] of Object.entries(CREDENTIALS[store])) {
      if (name === choice) continue;
      for (const f of c.fields) setField(store, d, f.key, undefined);
    }
    for (const f of CREDENTIALS[store][choice]?.fields ?? []) {
      if (fields(d)[f.key] === undefined) setField(store, d, f.key, '');
    }
  }

  /** One optional text field beside the credential: where the client reaches and how it signs in beyond the
   * credential. Absent is each one's default, so they sit under Advanced. */
  export type AccessField = { key: string; name: string; placeholder: string; help: string };

  export const ACCESS_FIELDS: Record<AccessKind, AccessField[]> = {
    s3: [
      {
        key: 'region',
        name: 'region',
        placeholder: "the host's",
        help: "The AWS region. Left empty, the host's own configuration names it.",
      },
      {
        key: 'endpoint',
        name: 'endpoint',
        placeholder: 'https://s3.amazonaws.com',
        help: "An S3-compatible service's address. Left empty, AWS.",
      },
      {
        key: 'roleArn',
        name: 'role ARN',
        placeholder: '',
        help: 'A role to assume with the credential, for resources in another account.',
      },
      {
        key: 'externalId',
        name: 'external id',
        placeholder: '',
        help: "The external id the role's trust policy asks for.",
      },
      {
        key: 'authEndpoint',
        name: 'auth endpoint',
        placeholder: "the region's STS",
        help: "Where the role is assumed: an STS address. Left empty, the region's own.",
      },
    ],
    gcs: [
      {
        key: 'endpoint',
        name: 'endpoint',
        placeholder: 'https://storage.googleapis.com',
        help: "Another address, such as an emulator's. Left empty, Google's.",
      },
    ],
    azureBlob: [
      {
        key: 'endpoint',
        name: 'endpoint',
        placeholder: 'https://{account}.blob.core.windows.net',
        help: "Another address, such as Azurite's or a sovereign cloud's. Left empty, the account's own in Azure's public cloud.",
      },
      AZURE_AUTH_ENDPOINT,
    ],
    sftp: [],
    elasticsearch: [],
    eventHubs: [AZURE_AUTH_ENDPOINT],
    logAnalytics: [AZURE_AUTH_ENDPOINT],
    iceberg: [
      {
        key: 'region',
        name: 'region',
        placeholder: "the catalog's, else the host's",
        help: "The bucket's region, and with AWS signatures the catalog's when its address names none. Left empty, the region the catalog gives the table, else the one its AWS address names, else the host's.",
      },
      {
        key: 'endpoint',
        name: 'endpoint',
        placeholder: "the catalog's, else AWS",
        help: "An S3-compatible service's address. Left empty, the one the catalog gives the table, else AWS.",
      },
      {
        key: 'roleArn',
        name: 'role ARN',
        placeholder: '',
        help: 'A role to assume, for a bucket in another account, in place of what the catalog vends.',
      },
      {
        key: 'externalId',
        name: 'external id',
        placeholder: '',
        help: "The external id the role's trust policy asks for.",
      },
      {
        key: 'authEndpoint',
        name: 'auth endpoint',
        placeholder: "the region's STS",
        help: "Where the role is assumed: an STS address. Left empty, the region's own.",
      },
    ],
  };

  /** For the Advanced header's marker: whether any field the Advanced half shows is set. */
  export function accessModified(store: AccessKind, d: object): boolean {
    return ACCESS_FIELDS[store].some((f) => fields(d)[f.key] !== undefined);
  }
</script>

<script lang="ts">
  import Field from './Field.svelte';
  import Nested from './Nested.svelte';
  import UrlField from './UrlField.svelte';
  import SecretRefInput from './SecretRefInput.svelte';
  import { numberOrUnset } from './page';
  import type { SecretDto, SecretRef } from './api';

  let {
    store,
    value,
    show,
    secrets = [],
    errors = {},
    help = {},
  }: {
    store: AccessKind;
    /** The config the access fields sit in, flattened. */
    value: object;
    show: 'credential' | 'advanced';
    secrets?: SecretDto[];
    /** Why a field cannot be saved, by its key, for a refusal the form can tell before the save. */
    errors?: Record<string, string | undefined>;
    /** What to know about a credential's field when its name does not say, by its key: what differs between the
     * forms that draw this block. */
    help?: Record<string, string | undefined>;
  } = $props();

  const optional = (v: string) => (v.trim() === '' ? undefined : v);

  /** Absent is the default port, so choosing it writes nothing. */
  function setPort(input: HTMLInputElement) {
    const port = numberOrUnset(input);
    setField(store, value, 'port', port === SFTP_PORT ? undefined : port);
  }
</script>

<!-- One required text field of an SFTP server's. -->
{#snippet serverField(key: string, label: string, about: string, placeholder = '')}
  <Field field={key} {label}>
    {#snippet help()}{about}{/snippet}
    <input
      type="text"
      value={(fields(value)[key] as string | undefined) ?? ''}
      oninput={(e) => setField(store, value, key, e.currentTarget.value)}
      {placeholder}
    />
  </Field>
{/snippet}

{#if show === 'credential'}
  {@const choice = credentialOf(store, value)}
  {@const credentialFields = CREDENTIALS[store][choice]?.fields ?? []}
  {#if store === 'sftp'}
    <div class="pair">
      {@render serverField('host', 'host', "The server's host name or address.")}
      <Field field="port" label="port">
        {#snippet help()}The server's SSH port.{/snippet}
        <input
          type="number"
          min="1"
          max="65535"
          value={(fields(value).port as number | undefined) ?? SFTP_PORT}
          oninput={(e) => setPort(e.currentTarget)}
        />
      </Field>
    </div>
    {@render serverField('username', 'username', 'The user it signs in as.')}
  {/if}
  <Field label={store === 'iceberg' ? 'storage credential' : 'credential'}>
    {#snippet help()}
      {#if store === 'sftp'}
        What the user signs in with: a password, or an unencrypted private key in OpenSSH's format.
      {:else if store === 'elasticsearch'}
        What the client signs in with. A cluster with security off needs none.
      {:else if store === 'iceberg'}
        What the table's storage is signed with. A catalog that vends credentials needs nothing
        named here, nor does the host's own. A role ARN is assumed in place of what the catalog
        vends.
      {:else}
        What the client signs in with. The host's own needs nothing named here.
        {#if store === 'logAnalytics'}See Microsoft's <a
            class="link"
            href="https://learn.microsoft.com/azure/azure-monitor/logs/logs-ingestion-api-overview"
            target="_blank"
            rel="noreferrer">Logs Ingestion API docs</a
          > for the role it needs.{/if}
      {/if}
    {/snippet}
    <select value={choice} onchange={(e) => setCredential(store, value, e.currentTarget.value)}>
      {#each Object.entries(CREDENTIALS[store]) as [name, c] (name)}
        <option value={name}>{c.label}</option>
      {/each}
    </select>
  </Field>
  {#if credentialFields.length > 0}
    <Nested>
      {#each credentialFields as f (f.key)}
        {@const v = fields(value)[f.key]}
        {@const about = help[f.key] ?? f.help}
        {#snippet fieldHelp()}{about}{/snippet}
        <Field field={f.key} label={f.name} help={about ? fieldHelp : undefined}>
          {#if f.secret}
            <SecretRefInput
              value={v as SecretRef | undefined}
              {secrets}
              label={f.name}
              onchange={(r) => setField(store, value, f.key, r ?? '')}
            />
          {:else}
            <input
              type="text"
              value={(v as string | undefined) ?? ''}
              oninput={(e) => setField(store, value, f.key, e.currentTarget.value)}
            />
          {/if}
        </Field>
      {/each}
    </Nested>
  {/if}
  {#if store === 'sftp'}
    {@render serverField(
      'hostKey',
      'host key',
      "The server's public key as OpenSSH writes it, without a host name: it connects to that server and no other. Copy it from the server's /etc/ssh/ssh_host_ed25519_key.pub, or from ssh-keyscan, dropping the host name it starts with.",
      'ssh-ed25519 AAAA…',
    )}
  {/if}
{:else}
  {#each ACCESS_FIELDS[store] as f (f.key)}
    {#if shown(store, value, f.key) && /endpoint$/i.test(f.key)}
      <UrlField
        field={f.key}
        label={f.name}
        bind:value={
          () => fields(value)[f.key] as string | undefined, (v) => setField(store, value, f.key, v)
        }
        sends
        optional
        mono
        error={errors[f.key] ?? null}
        placeholder={f.placeholder}
      >
        {#snippet help()}{f.help}{/snippet}
      </UrlField>
    {:else if shown(store, value, f.key)}
      <Field field={f.key} label={f.name} error={errors[f.key] ?? null}>
        {#snippet help()}{f.help}{/snippet}
        <input
          type="text"
          value={(fields(value)[f.key] as string | undefined) ?? ''}
          oninput={(e) => setField(store, value, f.key, optional(e.currentTarget.value))}
          placeholder={f.placeholder}
        />
      </Field>
    {/if}
  {/each}
{/if}

<style>
  input,
  select {
    font-family: var(--mono);
  }

  /* The host and its port, side by side: the port has no meaning without the host. */
  .pair {
    display: flex;
    gap: 0.8rem;
    align-items: flex-start;
  }

  .pair > :global(*) {
    flex: 1;
  }
</style>
