// Typed client for the config-bundle REST API (`src/bin/smeltr/config_api.rs`).
// Every handler answers with a JSON body carrying `ok`, on any status, so the
// status is what says whether a body is an answer or a refusal. A non-JSON body
// is a framework rejection (a request body the handler could not deserialize).
// Both surface as a thrown error.

export interface ApiError {
  /** The pipeline this came from, by its id, where one is known. */
  pipeline: string | null;
  /** The step that produced it, by its own id. What places a message on a
   * step: the compiler's index cannot, because a `call` is one step in the
   * editor and its callee's steps once inlined. */
  step: string | null;
  function: number | null;
  /** The eval assignment, by its position in the step's own list. Kept in
   * step with `PreviewError`'s own field (`pipeline.ts`) so the two share one
   * shape for `errorsByStep`. */
  assignmentIndex: number | null;
  /** The source, destination or identity this is about: its section of the
   * bundle and its id there. */
  item: { section: string; id: string } | null;
  /** The setting this is about, by its name in the file: a key of `item`, or
   * of the assignment. What `Validation` places a message by. */
  field: string | null;
  /** The route's delivery rule `field` is a setting of, by its position in
   * the route's `deliver`. */
  rule: number | null;
  message: string;
}

/** A branch that is not where the editor read it, as two commits rather than
 * as a sentence. The editor holds the only copy of the refused change, so it
 * has to know what it has and what the branch became without reading the
 * message. */
export type SaveConflict = {
  /** The commit the editor read and saved against. */
  base: string;
  /** Where the branch is now. Absent when it is gone. */
  now?: string;
};

export interface MutationResult {
  ok: boolean;
  errors: ApiError[];
  /** The commit this save produced. Absent when the save was refused. */
  commit?: string;
  /** Present when the branch moving is why the save was refused. */
  conflict?: SaveConflict;
}

import { funnyBranchName } from './branchNames';
import { csrf, recheck, signedOut, type AccessDto, type AccessLevel } from './session';
import { bareError, errors, Reported, type Call, type Weight } from './errors.svelte';

export function errorText(errors: ApiError[]): string {
  return errors.map((e) => e.message).join('; ');
}

/** The message of a caught value, for display. */
export function errorMessage(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

// -- DTO shapes: the JSON wire format of `src/config_file/dto.rs` ----------

/** A credential a bundle names, never carries. A bare string names an
 * environment variable on the worker's own host; an object names a value
 * the leader holds, at the version the deployment is built for. */
export type SecretRef = string | LeaderSecret;
export type LeaderSecret = { secret: string; version: number };

/** Whether this names a value the leader holds rather than a variable on the
 * worker's host. */
export const isLeaderStored = (r: SecretRef | undefined): r is LeaderSecret =>
  typeof r === 'object' && r !== null;

/** How a leader-stored reference reads where there is no picker for one. */
export const secretLabel = (r: LeaderSecret) => `${r.secret} @ v${r.version}`;

/** One identity a worker connects out under: what it shows a source or a
 * destination, and which authorities it verifies that far end against.
 *
 * A far end that asks for no certificate needs only the authorities, and one
 * served from a public root needs only the certificate, so either half may be
 * absent -- but not both, which the save refuses. A certificate is public and
 * is committed; its private key is a credential and is only named, like every
 * other. A client under it trusts the public roots and `authorities` both,
 * unless `trustPublic` is false. A receiver admits by `authorities` alone. */
export type IdentityDto = {
  id: string;
  description?: string;
  /** The certificate it presents, from the list, by name and version. A string is a certificate pasted into a
   * bundle written before identities named one from the list: the leader refuses it, and says to choose one. */
  certificate?: SecretRef;
  /** The private key such a bundle named beside its pasted certificate. Refused the same way. */
  key?: SecretRef;
  /** PEM. A trust store holds more than one root, so several may follow one another. */
  authorities?: string;
  /** Absent is true. False trusts `authorities` alone. */
  trustPublic?: boolean;
};

/** Extra librdkafka properties, split by whether the value is a secret.
 *
 * `properties` is committed to the config repository, so it holds only
 * values that belong there (`security.protocol`, `ssl.ca.location`).
 * `credentials` maps a property name to where its value lives, so a
 * password reaches a worker without reaching git. The loader refuses a
 * credential-valued property in the first map.
 *
 * Properties smeltr sets itself (`brokers`, `groupId`, `compression`,
 * `fetchBytes`, `bufferBytes`, `start`) are refused in both maps, and so are the fetch and batch sizes the worker
 * derives from those two at deployment. */
export type KafkaProps = {
  properties?: Record<string, string>;
  credentials?: Record<string, SecretRef>;
};

import type { ByteCount } from './bytes';
import type { StartPosition, StopPosition } from './kafka';
export type { StartPosition, StopPosition } from './kafka';
import type { Answer, AnswerName, ExceptionsDto } from './exceptions';
export type { Answer, ExceptionsDto } from './exceptions';

/** Where an SQL source connects, and what it reads: `postgres` and `mysql` alike. */
export interface SqlConnection {
  id: string;
  host: string;
  /** Absent is the kind's own: 5432 for `postgres`, 3306 for `mysql`. */
  port?: number;
  database: string;
  user: string;
  /** The credential holding the user's password. Absent is a server that asks for none. */
  password?: SecretRef;
  /** Absent is `require`, which encrypts and verifies the server's certificate and name. */
  tls?: 'require' | 'disable';
  /** The identity it connects under, by id. Only with `tls` `require`. */
  identity?: string;
  /** A `SELECT` or a `WITH`, with no `ORDER BY` or `LIMIT` of its own. */
  query: string;
  mode: SqlMode;
  /** Each field of the record, read from the result column of the same name. */
  fields: SqlField[];
  /** Absent is 60. */
  pollIntervalSecs?: number;
  /** The most rows one poll reads by a position, and every poll sends. Absent is 5000. */
  batch?: number;
}

/** A source. Switching `type` replaces the whole object. */
export type SourceDto =
  | ({
      id: string;
      type: 'kafka';
      brokers: string;
      topics: string[];
      /** The consumer group it reads as. Absent is its worker group's own, `smeltr.<worker group>`. */
      groupId?: string;
      /** The identity this consumer connects under, by id. */
      identity?: string;
      /** Where the read begins. Absent is `earliest`, which is what a
       * source that decided nothing gets. */
      start?: StartPosition;
      /** Where it ends, exclusive. Absent keeps reading. */
      stop?: StopPosition;
      fetchBytes?: ByteCount;
      /** `auto` (absent) detects each record's encoding; anything else is a
       * WHATWG label the save validates. */
      encoding?: string;
    } & StreamReadFormat &
      KafkaProps)
  | (EventHubsSignIn & {
      id: string;
      type: 'eventHubs';
      /** The namespace: its name in Azure's public cloud, its host name in
       * another cloud, or `host:port`. */
      namespace: string;
      /** The event hubs it reads, each a Kafka topic. */
      eventHubs: string[];
      consumerGroup: string;
      /** As a Kafka source's. */
      start?: StartPosition;
      stop?: StopPosition;
      fetchBytes?: ByteCount;
      encoding?: string;
    } & StreamReadFormat)
  | (S3Access & {
      id: string;
      type: 's3Sqs';
      /** The queue's own URL, which names its region and account. */
      queueUrl: string;
      /** The identity this client connects under, by id. */
      identity?: string;
      /** Absent is `lines`. */
      framing?: Framing;
      lineBreaker?: string;
      /** As a Kafka source's. */
      encoding?: string;
      visibilitySecs?: number;
    } & ReadFormat)
  | (AzureAccess & {
      id: string;
      type: 'azureBlobQueue';
      /** The Storage queue's own URL. */
      queueUrl: string;
      identity?: string;
      framing?: Framing;
      lineBreaker?: string;
      encoding?: string;
      visibilitySecs?: number;
      /** Receives before a message is moved to `<queue>-poison`. */
      maxDeliveries?: number;
    } & ReadFormat)
  | (GcsAccess & {
      id: string;
      type: 'gcsPubSub';
      /** `projects/<project>/subscriptions/<subscription>`. */
      subscription: string;
      /** Another Pub/Sub address, such as an emulator's. Absent is Google's. */
      pubsubEndpoint?: string;
      identity?: string;
      framing?: Framing;
      lineBreaker?: string;
      encoding?: string;
      /** The ack deadline: at most `LIMITS.ackDeadlineSecs.max`. */
      visibilitySecs?: number;
    } & ReadFormat)
  | (S3Access &
      ListingSettings & {
        id: string;
        type: 's3List';
        bucket: string;
        identity?: string;
      })
  | (GcsAccess &
      ListingSettings & {
        id: string;
        type: 'gcsList';
        bucket: string;
        identity?: string;
      })
  | (AzureAccess &
      ListingSettings & {
        id: string;
        type: 'azureBlobList';
        container: string;
        identity?: string;
      })
  | (SftpAccess &
      ListingSettings & {
        id: string;
        type: 'sftp';
        /** Absolute, or under the user's home directory. Absent is the home directory. */
        directory?: string;
      })
  | (ApiSettings & {
      id: string;
      type: 'httpApi';
      /** The whole address asked, with any fixed query parameters. */
      url: string;
      /** Absent is `GET`. */
      method?: 'GET' | 'POST';
      /** The JSON every POST carries. Absent is a POST of nothing. */
      body?: unknown;
      /** Header name to its value, committed to the config repository. */
      headers?: Record<string, string>;
      /** Header name to where its value lives. */
      credentials?: Record<string, SecretRef>;
      /** Absent is `body`. */
      records?: ApiRecords;
      /** The JSON pointer to the array `array` reads. Absent is an answer that is the array. */
      recordsAt?: string;
      /** Whether a poll follows each answer's `Link: rel="next"`. Absent is not. */
      followLinks?: boolean;
      /** The query parameter that asks for what happened since a time. Absent reads everything every poll. */
      sinceParam?: string;
      /** Seconds before the last poll's start `sinceParam` asks from. Absent is 0. */
      sinceOverlapSecs?: number;
      /** The OAuth client it signs in as, with `clientSecret` or `privateKey`. Absent sends only its headers. */
      clientId?: string;
      clientSecret?: SecretRef;
      /** An RSA key as PEM PKCS#8, in place of `clientSecret`. */
      privateKey?: SecretRef;
      /** Which of the client's keys `privateKey` is, for a client with several. */
      keyId?: string;
      /** Where the token is obtained. A sign-in names `tokenUrl` or `issuer`, never both. */
      tokenUrl?: string;
      /** The OpenID issuer whose discovery document names the token endpoint. */
      issuer?: string;
      scope?: string;
    })
  | (ApiSettings & {
      id: string;
      type: 'oktaLogs';
      /** The org's address, as `https://acme.okta.com`. */
      org: string;
      /** An API token. A source names a token or a service app, never both. */
      token?: SecretRef;
      /** The service app's client id, with `privateKey`. */
      clientId?: string;
      privateKey?: SecretRef;
      /** Which of the app's keys `privateKey` is, for an app with several. */
      keyId?: string;
    })
  | (ReceiverSettings & {
      id: string;
      type: 'httpReceiver';
      /** The path senders POST to. Absent is `/`. */
      path?: string;
      /** Token name to the credential holding a token a sender presents as `Authorization: Bearer`. */
      tokens?: Record<string, SecretRef>;
      /** Absent is `body`. */
      records?: ApiRecords;
      /** The JSON pointer to the array `array` reads. Absent is a body that is the array. */
      recordsAt?: string;
    })
  | (ReceiverSettings & {
      id: string;
      type: 'hecReceiver';
      /** Token name to a token a sender presents as `Authorization: Splunk`. */
      tokens?: Record<string, HecToken>;
      /** Sourcetype to where a raw request's events end, by the request's `sourcetype`. A sourcetype not named
       * here is an event per line. */
      sourcetypes?: Record<string, Sourcetype>;
    })
  | (ReceiverSettings & {
      id: string;
      type: 'otlpReceiver';
      /** The signal it takes. Absent is `logs`. The receivers of other signals may share its port. */
      signal?: OtlpSignal;
      /** Token name to the credential holding a token an exporter presents as `Authorization: Bearer`. */
      tokens?: Record<string, SecretRef>;
    })
  | (ReceiverSettings & {
      id: string;
      type: 'prometheusReceiver';
      /** Token name to the credential holding a token a sender presents as `Authorization: Bearer`. */
      tokens?: Record<string, SecretRef>;
    })
  | (ReceiverSettings & {
      id: string;
      type: 'elasticReceiver';
      /** Token name to the credential holding a token a sender presents as `Authorization: Bearer` or `ApiKey`, or
       * as a Basic password. */
      tokens?: Record<string, SecretRef>;
    })
  | (ReceiverSettings & {
      id: string;
      type: 's2sReceiver';
      /** Token name to the credential holding a token a forwarder presents, its outputs.conf `token`. */
      tokens?: Record<string, SecretRef>;
      /** Sourcetype to where its events end. A sourcetype not named here is an event per line. */
      sourcetypes?: Record<string, Sourcetype>;
    })
  | (ReceiverSettings & {
      id: string;
      type: 'syslogReceiver';
      /** The networks a sender may connect from, as CIDR blocks such as `10.0.0.0/8`. Syslog carries no token,
       * so this admits a sender, unless the identity names authorities a sender's certificate must chain to. */
      allowFrom?: string[];
      /** As a Kafka source's. */
      encoding?: string;
      /** What senders send over. Absent is `tcp`. Over `udp` each datagram is a message, served without TLS, so it
       * takes no identity and only `allowFrom` admits a sender. */
      transport?: 'tcp' | 'udp';
      /** Where a message over TCP ends, as an object source's `lineBreaker`. Absent, at a newline. Octet counting
       * still frames a message that starts with its length. */
      lineBreaker?: string;
    })
  | (SqlConnection & { type: 'postgres' })
  | (SqlConnection & { type: 'mysql' })
  | { id: string; type: 'metrics'; bucketSeconds: number }
  | {
      id: string;
      type: 'logs';
      /** The lowest severity it carries: `error`, `warn`, `info` (absent),
       * `debug` or `trace`. The process collects at `SMELTR_LOG_LEVEL`, and
       * what it never collected is not there to read. */
      minLevel?: string;
    };

/** How an SQL source knows a row is due, mirroring `config_file::SqlModeDto`. A position names the column that
 * orders the result, then the columns that tell apart rows with the same value in it. */
export type SqlMode =
  | {
      type: 'incrementing' | 'updatedAt';
      position: string[];
      /** How long to wait for late commits, for a time position. */
      settleSecs?: number;
      /** Where a source with no progress begins. Absent is `now`. */
      start?: 'now' | 'beginning' | 'from';
      /** Where `from` begins: an integer, or an ISO 8601 time with its offset. */
      startFrom?: number | string;
    }
  | { type: 'comparing'; key: string[] }
  | { type: 'everyPoll' };

/** How a source that reads payloads makes them its record. Absent `format` decodes each as text into `_raw`. With
 * `avro`, each payload is read into `fields`, and every top-level field they do not name is kept. An object is an
 * Avro container file, which holds its own schema. */
export interface ReadFormat {
  format?: 'avro';
  fields?: SqlField[];
}

/** A stream's `ReadFormat`: its Avro payloads are Confluent-framed with their schema's id in `schemaRegistry`. */
export interface StreamReadFormat extends ReadFormat {
  schemaRegistry?: SchemaRegistry;
}

/** A schema registry answering Confluent's API. `username` and `password` sign in together. */
export interface SchemaRegistry {
  url: string;
  username?: string;
  password?: SecretRef;
}

/** One field a source declares of its record: an SQL result's column or an Avro record's field, and the type it is
 * read as. */
export interface SqlField {
  name: string;
  type: string;
}

/** The types a source can declare a field as. */
export const FIELD_TYPES = [
  'i64',
  'f64',
  'bool',
  'str',
  'datetime',
  'duration',
  'bytes',
  'variant',
];

/** What Run found out about an SQL source's query (`heartbeat::SqlRun`). */
export interface SqlRun {
  columns: SqlColumn[];
  /** What the database plans for a poll, for a mode that reads past the last row. */
  verdict?: SqlVerdict;
}

/** One column of the query's result. */
export interface SqlColumn {
  name: string;
  /** As the database names it, such as `int8`. */
  databaseType: string;
  /** The smeltr type it reads as unless declared otherwise. */
  suggested: string;
  /** The table it is read from, schema-qualified, when it is a plain column of one. */
  table?: string;
  /** Whether the column is unique on that table by itself. */
  unique: boolean;
}

/** What the database plans for a poll: through `indexes`, or a scan of all of `relation`. `rows` is what it
 * expects to read. */
export type SqlVerdict =
  | { type: 'indexed'; indexes: string[]; rows: number }
  | { type: 'fullScan'; relation: string; rows: number };

/** What fills one placeholder of an SQL statement: a column's value in the last row sent, or the value the source
 * starts from. */
export type SqlBind = { last: string } | 'start';

export interface SqlStatement {
  sql: string;
  binds: SqlBind[];
}

export interface SqlStatementsResult {
  ok: boolean;
  errors: ApiError[];
  /** What a poll runs before any row is sent. */
  first?: SqlStatement;
  /** What a poll runs once a row is sent, for a mode that reads past the last row. */
  later?: SqlStatement;
  /** What Run runs: the result from the beginning, a few rows. */
  run?: SqlStatement;
}

/** The statements an SQL source runs, built by the leader as a poll builds them. Nothing connects. */
export function sqlStatement(
  source: SourceDto,
  identity?: IdentityDto,
): Promise<SqlStatementsResult> {
  return post('/api/sql/statement', { source, identity }, 'quiet');
}

/** Which of OpenTelemetry's signals an OTLP receiver takes, mirroring `plan::topology::Signal`. */
export type OtlpSignal = 'logs' | 'metrics' | 'traces';

/** What happens when the same document reaches Elasticsearch twice.
 * `auto` is plain at-least-once: a re-send after a crash indexes a second
 * copy. The other two key the document on a stable id and pay indexing
 * throughput for it. */
export type DocIdPolicy = 'auto' | 'dedupe' | 'overwrite';

/** A destination's codecs, mirroring `config_file::dto::CompressionDto`. A
 * closed set, so the editor offers a dropdown and no save can refuse one. */
export type Compression = 'none' | 'gzip' | 'snappy' | 'lz4' | 'zstd';
/** What an HTTP destination's requests may be compressed with: what `Content-Encoding` a service reliably takes. */
export type HttpCompression = Extract<Compression, 'none' | 'gzip'>;

/** How an HTTP destination puts a batch into requests, mirroring `config_file::dto::RequestBodyDto`: one JSON
 * document per line, one JSON array, or one row per request. */
export type HttpBody = 'lines' | 'jsonArray' | 'single';
export const COMPRESSIONS: Compression[] = ['none', 'gzip', 'snappy', 'lz4', 'zstd'];

/** Bounds and defaults of the numeric delivery settings, mirroring the
 * newtypes in `config_file::dto` (`BulkBytes`, `FetchBytes`, ...). The Rust
 * side is the authority; these keep the inputs from offering a value the
 * save would reject. */
export const LIMITS = {
  inFlight: { min: 1, max: 64, default: 4 },
  maxBulkBytes: { min: 64 * 1024, max: 100 * 1024 * 1024, default: 5 * 1024 * 1024 },
  maxRequestBytes: { min: 4 * 1024, max: 100 * 1024 * 1024, default: 1024 * 1024 },
  /** A Log Analytics destination's `maxRequestBytes`: at most what the Logs Ingestion API takes in one call. */
  logAnalyticsRequestBytes: { min: 4 * 1024, max: 1024 * 1024, default: 1024 * 1024 },
  /** A receiver's `maxRequestBytes` (`MaxRequestBytes`): at most what one request may hold of its inbox. */
  receiverRequestBytes: { min: 1024, max: 64 * 1024 * 1024, default: 10 * 1024 * 1024 },
  maxObjectBytes: { min: 64 * 1024, max: 64 * 1024 * 1024 * 1024, default: 64 * 1024 * 1024 },
  stallAfterSecs: { min: 1, max: 120, default: 30 },
  bufferBytes: { min: 4 * 1024 * 1024, max: 1024 * 1024 * 1024, default: 64 * 1024 * 1024 },
  /** An object store destination's `fileIntervalSecs` (`FileInterval`). */
  fileIntervalSecs: { min: 5, max: 3600, default: 60 },
  /** A table destination's `fileIntervalSecs` (`config_file::load::TABLE_FILE_INTERVAL_MAX`): a source waits that
   * long for its batches' acknowledgement, and a Kafka reader that waits past its poll interval is evicted. */
  tableFileIntervalSecs: { min: 5, max: 240, default: 60 },
  /** An object store destination's `bufferBytes`: it sizes the files (`BufferBytes::OBJECT_DEFAULT`). */
  objectBufferBytes: {
    min: 4 * 1024 * 1024,
    max: 32 * 1024 * 1024 * 1024 * 1024,
    default: 256 * 1024 * 1024,
  },
  fetchBytes: { min: 4 * 1024 * 1024, max: 512 * 1024 * 1024, default: 64 * 1024 * 1024 },
  visibilitySecs: { min: 1, max: 43_200, default: 60 },
  /** A Pub/Sub/GCS source's `visibilitySecs`: Pub/Sub's ack deadline is at most ten minutes. */
  ackDeadlineSecs: { min: 1, max: 600, default: 60 },
  maxDeliveries: { min: 1, max: 1000, default: 5 },
  /** The load bounds it only from below. */
  pollIntervalSecs: { min: 1, max: Number.MAX_SAFE_INTEGER, default: 60 },
} as const;

/** A destination, discriminated on `type`. A union rather than one flat
 * shape: the two kinds share only `id`, and the loader rejects unknown
 * fields, so a Kafka field left on an Elasticsearch destination would stop
 * the bundle loading. */
export type DestinationDto =
  | ({
      id: string;
      type: 'kafka';
      exceptions?: ExceptionsDto;
      brokers: string;
      defaultTopic: string;
      /** The identity this producer connects under, by id. */
      identity?: string;
      /** How each record is written. Absent is `json`. `avro` frames each row with its record's schema's id in
       * `schemaRegistry`. */
      format?: StreamFormat | 'avro';
      /** The field `text` or `bytes` writes, and only those. Absent is `_raw` for `text`, `__source.bytes` for
       * `bytes`. */
      field?: string;
      compression?: Compression;
      stallAfterSecs?: number;
      bufferBytes?: ByteCount;
      /** Where `format` is `avro`: the registry each route's record is registered with. */
      schemaRegistry?: SchemaRegistry;
    } & KafkaProps)
  | (EventHubsSignIn & {
      id: string;
      type: 'eventHubs';
      exceptions?: ExceptionsDto;
      /** The namespace: its name in Azure's public cloud, its host name in
       * another cloud, or `host:port`. */
      namespace: string;
      /** The event hub a row goes to when it names none. */
      defaultEventHub: string;
      /** As a Kafka destination's. */
      format?: StreamFormat | 'avro';
      field?: string;
      /** Event Hubs takes no other codec. */
      compression?: 'none' | 'gzip';
      stallAfterSecs?: number;
      bufferBytes?: ByteCount;
      schemaRegistry?: SchemaRegistry;
    })
  | {
      id: string;
      type: 'elasticsearch';
      exceptions?: ExceptionsDto;
      url: string;
      /** Zone to the address a worker in that zone sends to instead of `url`. */
      zoneUrls?: Record<string, string>;
      defaultIndex: string;
      /** The identity this client connects under, by id. */
      identity?: string;
      auth?: SecretRef;
      /** Basic auth, in place of `auth`: `username` and `password` together. */
      username?: string;
      password?: SecretRef;
      docId?: DocIdPolicy;
      idField?: string;
      inFlight?: number;
      maxBulkBytes?: ByteCount;
      bufferBytes?: ByteCount;
      stallAfterSecs?: number;
      /** Whether a worker learns the cluster's nodes from `url` and spreads its requests across them. */
      sniff?: boolean;
      /** The node attribute naming a node's zone. Only with `sniff`; absent is `zone`. */
      zoneAttribute?: string;
    }
  | {
      id: string;
      type: 'splunkHec';
      exceptions?: ExceptionsDto;
      /** The collector's base address; the destination adds the endpoint paths. */
      url: string;
      /** Zone to the address a worker in that zone sends to instead of `url`. */
      zoneUrls?: Record<string, string>;
      token: SecretRef;
      index?: string;
      sourcetype?: string;
      source?: string;
      host?: string;
      endpoint?: SplunkEndpoint;
      /** How each record is written: `json` or `text` on the event endpoint, any on the raw. Absent is `json`. */
      format?: StreamFormat;
      /** The field `text` or `bytes` writes, and only those. Absent is `_raw` for `text`, `__source.bytes` for
       * `bytes`. */
      field?: string;
      acknowledgement?: SplunkAcknowledgement;
      /** The identity this client connects under, by id. */
      identity?: string;
      inFlight?: number;
      maxRequestBytes?: ByteCount;
      bufferBytes?: ByteCount;
      stallAfterSecs?: number;
    }
  | {
      id: string;
      type: 'http';
      exceptions?: ExceptionsDto;
      /** The whole address each request is POSTed to. */
      url: string;
      /** Zone to the address a worker in that zone sends to instead of `url`. */
      zoneUrls?: Record<string, string>;
      /** How a batch goes into requests. Absent is `lines`. */
      body?: HttpBody;
      /** How each record is written. Absent is `json`. A JSON array holds only `json`; `bytes` goes only
       * `single`. */
      format?: StreamFormat;
      /** The field `text` or `bytes` writes, and only those. Absent is `_raw` for `text`, `__source.bytes` for
       * `bytes`. */
      field?: string;
      /** Absent is `none`. */
      compression?: HttpCompression;
      /** Header name to its value, committed to the config repository. */
      headers?: Record<string, string>;
      /** Header name to where its value lives. */
      credentials?: Record<string, SecretRef>;
      /** The identity this client connects under, by id. Only on an https:// URL. */
      identity?: string;
      inFlight?: number;
      maxRequestBytes?: ByteCount;
      bufferBytes?: ByteCount;
      stallAfterSecs?: number;
    }
  | {
      id: string;
      type: 'prometheus';
      exceptions?: ExceptionsDto;
      /** The store's remote_write endpoint, the whole address each request is POSTed to. */
      url: string;
      /** The remote_write protocol version sent. Absent is `1.0`. */
      version?: '1.0' | '2.0';
      /** Header name to its value, committed to the config repository. */
      headers?: Record<string, string>;
      /** Header name to where its value lives. */
      credentials?: Record<string, SecretRef>;
      /** The identity this client connects under, by id. Only on an https:// URL. */
      identity?: string;
      maxRequestBytes?: ByteCount;
      bufferBytes?: ByteCount;
      stallAfterSecs?: number;
    }
  | (EntraSignIn & {
      id: string;
      type: 'logAnalytics';
      exceptions?: ExceptionsDto;
      /** The rule's logs ingestion endpoint, or the data collection endpoint linked to it. */
      endpoint: string;
      /** The rule's immutable id, `dcr-` and 32 hex digits. */
      ruleId: string;
      /** The rule's stream the rows go to, such as `Custom-Events_CL`. */
      stream: string;
      /** The identity this client connects under, by id. */
      identity?: string;
      inFlight?: number;
      maxRequestBytes?: ByteCount;
      bufferBytes?: ByteCount;
      stallAfterSecs?: number;
    })
  | (ObjectSettings &
      CloudSettings &
      S3Access & {
        id: string;
        type: 's3';
        exceptions?: ExceptionsDto;
        bucket: string;
        kmsKeyId?: string;
      })
  | (ObjectSettings &
      CloudSettings &
      GcsAccess & {
        id: string;
        type: 'gcs';
        exceptions?: ExceptionsDto;
        bucket: string;
      })
  | (ObjectSettings &
      CloudSettings &
      AzureAccess & {
        id: string;
        type: 'azureBlob';
        exceptions?: ExceptionsDto;
        container: string;
      })
  | (ObjectSettings &
      SftpAccess & {
        id: string;
        type: 'sftp';
        exceptions?: ExceptionsDto;
        /** Absolute, or under the user's home directory. Absent is the home directory. It must exist. */
        directory?: string;
      })
  | (S3Access & {
      id: string;
      type: 'iceberg';
      exceptions?: ExceptionsDto;
      /** The REST catalog's base address, before `/v1`. */
      catalog: string;
      /** Absent is the catalog's default warehouse. */
      warehouse?: string;
      /** The table's namespace, its levels joined by `.`. */
      namespace: string;
      table: string;
      /** The partition spec a created table takes: `host`, `day(ts)`, `bucket(16, id)`, `truncate(4, name)`. */
      partitionBy?: string[];
      /** A bearer token for the catalog. Not with `clientId` or `signingName`. */
      token?: SecretRef;
      /** OAuth client credentials: `clientId` and `clientSecret` together. */
      clientId?: string;
      clientSecret?: SecretRef;
      /** Where client credentials are exchanged for a token. Absent is the catalog's own `/v1/oauth/tokens`. */
      tokenUrl?: string;
      scope?: string;
      /** Signs every catalog call with AWS Signature Version 4 for this service, `glue` or `s3tables`. Not with
       * `token` or `clientId`. */
      signingName?: string;
      /** The identity this client connects under, to the catalog and to storage, by id. */
      identity?: string;
      /** How each Parquet file compresses its columns. Absent is `zstd`. */
      compression?: Compression;
      /** Parts of one file uploading at once. */
      inFlight?: number;
      maxObjectBytes?: ByteCount;
      /** Memory for records not yet committed. Absent is `LIMITS.objectBufferBytes.default`. */
      bufferBytes?: ByteCount;
      stallAfterSecs?: number;
      /** Seconds files stay open before they are committed. Within `LIMITS.tableFileIntervalSecs`. */
      fileIntervalSecs?: number;
    })
  | (S3Access & {
      id: string;
      type: 'delta';
      exceptions?: ExceptionsDto;
      /** The table's root, `s3://bucket/path`. Exactly one of `location` and `unityCatalog`. */
      location?: string;
      /** A Unity Catalog external table, whose root and storage credentials the catalog gives. */
      unityCatalog?: UnityCatalog;
      /** The columns a created table is partitioned by, each a top-level column of the record. */
      partitionBy?: string[];
      /** The identity this client connects to storage under, by id. */
      identity?: string;
      /** How each Parquet file compresses its columns. Absent is `zstd`. */
      compression?: Compression;
      /** Parts of one file uploading at once. */
      inFlight?: number;
      maxObjectBytes?: ByteCount;
      /** Memory for records not yet committed. Absent is `LIMITS.objectBufferBytes.default`. */
      bufferBytes?: ByteCount;
      stallAfterSecs?: number;
      /** Seconds files stay open before they are committed. Within `LIMITS.tableFileIntervalSecs`. */
      fileIntervalSecs?: number;
    });

/** A Databricks Unity Catalog table, signed in to as a service principal with OAuth client credentials. */
export interface UnityCatalog {
  /** The workspace's address and nothing after its host, as `https://dbc-1234.cloud.databricks.com`. */
  workspace: string;
  /** The table's three-part name, `catalog.schema.table`. */
  table: string;
  /** The service principal's OAuth client id. */
  clientId: string;
  clientSecret: SecretRef;
}

/** Where Google Cloud is reached and how its requests are signed, as the GCS destination and the Pub/Sub/GCS source
 * both name it. */
export interface GcsAccess {
  /** Another Cloud Storage address, such as an emulator's. Absent is Google's. */
  endpoint?: string;
  /** A service account's JSON key. Absent takes the host's Application Default Credentials. */
  serviceAccountKey?: SecretRef;
}

/** Where an Azure storage account is reached and how its requests are signed, as the Azure Blob destination and the
 * Queue/Azure Blob source both name it. */
export interface AzureAccess {
  account: string;
  /** Another address, such as Azurite's or a sovereign cloud's. Absent is the public cloud's. */
  endpoint?: string;
  /** Where `clientSecret` is exchanged for a token. Absent is the one in the cloud `endpoint` is in. */
  authEndpoint?: string;
  /** At most one of `accessKey`, `sasToken`, `clientSecret` and `managedIdentityClientId`. None takes the host's
   * managed identity. */
  accessKey?: SecretRef;
  sasToken?: SecretRef;
  /** A service principal: all three or none. */
  tenantId?: string;
  clientId?: string;
  clientSecret?: SecretRef;
  /** Which of the host's managed identities signs in: a user-assigned one's client id. */
  managedIdentityClientId?: string;
}

/** How an Event Hubs source or destination signs in to its namespace. */
export interface EventHubsSignIn {
  /** At most one of `connectionString`, `clientSecret` and `managedIdentityClientId`. None takes the host's managed
   * identity. */
  connectionString?: SecretRef;
  /** A service principal: all three or none. */
  tenantId?: string;
  clientId?: string;
  clientSecret?: SecretRef;
  /** Where `clientSecret` is exchanged for a token. Absent is the one in the cloud the namespace is in. */
  authEndpoint?: string;
  /** Which of the host's managed identities signs in: a user-assigned one's client id. */
  managedIdentityClientId?: string;
}

/** How a client signs in to an Azure service with Entra ID, as `EventHubsSignIn` does less its connection string. */
export type EntraSignIn = Omit<EventHubsSignIn, 'connectionString'>;

/** Where AWS is reached and how its requests are signed, as the S3 destination and the SQS/S3 source both name it. */
export interface S3Access {
  /** Absent takes the host's. */
  region?: string;
  /** An S3-compatible service's address. Absent is AWS. */
  endpoint?: string;
  /** Where credentials for assuming `roleArn` are obtained. Absent is the region's own. */
  authEndpoint?: string;
  /** Both or neither. Neither takes the host's credentials. */
  accessKeyId?: SecretRef;
  secretAccessKey?: SecretRef;
  /** A role to assume, for a bucket in another account. */
  roleArn?: string;
  externalId?: string;
}

/** Where an SFTP server is reached, which server it must be, and how this side signs in, as the SFTP source and
 * destination both name it. */
export interface SftpAccess {
  host: string;
  /** Absent is `SFTP_PORT`. */
  port?: number;
  username: string;
  /** Exactly one of `password` and `privateKey`. */
  password?: SecretRef;
  /** An OpenSSH private key, unencrypted. */
  privateKey?: SecretRef;
  /** The server's public key as OpenSSH writes it, `ssh-ed25519 AAAA…`: the one server this connects to. */
  hostKey: string;
}

/** `SftpAccessDto::DEFAULT_PORT`. */
export const SFTP_PORT = 22;

/** The shortest `stallAfterSecs` an SFTP destination takes (`config_file::load::SFTP_MIN_STALL_AFTER`): a write is
 * cut off at it, and a sign-in must finish within it. */
export const SFTP_MIN_STALL_AFTER = 15;

/** What a listing source reads first: every object its first listing finds, or only what arrives after it. `new`
 * needs `afterCommit` `keep`, whose ledger records the first listing as read. */
export type ListStart = 'everything' | 'new';

/** What a listing source does with an object once every record of it is committed: leaves it and records it in
 * its ledger, deletes it, or moves it under `movePrefix`. */
export type AfterCommit = 'keep' | 'delete' | 'move';

/** Where an API's answer holds its records: the whole answer, a record per line, or a record per element of a JSON
 * array. */
export type ApiRecords = 'body' | 'lines' | 'array';

/** What every receiver takes, mirroring `config_file::dto::ReceiverSettingsDto`: where it listens, the TLS it serves
 * and how large a request it reads. Every worker of the group listens on `port`, at the address its own startup
 * settings name. */
export interface ReceiverSettings {
  port: number;
  /** The identity it serves TLS under, by id. Its authorities are what a sender's certificate must chain to. */
  identity?: string;
  /** Absent is `LIMITS.receiverRequestBytes.default`. */
  maxRequestBytes?: number;
}

/** A token a HEC receiver admits, mirroring `config_file::dto::HecTokenDto`. */
export type HecToken = {
  token: SecretRef;
  /** Whether its senders use indexer acknowledgement, as Splunk's `useACK`. Absent is not. */
  acknowledgement?: boolean;
};

/** How an S2S receiver breaks one sourcetype's stream into events, mirroring `config_file::dto::SourcetypeDto`. */
export type Sourcetype = {
  /** A pattern whose first capture group is where an event ends, the group dropped, as Splunk's `LINE_BREAKER`. */
  lineBreaker?: string;
};

/** What every API source takes besides where it asks and how it signs in. */
export interface ApiSettings {
  /** The identity this client connects under, by id. */
  identity?: string;
  /** Seconds from the start of one poll to the start of the next. Absent is `LIMITS.pollIntervalSecs.default`. */
  pollIntervalSecs?: number;
  /** What the first poll asks for. Absent is `everything`. */
  start?: ListStart;
}

/** What every listing source takes besides where it lists and how it signs in. */
export interface ListingSettings extends ReadFormat {
  /** Only keys that start with it. Absent is all of them. */
  prefix?: string;
  /** Absent is `lines`. */
  framing?: Framing;
  lineBreaker?: string;
  /** As a Kafka source's. */
  encoding?: string;
  /** Seconds from the start of one listing to the start of the next. Absent is
   * `LIMITS.pollIntervalSecs.default`. */
  pollIntervalSecs?: number;
  /** Absent is `everything`. */
  start?: ListStart;
  /** Absent is `keep`. */
  afterCommit?: AfterCommit;
  /** Where `move` puts an object, in place of `prefix`. Named with `move` and only then. */
  movePrefix?: string;
}

/** How an object source cuts an object into records: one per line, the whole object as one, or one per line
 * unescaped as a `bytes` object destination wrote it. `lines` alone takes the source's `lineBreaker`, a regex
 * whose first capture group is where a record ends, the group dropped, as Splunk's `LINE_BREAKER`; absent, a
 * record ends at a line end. */
export type Framing = 'lines' | 'object' | 'escapedLines';

/** An object store's kind, as a destination names it and as an object source's access is drawn. */
export type ObjectStore = CloudStore | 'sftp';

/** An object store reached over HTTPS: a cloud's. */
export type CloudStore = 's3' | 'gcs' | 'azureBlob';

/** What every object store destination writes, and how much at once. */
export interface ObjectSettings {
  /** What every object key starts with, for a row with no `__destination.prefix`. `%Y %m %d %H %M` are the
   * write time, UTC. Absent is `OBJECT_PREFIX`. */
  prefix?: string;
  /** How each record is written: one JSON document per line, a JSON array, a Parquet file, the bare text of
   * `field`, or the bytes of `field`. Absent is `ndjson`. */
  format?: ObjectFormat;
  /** The field `text` or `bytes` writes, and only those. Absent is `_raw` for `text`, `__source.bytes` for
   * `bytes`. */
  field?: string;
  /** What `bytes` does with a row that holds a line break. Absent is `escape`. */
  newlines?: 'escape' | 'refuse';
  /** Absent is `gzip`. */
  compression?: Compression;
  inFlight?: number;
  maxObjectBytes?: ByteCount;
  /** Unset is 512 MiB. */
  bufferBytes?: ByteCount;
  stallAfterSecs?: number;
  /** Seconds a file stays open, taking batch after batch, before it is finished. */
  fileIntervalSecs?: number;
}

/** What an object store destination reached over HTTPS takes besides its `ObjectSettings`. */
export interface CloudSettings {
  /** One of `STORAGE_CLASSES` for the kind. Absent is the bucket's default. */
  storageClass?: string;
  /** The identity this client connects under, by id. */
  identity?: string;
}

export type ObjectFormat = 'ndjson' | 'json' | 'parquet' | 'avro' | 'csv' | 'text' | 'bytes';

/** What a Kafka, Splunk HEC or HTTP destination writes: the record as JSON, one field's text, or one field's bytes. */
export type StreamFormat = 'json' | 'text' | 'bytes';

/** `ObjectPrefix::DEFAULT`. */
export const OBJECT_PREFIX = '%Y/%m/%d/%H/';

/** What each cloud takes as an object's storage class, mirroring `io::objects` (`S3_STORAGE_CLASSES`,
 * `GCS_STORAGE_CLASSES`, `AZURE_ACCESS_TIERS`). The load refuses any other. */
export const STORAGE_CLASSES: Record<CloudStore, readonly string[]> = {
  s3: [
    'STANDARD',
    'REDUCED_REDUNDANCY',
    'STANDARD_IA',
    'ONEZONE_IA',
    'INTELLIGENT_TIERING',
    'GLACIER',
    'GLACIER_IR',
    'DEEP_ARCHIVE',
    'SNOW',
  ],
  gcs: ['STANDARD', 'NEARLINE', 'COLDLINE', 'ARCHIVE'],
  azureBlob: ['Hot', 'Cool', 'Cold', 'Archive'],
};

export type SplunkEndpoint = 'event' | 'raw';
export type SplunkAcknowledgement = 'indexer' | 'receipt';

/** One delivery rule: an optional condition over the pipeline's output,
 * and the destination ids a matching row goes to.
 *
 * `when` sees every field the pipeline left live plus the reserved names,
 * and the pipeline's exception tag. It must resolve to Bool. */
export type DeliverRuleDto = { when?: string; to: string[] };

export interface RouteDto {
  id: string;
  source: string;
  condition: string | null;
  pipeline: string;
  /** Where this route's output goes. Rules are additive: a row matching
   * two goes to the union of their destinations, once each. Omitted or
   * empty delivers nowhere. */
  deliver?: DeliverRuleDto[];
  /** Laid over the group's `exceptions`, for every kind but the delivery ones. */
  exceptions?: ExceptionsDto;
}

/** The tag field's default, which the config omits. */
export const TAG_FIELD = 'smeltr.exception';

/** `steps` stays raw `StepDto[]` JSON; `pipeline.ts`'s `stepsFromDto`
 * converts it on demand, so the pipelines list never pays for that. */
export interface PipelineDto {
  id: string;
  name: string;
  steps: unknown[];
}

/** A shared lookup table, referenced by id from any `lookup` step. `headers`
 * on `csv` is a GUI-only hint for the column pickers; the compiler reads
 * the file itself.
 *
 * `path` is relative to the bundle declaring the table (`lookups/<name>`,
 * which is what the upload hands back), so the same commit resolves on the
 * leader and on every worker. An absolute path is for a file a deployment
 * mounts rather than commits. */
export type LookupTableDto =
  | {
      id: string;
      type: 'csv';
      path: string;
      delimiter: string;
      headers: string[];
      /** Edited rows, as the file's whole text: Save writes it as a new file and sets `path` and
       * `headers` from it. Sent only for a table whose rows were edited. */
      contents?: string;
    }
  | { id: string; type: 'inline'; headers: string[]; rows: string[][] }
  | { id: string; type: 'mmdb'; path: string };

/** A named grok pattern, declared once for a bundle and named as `%{id}` from any grok in its group. */
export interface GrokPatternDto {
  id: string;
  pattern: string;
}

export interface FileConfigDto {
  version: number;
  /** What each exception does to its row, under each route's own. Absent leaves the built-in answers. The group's
   * own manifest, so `shared/` never carries one. */
  exceptions?: ExceptionsDto;
  sources: SourceDto[];
  destinations: DestinationDto[];
  pipelines: PipelineDto[];
  routes: RouteDto[];
  lookupTables: LookupTableDto[];
  grokPatterns: GrokPatternDto[];
  identities: IdentityDto[];
  /** A group's display name, from its manifest. Absent shows the id. */
  name?: string;
}

/** Reads are lenient: `config` holds whatever parsed, and `errors` says
 * what does not currently validate. `ok: false` with a populated `config`
 * is a warning, not a failed load. */
export interface GetConfigResult {
  ok: boolean;
  errors: ApiError[];
  config: FileConfigDto | null;
  /** The commit this was read at. */
  commit?: string;
}

export interface ListPipelinesResult {
  ok: boolean;
  errors: ApiError[];
  pipelines: PipelineDto[];
  commit?: string;
}

export interface GetPipelineResult {
  ok: boolean;
  errors: ApiError[];
  pipeline: PipelineDto | null;
  commit?: string;
}

/** Methods that change nothing, so the leader asks nothing of them beyond a
 * session. Everything else echoes the CSRF value. */
const SAFE = new Set(['GET', 'HEAD', 'OPTIONS']);

/** The one place every call goes, which is why the session lives here rather
 * than in each of them: the CSRF header on unsafe methods, and a refusal
 * reported once so the shell can show the sign-in screen instead of
 * thirty-seven screens each showing an error.
 *
 * Every failure is reported to the error bar the same way (`errors.svelte.ts`),
 * by `weight`: a refusal, an answer that is not JSON, or no answer at all. An
 * answer the caller shows itself (`HANDLED`) is not a failure to show. */
async function apiJson<T>(url: string, init?: RequestInit, weight: Weight = 'bar'): Promise<T> {
  const method = (init?.method ?? 'GET').toUpperCase();
  const call = errors.call(method, url, weight);
  let answer: T;
  try {
    answer = await send<T>(url, method, init);
  } catch (e) {
    throw failed(call, e);
  }
  answered(call, answer);
  return answer;
}

/** What a call that threw throws on: reported, unless it is quiet and its caller shows it. */
function failed(call: Call, e: unknown): unknown {
  if (call.weight === 'quiet' || e instanceof Reported) return e;
  errors.report(call, [bareError(errorMessage(e))]);
  return new Reported(errorMessage(e), { cause: e });
}

/** Reports what `answer` refused, or clears what `call` reported last. */
function answered(call: Call, answer: unknown): void {
  const body = answer as Record<string, unknown> | null;
  const handled = HANDLED.some((field) => {
    const v = body?.[field];
    return Array.isArray(v) ? v.length > 0 : v !== undefined && v !== null;
  });
  if (body?.ok === false && Array.isArray(body.errors) && body.errors.length > 0 && !handled) {
    errors.report(call, body.errors as ApiError[], body.errors as ApiError[]);
  } else {
    errors.cleared(call);
  }
}

/** The fields of an answer that its caller shows itself, errors and all: files
 * both sides changed and groups a deploy would not load (each a question the
 * page asks), and a read that came back with files it could not read beside
 * the ones it could (`UnreadableFiles`). */
const HANDLED = ['conflicts', 'doesNotLoad', 'config', 'samples', 'pipelines'];

/** The session ended: the shell shows the sign-in screen, so no page shows it as an error. */
class SignedOut extends Reported {}

async function send<T>(url: string, method: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers);
  if (!SAFE.has(method)) {
    const token = csrf();
    if (token !== null) headers.set('x-smeltr-csrf', token);
  }
  // The leader refuses a body it was not told the type of, so a caller that
  // sends one never has to remember to say so. Never for FormData: the browser
  // sets `multipart/form-data` with the boundary it generated, and naming a
  // type here would strip the boundary and make the body unreadable.
  const body = init?.body;
  if (
    body !== undefined &&
    body !== null &&
    !(body instanceof FormData) &&
    !headers.has('content-type')
  ) {
    headers.set('content-type', 'application/json');
  }

  const res = await fetch(url, { ...init, headers });
  if (res.status === 401) {
    signedOut();
    throw new SignedOut('your session ended -- sign in again');
  }
  // What this page was offered came from the access it last read, which a role change can leave
  // behind.
  if (res.status === 403) void recheck();

  const text = await res.text();
  let answer: unknown;
  try {
    answer = JSON.parse(text);
  } catch {
    throw new Error(`HTTP ${res.status}: ${text.slice(0, 300) || res.statusText}`);
  }
  // A refusal is JSON too, and parses into whatever shape the caller asked for.
  // Returned, `{ok: false, error}` read as a certificates page says this leader
  // holds no certificate, and read as a worker list says the fleet is empty. The
  // status is what tells the two apart, so it is what decides here.
  if (!res.ok) {
    const said = (answer as { error?: unknown } | null)?.error;
    throw new Error(
      typeof said === 'string' && said !== '' ? said : `HTTP ${res.status}: ${res.statusText}`,
    );
  }
  return answer as T;
}

// -- the branch being edited ----------------------------------------------
//
// Every config call names the branch it reads or writes and the commit it
// last saw. The leader refuses a save against a branch that has moved past
// that commit, so `configJson` is the one place both travel from.

/** What the fleet runs. It always exists, which is why it is where a
 * branch that stops existing sends you. */
export const MAIN_BRANCH = 'main';
const BRANCH_KEY = 'smeltr-branch';

/** Remembered across visits: a branch is work in flight, and reloading a page
 * onto someone else's default would read a different config than the one that
 * was on screen. A browser that remembers none opens on `main`, what the fleet
 * runs, so its first edit asks where to go (W1). */
function loadBranch(): string {
  if (typeof window === 'undefined') return MAIN_BRANCH;
  return window.localStorage.getItem(BRANCH_KEY) || MAIN_BRANCH;
}

let branch = loadBranch();
let baseCommit: string | null = null;

export function currentBranch(): string {
  return branch;
}

/** Leave `name` if it is the branch in hand, because it no longer exists:
 * deploying a change deletes its branch, and so does deleting one by hand.
 * Answers whether it had to move, since the caller then owes a reload --
 * a page holds what it read, and what it read was branch-scoped. */
export function leaveBranch(name: string): boolean {
  if (branch !== name) return false;
  setBranch(MAIN_BRANCH);
  return true;
}

/** Raised when a first write starts branch `detail.to` from `main` (W1). The new branch forks from what was read on
 * `main`, so what was typed there is still about it (`edits.ts`). */
export const BRANCH_STARTED_EVENT = 'smeltr:branch-started';

/** Moves to `chosen`, a branch a first write is starting from `main`. */
function startBranch(chosen: string): void {
  setBranch(chosen);
  if (typeof window !== 'undefined')
    window.dispatchEvent(new CustomEvent(BRANCH_STARTED_EVENT, { detail: { to: chosen } }));
}

/** Switch branches. The commit is dropped with it: it names a place on the
 * branch being left. */
export function setBranch(name: string): void {
  branch = name;
  baseCommit = null;
  if (typeof window !== 'undefined') window.localStorage.setItem(BRANCH_KEY, name);
}

/** Where a call against `current` must land. `main` is what deploys move
 * and is never written to directly (W1); a mutation starting there asks which
 * branch to go on, offering a fresh name, and the first edit starts it. A
 * read, and a mutation already on another branch, stay put. */
export function needsBranch(current: string, method: string | undefined): boolean {
  const mutating = !!method && method !== 'GET';
  return mutating && current === MAIN_BRANCH;
}

/** How a write asks which branch to go to when the editor is on `main` (W1).
 * The shell registers this once; a write that finds nobody registered is
 * refused rather than moved somewhere nobody chose. Answering `null` is the
 * writer changing their mind, and the write does not happen. */
let askForBranch: ((suggested: string) => Promise<string | null>) | null = null;

export function onBranchNeeded(ask: (suggested: string) => Promise<string | null>) {
  askForBranch = ask;
}

/** What a write refused for want of a branch answers. Not a failure: nothing
 * broke, and the writer is the one who said no. */
function noBranchChosen<T>(): T {
  return {
    ok: false,
    errors: [bareError('not saved -- no branch was chosen to save it on')],
  } as T;
}

/** The commit the editor is working from, `null` before its first read. */
export function currentBaseCommit(): string | null {
  return baseCommit;
}

// -- the scope being edited -------------------------------------------------
//
// Which bundle a config call reads or writes within the branch: the one every
// group draws on, or one group's own (design doc, Control plane). Kept the
// same way the
// branch is -- module state, persisted, a setter that says a reload is owed --
// so a page that holds what it read is never wrong about which bundle that was.

/** Matches `config_file::SHARED_DIR`. Always exists, which is why it is
 * where a scope that stops existing sends you. Every config call names its
 * scope: the leader refuses one that names none. */
export const SHARED_SCOPE = 'shared';
const SCOPE_KEY = 'smeltr-scope';

function loadScope(): string {
  if (typeof window === 'undefined') return SHARED_SCOPE;
  return window.localStorage.getItem(SCOPE_KEY) || SHARED_SCOPE;
}

let scope = loadScope();

export function currentScope(): string {
  return scope;
}

/** Switch to scope `name`, and open `hash` there when given. The shell mounts
 * the page in view afresh on a switch, so nothing read under the old scope
 * outlives it. The commit is dropped with it, for the reason `setBranch` gives
 * -- a different scope is a different bundle. */
export function setScope(name: string, hash?: string): void {
  scope = name;
  baseCommit = null;
  if (typeof window !== 'undefined') {
    window.localStorage.setItem(SCOPE_KEY, name);
    // The shell draws the scope -- the chip, the rail's hue and the page
    // itself -- from its own copy. A scope switched from anywhere else has to
    // reach it, or the page says one group while the writes go to another.
    window.dispatchEvent(new CustomEvent(SCOPE_EVENT));
    if (hash !== undefined) location.hash = hash;
  }
}

/** The scope moved. */
export const SCOPE_EVENT = 'smeltr:scope';

/** Leave `name` if it is the scope in hand, because the group it names no
 * longer exists -- a group's manifest can live only on a branch, and that
 * branch can go. Same treatment as `leaveBranch`, and for the same reason: a
 * page holds what it read, and what it read was scope-branded.
 *
 * `setScope` drops the base commit with the scope, since it ordinarily named a bundle this scope
 * no longer reads. `keepBase` overrides that for the one case where it does not: the write that
 * made `name` worth leaving -- deleting the group it named -- already moved the base past that
 * write, on the branch as a whole rather than on any one scope's bundle, and a leave right after
 * must not throw that away and send the next write with no base at all. */
export function leaveScope(name: string, keepBase?: string | null): boolean {
  if (scope !== name) return false;
  setScope(SHARED_SCOPE);
  if (keepBase) baseCommit = keepBase;
  return true;
}

function branched(url: string, name: string = branch): string {
  const sep = url.includes('?') ? '&' : '?';
  return `${url}${sep}branch=${encodeURIComponent(name)}&scope=${encodeURIComponent(scope)}`;
}

/** `branched()` without `scope=` -- for a call whose scope the server derives or does not use at all, so
 * sending the chip's would be inert-but-present, which reads as meaningful when it is not. */
function branchOnly(url: string, name: string = branch): string {
  const sep = url.includes('?') ? '&' : '?';
  return `${url}${sep}branch=${encodeURIComponent(name)}`;
}

/** What a review's test calls need: `onBranch`, the branch under review, and `ownScopes=true` telling
 * the server to derive that branch's own scopes (`test_api::TestScopeQuery`) rather than trust the chip
 * -- the chip is the reviewer's own editing scope, not the change's.
 *
 * `true` spelled out: the field is a `bool`, and serde reads only `true` or `false` there. `1` is a
 * deserialize error, which arrives as an HTTP 400 in place of the test results. */
function reviewScoped(url: string, onBranch: string): string {
  const sep = url.includes('?') ? '&' : '?';
  return `${url}${sep}branch=${encodeURIComponent(onBranch)}&ownScopes=true`;
}

/** What a config call needs beyond its URL and its body. Each field is
 * documented on `configJson`, the only thing that reads one. */
interface ConfigCall {
  scoped?: boolean;
  adopt?: boolean;
  base?: string | null;
  /** How a failure is shown (`apiJson`). */
  weight?: Weight;
}

/** A config call: branch in the query, base commit on a write, and the
 * commit in the answer kept for the next one. A write reaching `main` asks
 * for a branch first (`needsBranch`), so no caller here can forget and every
 * one of them lands the same way a first edit always has.
 * A refused save carries no commit, so the base stays what it was until a
 * read moves it.
 *
 * `scoped` is false for a write the scope chip has nothing to say about --
 * a worker group is not held in one -- so the chip's value is not sent as
 * inert-but-present. Everything else about a config write still applies.
 *
 * `adopt` decides whether this call's commit becomes the shared base, and
 * defaults to true: ordinarily a GET here is the page's own primary read,
 * the one its own save is meant to land against, same as a write's commit
 * becomes the base for the write that follows it. Pass `false` for a GET
 * that is not that -- an auxiliary roster fetched beside the page's real
 * read, say -- so it cannot race that read and silently move the base to a
 * commit the editor never actually saw its content at, hiding a real
 * conflict on the save that follows.
 *
 * `base` is the commit this write was made against, for a caller that knows
 * its own. The shared base is the commit of the last adopting read, which is
 * the write's own read for as long as the editor that made it stays on
 * screen. An edit that outlives its read does not have that: the read that
 * would notice the branch moved is the same read that adopts the new head,
 * so by the time such an edit is saved the shared base is the head and the
 * leader has nothing left to refuse. Passing the commit the edit was read at
 * is what keeps the check real. Absent means the shared base, which is right
 * wherever the two are the same thing; `null` means this write is against no
 * commit, which sends no header and skips the check, where absent would fall
 * back to a commit the write was never against. */
async function configJson<T extends { commit?: string | null }>(
  url: string,
  init?: RequestInit,
  { scoped = true, adopt = true, base, weight }: ConfigCall = {},
): Promise<T> {
  const method = init?.method;
  const write = !!method && method !== 'GET';
  if (needsBranch(branch, method)) {
    const chosen = askForBranch ? await askForBranch(funnyBranchName()) : null;
    if (!chosen) {
      const refused = noBranchChosen<T>();
      answered(errors.call(method ?? 'GET', url, weight ?? 'bar'), refused);
      return refused;
    }
    // `askForBranch` answers only with a branch this names for the first time -- an existing one
    // takes the caller straight to a reload instead, so its own unseen state is never in play
    // here. The branch a write like that starts still forks from whatever `main` actually is by
    // the time it lands, which can have moved past what the editor read. `setBranch` would drop
    // that reading along with the branch it belonged to; carrying it through as this first
    // write's base lets the server refuse if `main` did move, the same compare-and-swap check
    // every other write already gets.
    const reviewed = baseCommit;
    startBranch(chosen);
    baseCommit = reviewed;
  }
  const headers = new Headers(init?.headers);
  // The two empty values mean different things, so this cannot take a default
  // the way `scoped` and `adopt` do: `undefined` is "I did not say", which
  // takes the shared base, and `null` is "against no commit", which sends
  // nothing.
  const against = base === undefined ? baseCommit : base;
  if (write && against) {
    headers.set('x-smeltr-base-commit', against);
  }
  const result = await apiJson<T>(
    scoped ? branched(url) : branchOnly(url),
    { ...init, headers },
    weight,
  );
  if (result.commit && adopt) {
    baseCommit = result.commit;
  }
  // A refused save carries no commit, so this fires only where the branch
  // really moved. The shell's review bar counts what is waiting on it, and
  // nothing else would tell it that number just changed.
  if (result.commit && write && typeof window !== 'undefined') {
    window.dispatchEvent(new CustomEvent(SAVED_EVENT));
  }
  return result;
}

function putBody(body: unknown): RequestInit {
  return {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  };
}

export function getConfig(): Promise<GetConfigResult> {
  return configJson('/api/config');
}

/** A save landed on the branch it was aimed at. */
export const SAVED_EVENT = 'smeltr:saved';

/** One branch's configuration in one scope, read without moving the branch
 * or the scope the rest of the app is pointed at. Naming a change means
 * comparing a branch against `main` in every scope it touched, and none of
 * those reads is where the editor happens to be. */
export function configAt(onBranch: string, inScope: string, at?: string): Promise<GetConfigResult> {
  const q = new URLSearchParams({ branch: onBranch, scope: inScope });
  if (at) q.set('at', at);
  return apiJson(`/api/config?${q}`);
}

/** `shared/`'s bundle on the current branch, which a group's pages show beside the group's own, since a group
 * runs the two merged. Null in `shared/` itself, which draws on nothing, and when it cannot be read. */
export async function sharedConfig(): Promise<FileConfigDto | null> {
  if (currentScope() === SHARED_SCOPE) return null;
  return (await configAt(currentBranch(), SHARED_SCOPE)).config;
}

/** A section of a bundle, by its key in `FileConfigDto`. */
export type ConfigSection =
  | 'sources'
  | 'destinations'
  | 'routes'
  | 'lookupTables'
  | 'grokPatterns'
  | 'identities'
  | 'pipelines';

/** What Undo changes puts back: one object, or one file no section owns by
 * its path in the scope's bundle. */
export type UndoTarget = { section: ConfigSection; id: string } | { path: string };

/** Undo changes: put `what` back the way it was where `onBranch` left
 * `main`, as a new save on that branch. Names the branch and scope itself,
 * since a review can act on a branch the editor is not on. `base` is the
 * commit the caller read the branch at; the leader refuses the save if the
 * branch has moved past it. */
export async function undoChanges(
  onBranch: string,
  inScope: string,
  what: UndoTarget,
  base: string,
): Promise<MutationResult> {
  const q = new URLSearchParams({ branch: onBranch, scope: inScope });
  // Refused beside the undo that asked.
  const result = await apiJson<MutationResult>(
    `/api/undo?${q}`,
    {
      method: 'POST',
      headers: { 'x-smeltr-base-commit': base },
      body: JSON.stringify(what),
    },
    'quiet',
  );
  if (result.commit && typeof window !== 'undefined') {
    window.dispatchEvent(new CustomEvent(SAVED_EVENT));
  }
  return result;
}

export function putSources(sources: SourceDto[], base?: string | null): Promise<MutationResult> {
  return configJson('/api/sources', putBody(sources), { base });
}

export function putDestinations(
  destinations: DestinationDto[],
  base?: string | null,
): Promise<MutationResult> {
  return configJson('/api/destinations', putBody(destinations), { base });
}

/** What the leader reads out of a PEM: every certificate in it, or why it is
 * not one. Asked of the leader because `leader::tls` already reads PEM, and a
 * second reading in the browser would drift from the one a save is judged by. */
export function describeCertificates(
  pem: string,
): Promise<{ certificates: DescribedCertificate[] } | { error: string }> {
  return post('/api/identities/describe', { pem }, 'quiet');
}

/** Refused when an identity's certificate and the key it names are not a pair --
 * the one check the leader makes that the bundle alone cannot. */
export function putIdentities(
  identities: IdentityDto[],
  base?: string | null,
): Promise<MutationResult> {
  return configJson('/api/identities', putBody(identities), { base });
}

export function putRoutes(routes: RouteDto[], base?: string | null): Promise<MutationResult> {
  return configJson('/api/routes', putBody(routes), { base });
}

/** The group's manifest. A `null` leaves the built-in answers. Refused in `shared/`: the manifest is a group's own. */
export function putManifest(
  manifest: {
    exceptions: ExceptionsDto | null;
  },
  base?: string | null,
): Promise<MutationResult> {
  return configJson('/api/manifest', putBody(manifest), { base });
}

export function putGrokPatterns(
  grokPatterns: GrokPatternDto[],
  base?: string | null,
): Promise<MutationResult> {
  return configJson('/api/grok-patterns', putBody(grokPatterns), { base });
}

export function putLookupTables(
  lookupTables: LookupTableDto[],
  base?: string | null,
): Promise<MutationResult> {
  return configJson('/api/lookup-tables', putBody(lookupTables), { base });
}

/** A lookup file upload: one commit on the branch, like every other save. */
export interface UploadLookupResult {
  ok: boolean;
  errors: ApiError[];
  /** The path a lookup table should name, relative to its bundle. */
  path?: string;
  /** The header row of a CSV; absent for an MMDB. */
  headers?: string[];
  commit?: string;
}

/** `delimiter` is a CSV table's own, which the leader splits the header row on. `base` is the
 * commit the caller read, as `putLookupTables` takes it. */
export function uploadLookupFile(
  file: File,
  delimiter?: string,
  base?: string | null,
): Promise<UploadLookupResult> {
  const body = new FormData();
  body.append('file', file);
  const q = delimiter ? `?${new URLSearchParams({ delimiter })}` : '';
  return configJson(`/api/lookup/upload${q}`, { method: 'POST', body }, { base, weight: 'quiet' });
}

/** A CSV lookup file's whole text. */
type LookupFileResult = { ok: boolean; errors: ApiError[]; text?: string; commit?: string };

export function getLookupFile(path: string): Promise<LookupFileResult> {
  const q = new URLSearchParams({ path });
  return configJson(`/api/lookup/file?${q}`, undefined, { adopt: false, weight: 'quiet' });
}

/** A CSV lookup file's whole text at commit `at` of `onBranch`, as `configAt` reads a bundle. */
export function lookupFileAt(
  onBranch: string,
  inScope: string,
  path: string,
  at: string,
): Promise<LookupFileResult> {
  const q = new URLSearchParams({ branch: onBranch, scope: inScope, path, at });
  return apiJson(`/api/lookup/file?${q}`, undefined, 'quiet');
}

/** An MMDB file's metadata, and with `ip` the record for that address: `null` on a miss. */
export interface LookupMmdbResult {
  ok: boolean;
  errors: ApiError[];
  databaseType: string;
  buildEpoch: number;
  ipVersion: number;
  languages: string[];
  networks: number;
  record?: unknown;
  commit?: string;
}

export function getLookupMmdb(path: string, ip?: string): Promise<LookupMmdbResult> {
  const q = new URLSearchParams(ip ? { path, ip } : { path });
  return configJson(`/api/lookup/mmdb?${q}`, undefined, { adopt: false, weight: 'quiet' });
}

// -- files that do not load --------------------------------------------------
//
// A file that does not parse never reaches the section editor that owns it, so it is reached
// here instead: by its full path in the repository, as raw text. Saving and deleting one are
// writes like any other, on the branch and against its base.

/** One file of a bundle that does not load. */
export interface UnreadableFileDto {
  /** Its full path in the repository: `shared/system.json`, `groups/west/pipelines/x.json`. */
  path: string;
  why: string;
  /** False for a group's `system.json`, which the group cannot exist without. */
  deletable: boolean;
}

export interface UnreadableFilesResult {
  ok: boolean;
  errors: ApiError[];
  files: UnreadableFileDto[];
  commit?: string;
}

/** The files of `inScope`'s bundle that do not load. Not the page's own read, so it does not
 * move the base. */
export function unreadableFiles(inScope: string = scope): Promise<UnreadableFilesResult> {
  const q = new URLSearchParams({ scope: inScope });
  return configJson(`/api/config/unreadable?${q}`, undefined, { scoped: false, adopt: false });
}

/** `unreadableFiles` at commit `at` of `onBranch`, as `configAt` reads a bundle. */
export function unreadableAt(
  onBranch: string,
  inScope: string,
  at: string,
): Promise<UnreadableFilesResult> {
  const q = new URLSearchParams({ branch: onBranch, scope: inScope, at });
  return apiJson(`/api/config/unreadable?${q}`);
}

export interface ConfigFileResult {
  ok: boolean;
  errors: ApiError[];
  path: string;
  text: string;
  commit?: string;
}

/** One file's raw text, by its full path. The path names its scope, so the chip's is not sent. */
export function getConfigFile(path: string): Promise<ConfigFileResult> {
  const q = new URLSearchParams({ path });
  return configJson(`/api/config/file?${q}`, undefined, { scoped: false });
}

export function putConfigFile(
  path: string,
  text: string,
  base?: string | null,
): Promise<MutationResult> {
  const q = new URLSearchParams({ path });
  return configJson(`/api/config/file?${q}`, putBody({ text }), { scoped: false, base });
}

export function deleteConfigFile(path: string, base?: string | null): Promise<MutationResult> {
  const q = new URLSearchParams({ path });
  return configJson(`/api/config/file?${q}`, { method: 'DELETE' }, { scoped: false, base });
}

export function listPipelines(): Promise<ListPipelinesResult> {
  return configJson('/api/pipelines');
}

export function getPipeline(id: string): Promise<GetPipelineResult> {
  return configJson(`/api/pipelines/${encodeURIComponent(id)}`);
}

export function putPipeline(
  id: string,
  pipeline: PipelineDto,
  base?: string | null,
): Promise<MutationResult> {
  return configJson(`/api/pipelines/${encodeURIComponent(id)}`, putBody(pipeline), { base });
}

export function deletePipeline(id: string): Promise<MutationResult> {
  return configJson(`/api/pipelines/${encodeURIComponent(id)}`, { method: 'DELETE' });
}

// -- samples ----------------------------------------------------------------
//
// Records under a name, saved beside the config they are authored against
// (`config_file::sample`). A paste and an uploaded file both land here, so a
// preview always runs over a saved sample rather than over whatever was in a
// text box.

/** One record, as the root object a pipeline reads, `$`: its fields, and
 * under `__source` every field its source set. A byte source's record is
 * `{ _raw: text }`. A pasted record was read from nowhere and has no
 * `__source`. One captured at a destination also has the `__destination`
 * fields it was sent with. A value that failed its type is kept as the
 * source spelled it, its field listed under `smeltr.mistyped`, and fails
 * again when the sample is read. */
export type SampleRecordDto = Record<string, unknown>;

/** The fields a sample record's source set, if it was read from one. */
export function sampleSource(record: SampleRecordDto): Record<string, unknown> | undefined {
  const source = record.__source;
  return source !== null && typeof source === 'object'
    ? (source as Record<string, unknown>)
    : undefined;
}

/** A record as one line, without the `__source` fields its source set: a byte
 * record's text as it is, any other record as compact JSON. */
export function recordLine(record: Record<string, unknown>): string {
  if ('__source' in record) {
    const { __source: _, ...fields } = record;
    record = fields;
  }
  const keys = Object.keys(record);
  if (keys.length === 1 && keys[0] === '_raw' && typeof record._raw === 'string')
    return record._raw;
  return JSON.stringify(record);
}

export interface SampleDto {
  /** Stable across renames; the filename is its slug. */
  id: string;
  name: string;
  /** The source these records stand for. Routing selects on a source, so a
   * sample naming none can be run through a pipeline but not through a route. */
  source?: string;
  /** The columns a captured sample was taken under, each named and typed once.
   * A pasted sample has none. */
  columns?: { name: string; type: string }[];
  records: SampleRecordDto[];
}

/** A library listing: everything but the records. */
export interface SampleSummary {
  id: string;
  name: string;
  source?: string;
  records: number;
  /** Unix seconds: when the leader last wrote the sample. */
  captured?: number;
}

export interface ListSamplesResult {
  ok: boolean;
  errors: ApiError[];
  samples: SampleSummary[];
  commit?: string;
}

export interface GetSampleResult {
  ok: boolean;
  errors: ApiError[];
  sample?: SampleDto;
  commit?: string;
}

export function listSamples(): Promise<ListSamplesResult> {
  return configJson('/api/samples');
}

export function getSample(id: string): Promise<GetSampleResult> {
  return configJson(`/api/samples/${encodeURIComponent(id)}`);
}

/** One sample of `inScope` at commit `at` of `onBranch`, read as `configAt` reads a bundle. */
export function sampleAt(
  onBranch: string,
  inScope: string,
  id: string,
  at: string,
): Promise<GetSampleResult> {
  const q = new URLSearchParams({ branch: onBranch, scope: inScope, at });
  return apiJson(`/api/samples/${encodeURIComponent(id)}?${q}`);
}

/** Create or replace: the sample carries its own id. */
export function putSample(sample: SampleDto): Promise<MutationResult> {
  return configJson('/api/samples', putBody(sample));
}

export function deleteSample(id: string): Promise<MutationResult> {
  return configJson(`/api/samples/${encodeURIComponent(id)}`, { method: 'DELETE' });
}

// -- pipeline preview ---------------------------------------------------
//
// Runs one pipeline over a saved sample in memory (`leader::preview::run_preview`).
// Nothing is saved. `atMicros` is what `now()` and a reference-less
// `parseTime` read; a fresh one is an ordinary preview, a locked one is
// how a test re-runs against the instant it was captured at.

export interface PreviewRequest {
  /** A whole `FileConfigDto` bundle, serialized. */
  config: string;
  /** Defaults to the config's first pipeline. */
  pipeline?: string;
  /** A saved sample, by id. Omitted runs no records. */
  sample?: string;
  /** Show `__`-prefixed control variables in the output. */
  showInternal?: boolean;
  /** Unix microseconds. Omitted reads the wall clock. */
  atMicros?: number;
  /** A Bool over the pipeline's output: only the rows it is true for come back.
   * One that does not compile fails at the `filter` stage. */
  filter?: string;
  /** Asks what may follow a `.` instead of running the sample. */
  afterDot?: { text: string; dot: number; at: DotAt };
  /** A capture filter at a destination to check against each route delivering there. Asked, the preview answers it
   * instead of running a sample. */
  captureFilter?: { text: string; destination: string };
}

/** Where an expression sits, which decides what its names read: at the start
 * of a step of the request's pipeline, a source's record as it arrives (a
 * route's condition, a capture filter there), a route's pipeline's output (its
 * `when`s), or what every route delivering to a destination gives it (a capture
 * filter there). Each by its id. */
export type DotAt =
  { step: string } | { source: string } | { when: string } | { destination: string };

/** What may follow a `.`: the type of what it follows, as the engine spells
 * it (absent where it does not resolve), an object's fields, and each
 * function a call there may name, with what its first parameter takes. */
export interface DotAnswer {
  ty?: string;
  fields: [string, string][];
  methods: { name: string; takes: string; fits: boolean }[];
}

/** Asks what may follow the `.` at `dot` in `text`, an expression at `at`: `afterDot` over the bundle being edited. */
export type AskAt = (at: DotAt) => (text: string, dot: number) => Promise<DotAnswer | null>;

export interface PreviewException {
  /** The answer the exception's kind takes. */
  answer: 'ignore' | 'tag' | 'drop' | 'block';
  message: string;
}

export interface PreviewRow {
  row: number;
  /** `null` when the row did not survive the pipeline. */
  json: unknown | null;
  exceptions: PreviewException[];
}

/** One field a parser step read from the sample. `suggested` is a declared
 * field's type, or the declaration an undeclared field's values suggest: one
 * of the named types, `array(<named type>)`, or `struct` (not
 * representable as a declared field in the pipeline editor). */
export interface ParserFieldProposal {
  name: string;
  declared: boolean;
  suggested: string;
  rows: number;
  present: number;
  kinds: [string, number][];
  failed?: number;
  /** Rows whose value read as a time; every present row is a field to declare a `datetime`. */
  times?: number;
}

/** One parser step's proposal, in the order that step ran. */
export interface ParserProposal {
  fields: ParserFieldProposal[];
}

export interface PreviewResult {
  ok: boolean;
  stage: string | null;
  errors: ApiError[];
  rows: PreviewRow[];
  /** How many rows there were before the request's filter. Absent with no filter. */
  unfiltered?: number;
  /** One entry per parser step, in run order -- what the field-declaration
   * panel reads to propose fields from a sample. */
  parsers: ParserProposal[];
  /** The record the pipeline writes. Absent when it did not compile. */
  dictionary?: DictionaryOut;
  /** The answer to `PreviewRequest.afterDot`. Absent where the dot follows no value. */
  after_dot?: DotAnswer;
  /** The answer to `PreviewRequest.captureFilter`. */
  capture_filter?: CaptureFilterCheck;
  /** What compiles but is likely a mistake, each placed at its step as an error is. */
  warnings?: ApiError[];
}

/** What a preview that did not run says went wrong. */
export function previewFailure(res: Pick<PreviewResult, 'stage' | 'errors'>): string {
  return res.errors.map((e) => e.message).join('; ') || `${res.stage} failed`;
}

/** One record type: each field with its type spelled, and as an
 * Elasticsearch mapping. */
export interface Dictionary {
  fields: [string, string][];
  elasticsearch: unknown;
}

/** The record by its compiled types alone, and with the sample's output
 * filling what only a row knows; `sampled` is null with no sample. */
export interface DictionaryOut {
  compiled: Dictionary;
  sampled: Dictionary | null;
}

export function previewPipeline(req: PreviewRequest): Promise<PreviewResult> {
  return post(
    branched('/api/preview'),
    {
      config: req.config,
      pipeline: req.pipeline ?? null,
      sample: req.sample ?? null,
      show_internal: req.showInternal ?? false,
      at_micros: req.atMicros ?? null,
      filter: req.filter ?? null,
      after_dot: req.afterDot ?? null,
      capture_filter: req.captureFilter ?? null,
    },
    'quiet',
  );
}

/** What may follow the `.` at `dot` in `text`, an expression at `at` in `config`. Null where the dot follows no
 * value, or the engine could not say. */
export async function afterDot(
  req: Pick<PreviewRequest, 'config' | 'pipeline' | 'sample'> & {
    text: string;
    dot: number;
    at: DotAt;
  },
): Promise<DotAnswer | null> {
  const { text, dot, at, ...rest } = req;
  const res = await previewPipeline({ ...rest, afterDot: { text, dot, at } });
  return (res.ok && res.after_dot) || null;
}

/** How many routes delivering to a destination a capture filter there runs on, and each it passes over with why: it
 * does not compile against that route's output. */
export interface CaptureFilterCheck {
  reads: number;
  skips: { route: string; error: string }[];
}

/** What a capture filter at `destination` makes of each route delivering there, over `config`. */
export async function checkCaptureFilter(
  config: string,
  text: string,
  destination: string,
): Promise<CaptureFilterCheck | null> {
  const res = await previewPipeline({ config, captureFilter: { text, destination } });
  return (res.ok && res.capture_filter) || null;
}

/** What a grok pattern captures, which is the set of fields the step declares.
 *
 * Not part of `previewPipeline`: that needs a chosen sample and answers nothing
 * once compilation fails, and both are exactly when the editor needs these
 * names -- a pattern that just gained a capture nothing declares does not
 * compile, and this is the list that fixes it. `ok: false` carries the reason a
 * half-typed pattern will not compile, and the editor keeps the fields it has. */
export interface GrokCaptures {
  ok: boolean;
  errors: ApiError[];
  captures: string[];
}

/** `grokPatterns` are the bundle's, which the step's patterns may name. */
export function grokCaptures(
  patterns: string[],
  customPatterns: { name: string; pattern: string }[],
  grokPatterns: GrokPatternDto[] = [],
): Promise<GrokCaptures> {
  return post('/api/grok-captures', { patterns, customPatterns, grokPatterns }, 'quiet');
}

/** Whether a save would take a line breaker, asked as one is typed. Not `ok` when it would refuse it, with why, in
 * the save's words. */
export interface LineBreakerVerdict {
  ok: boolean;
  errors: ApiError[];
}

export function lineBreakerVerdict(pattern: string): Promise<LineBreakerVerdict> {
  return post('/api/line-breaker', { pattern }, 'quiet');
}

/** Pipeline code laid out in the style, or `code: null` where it does not parse yet. */
export interface Formatted {
  ok: boolean;
  errors: ApiError[];
  code: string | null;
}

export function formatCode(code: string, kind: 'expr' | 'block'): Promise<Formatted> {
  return post('/api/format', { code, kind }, 'quiet');
}

// -- onboarding -----------------------------------------------------------
//
// What a capture of a source reads as (`leader::onboard`): the steps that parse its records and the condition a
// route takes them under. Nothing is saved.

/** A capture's proposal. `steps` are `StepDto`s, none where the records arrive as fields (`structured`) or no
 * format read them; `read` of `of` records read as `format`. `condition` is there when every record came under one
 * sourcetype or index. */
export interface OnboardProposal {
  steps?: unknown[];
  format?:
    'json' | 'cef' | 'keyValue' | 'csv' | 'syslog' | 'syslog5424' | 'commonLog' | 'combinedLog';
  read: number;
  of: number;
  structured: boolean;
  condition?: string;
}

/** The most records the leader reads a format from (`onboard::PROPOSED_FROM`): enough to tell one, and few enough
 * to stay inside what one request to it may carry however long each is. */
const PROPOSED_FROM = 200;

// Only the records the format is read from go whole; the route condition must hold for every captured record, so
// each one's `__source` goes too.
export function proposeOnboard(records: SampleRecordDto[]): Promise<OnboardProposal> {
  const filed = records.map((r) => r.__source ?? null);
  return post('/api/onboard/propose', { records: records.slice(0, PROPOSED_FROM), filed }, 'quiet');
}

// -- tests --------------------------------------------------------------
//
// A test locks a pipeline's expected output over a saved sample, at the
// instant it ran (`config_file::test`). Locking is a save like any other,
// through `configJson`. Running writes nothing: a result is not a
// function of the commit alone, since `now()` and a reference-less
// `parseTime` read the clock, so it is computed on every ask.

export type TestSummary = {
  id: string;
  name: string;
  /** Absent names an end-to-end test: the sample routed by its source, not one pipeline. */
  pipeline?: string;
  /** Which scope this test is on. `id` is unique only within a scope, so a review running more than one
   * scope's tests at once needs this to tell two groups' same-named tests apart. */
  scope: string;
  sample: string;
  rowsIn: number;
  /** Documents kept; fewer than `rowsIn` means rows were dropped, or (for an end-to-end test) matched no
   * route. */
  rowsOut: number;
  /** Every pipeline this test exercises: what `pipeline` names, for a pipeline test; each matched route's
   * pipeline, read off the locked outcomes, for an end-to-end test. A record can match more than one
   * route, so this can name more than one pipeline even where `pipeline` names none. */
  pipelines: string[];
  /** Every route the locked outcomes carry a document through. Absent for a pipeline test, which has
   * none. */
  routes?: string[];
};

export type TestDto = {
  /** Stable across renames; the filename is its slug. */
  id: string;
  name: string;
  /** The pipeline whose output is locked. Absent routes the sample by its source instead -- the route
   * preview, which needs `SampleDto.source`. */
  pipeline?: string;
  sample: string;
  /** The instant the test was first locked, in unix microseconds: what `now()`
   * reads on every run, and on every re-lock. */
  lockedAt: number;
  /** Records the sample held when this was locked. */
  rowsIn: number;
  /** A pipeline test: each document the pipeline produced, in order; `null` is a dropped row. An
   * end-to-end test: each document a route kept, packed with where it went (`endToEndDocument`) -- never
   * `null`, since a route that drops a record contributes no entry rather than one recording nothing. */
  expected: (unknown | null)[];
};

/** One document an end-to-end test kept, packed with where it went -- what one entry of an end-to-end
 * `TestDto.expected` holds, mirroring `TestDto::end_to_end_document`. `destinations` are names, resolved
 * through `RoutePreviewResult.destination_names` at lock and at run, never the runtime ids those name. */
export type EndToEndExpected = {
  document: unknown;
  pipeline: string;
  route: string;
  destinations: string[];
};

export function endToEndDocument(
  document: unknown,
  pipeline: string,
  route: string,
  destinations: string[],
): EndToEndExpected {
  return { document, pipeline, route, destinations };
}

/** Where a run first stopped matching what a test locks. Either side is
 * absent past its own end. */
export type Difference = {
  /** Position in the output, counting from zero. */
  at: number;
  /** What the test locked here. Absent when the run produced more
   * documents than the test expects. */
  expected?: unknown;
  /** What the run produced here. Absent when the run produced fewer. */
  actual?: unknown;
  expectedDropped: boolean;
  actualDropped: boolean;
};

/** What a run made of a test: the first difference and the two counts,
 * not a per-row tally -- an unroll that changes row counts would else
 * report one record's change as every later row changing. */
export type Verdict = {
  passed: boolean;
  /** Documents the test locks, dropped rows included. */
  expectedRows: number;
  /** Documents the run produced, dropped rows included. */
  actualRows: number;
  firstDifference?: Difference;
  /** Present when the sample no longer holds the records it held at lock: a
   * changed input, which fails the test whatever the output says. */
  rowsIn?: { locked: number; now: number };
};

export type TestResult = {
  id: string;
  name: string;
  /** Absent for an end-to-end test, which names no pipeline. */
  pipeline?: string;
  /** Which scope this ran in, same reason `TestSummary.scope` carries one. */
  scope: string;
  /** Absent when the run could not happen at all. */
  verdict?: Verdict;
  /** Why there is no verdict: a missing sample, a pipeline that will not
   * compile. Not a failing test, and never shown as one. */
  error?: string;
};

export interface ListTestsResult {
  ok: boolean;
  errors: ApiError[];
  tests: TestSummary[];
}

export interface GetTestResult {
  ok: boolean;
  errors: ApiError[];
  test: TestDto | null;
}

/** Why a change reaches a pipeline or route, mirroring `config_file::Reason`.
 * Internally tagged on `type`; a variant's own fields keep their Rust names
 * (`rename_all` on the enum renames variants, not a struct variant's
 * fields). */
export type Reason =
  | { type: 'edited' }
  | { type: 'readsTable'; table: string }
  | { type: 'namesGrokPattern'; pattern: string }
  | { type: 'calls'; pipeline: string }
  | { type: 'sourceChanged'; source: string }
  | { type: 'destinationChanged'; destination: string };

/** One pipeline or route a change reaches, and every reason -- reached two
 * ways at once keeps both. */
export type Reached = { id: string; reasons: Reason[] };

/** A scope and an id, combined into one opaque key. An id is unique only within a scope (`TestSummary.scope`'s
 * doc), so joining a run's results, a test list, or a touched `Reached` by id alone across scopes would
 * collide two groups' same-named tests or pipelines. */
export function scopedKey(scope: string, id: string): string {
  return `${scope} ${id}`;
}

/** Everything a change reaches. `pipelines` and `routes` are separate lists:
 * `RouteDto.id` and `PipelineDto.id` are different namespaces. `pipelineNames` holds every pipeline on
 * either side of the change, by id, to the name a reader sees in its place; a leader that predates it omits
 * it. */
export type Impact = {
  pipelines: Reached[];
  routes: Reached[];
  pipelineNames?: Record<string, string>;
};

/** One scope's `Impact`, tagged with the scope -- `Reached.id` is unique only within a scope, the same
 * reason `TestSummary.scope` carries one. */
export type ScopedImpact = { scope: string } & Impact;

export interface RunTestsResult {
  ok: boolean;
  errors?: ApiError[];
  results: TestResult[];
  /** Present when asked with `touched: true`: one entry per scope the run reached anything in, each
   * naming every pipeline and route that scope's change reaches, against `main`, with why. Something
   * named here that no result covers -- by id for a pipeline test, by
   * `TestSummary.pipelines`/`.routes` for an end-to-end one -- is reached and untested. */
  touched?: ScopedImpact[];
}

/** Every test's summary: the branch's own scope (the chip) when `onBranch` is absent, or -- when it names
 * a branch under review -- every scope that branch's own change reaches, never the chip's (a review reads
 * another open change's tests without switching the nav's own scope, any more than it switches its
 * branch). */
export function listTests(onBranch?: string): Promise<ListTestsResult> {
  return apiJson(onBranch ? reviewScoped('/api/tests', onBranch) : branched('/api/tests'));
}

export function getTest(id: string): Promise<GetTestResult> {
  return apiJson(branched(`/api/tests/${encodeURIComponent(id)}`));
}

/** One test of `inScope` at commit `at` of `onBranch`, read as `configAt` reads a bundle. */
export function testAt(
  onBranch: string,
  inScope: string,
  id: string,
  at: string,
): Promise<GetTestResult> {
  const q = new URLSearchParams({ branch: onBranch, scope: inScope, at });
  return apiJson(`/api/tests/${encodeURIComponent(id)}?${q}`);
}

/** Lock a test, or replace one under the same id. */
export function putTest(test: TestDto): Promise<MutationResult> {
  return configJson('/api/tests', putBody(test));
}

export function deleteTest(id: string): Promise<MutationResult> {
  return configJson(`/api/tests/${encodeURIComponent(id)}`, { method: 'DELETE' });
}

/** Run tests against a branch, other than the one being edited when `onBranch` names one -- a change under
 * review is not what the picker in the nav has open, and runs across that change's own scopes rather than
 * the chip's (`listTests`'s doc). Empty `ids` runs every test in scope. */
export function runTests(
  opts: { ids?: string[]; touched?: boolean } = {},
  onBranch?: string,
): Promise<RunTestsResult> {
  const url = onBranch ? reviewScoped('/api/tests/run', onBranch) : branched('/api/tests/run');
  return post(url, { ids: opts.ids ?? [], touched: opts.touched ?? false }, 'quiet');
}

// -- route preview ----------------------------------------------------------
//
// Where a sample's records would go: which routes match, what each matched
// pipeline produces, and which destinations the delivery rules choose
// (`runtime::preview_routes`). Distinct from `/api/preview`, which runs one
// pipeline over a sample and answers rows in, rows out.

/** One document a route produced, and where it would go. */
export interface RoutePreviewDocument {
  /** The document as it would leave, serialized. */
  document: string;
  /** Destination ids the delivery rules chose. Empty is a row every rule declined. */
  destinations: number[];
  dlq?: number;
  /** Every `__destination` value that steers the document, by its name under `__destination`. */
  destination?: Record<string, string>;
}

/** One route a record went through. */
export interface RoutePreviewThrough {
  route: number;
  /** More than one when the pipeline unrolls an array; none when it was
   * dropped or blocked. */
  documents: RoutePreviewDocument[];
  /** Each exception this route's pipeline raised on the record. */
  exceptions: { kind: string; message: string }[];
  /** The joined answer to them; `ignore` when there are none. */
  answer: AnswerName;
}

export interface RoutePreviewRow {
  row: number;
  /** Every route whose condition selected this record. */
  matched: number[];
  /** What each matched route made of it. */
  through: RoutePreviewThrough[];
  /** Why this record stopped short, where it did: it asked for more memory
   * than a preview may hold, it blocks its source, or another record of the
   * sample does -- a block holds every record polled with it. */
  refused?: string;
}

export interface RoutePreviewResult {
  ok: boolean;
  errors: ApiError[];
  /** The pipeline each route dispatches to, by route index. */
  route_pipelines: string[];
  /** The id of the pipeline each route dispatches to, position-parallel to
   * `route_pipelines`: what a locked test records. */
  route_pipeline_ids: string[];
  /** `RouteDto.id` by route index, position-parallel to `route_pipelines` --
   * resolves an index in `matched`/`through` to the id the config gave that
   * route, which an edit keeps and a delete-and-insert does not. */
  route_ids: string[];
  destination_names: Record<number, string>;
  /** What happens to a row no route matched, with the destination by its author's id. */
  unmatched: Answer;
  rows: RoutePreviewRow[];
  /** How the columns the sample was captured under differ from what its source
   * declares now. */
  warnings?: string[];
}

/** `config` is the whole bundle as the editor has it, so unsaved route edits
 * are what gets previewed. `atMicros` pins `now()` and a reference-less
 * `parseTime`, the way a locked test re-runs against the instant it locked
 * at; an ordinary preview omits it and reads the wall clock. */
export function routePreview(
  config: string,
  sample: string,
  atMicros?: number,
): Promise<RoutePreviewResult> {
  return post(
    branched('/api/route-preview'),
    { config, sample, at_micros: atMicros ?? null },
    'quiet',
  );
}

// -- capture ----------------------------------------------------------------
//
// Records taken from a source by the workers themselves. The leader hands
// every worker the same invitation on its next heartbeat and merges what
// comes back (`leader::capture`); a source's capture ends by being saved as a
// sample, which is where it and a paste become the same thing. A destination's
// is read and discarded.

/** Which bound ended one worker's capture. A bound reached is its own name; a failure
 * carries what went wrong, so "Failed" is never all a reader gets. */
export type CaptureEndDto = 'matches' | 'bytes' | 'deadline' | { failed: string };

/** What to show for a capture's end: the bound's name, or what failed. */
export function captureEndText(end: CaptureEndDto): string {
  return typeof end === 'string' ? end : `failed — ${end.failed}`;
}

/** Which catalog entry raised a warning (`src/leader/warnings.rs`). The catalog is
 * fixed, so a reader switches on this and a new entry is a type error rather than a
 * blank label. */
export type WarningCondition =
  | 'workerSilent'
  | 'workerNeverHeartbeated'
  | 'sourceBlocked'
  | 'sourceRefused'
  | 'sourceWaiting'
  | 'sourceUnreachable'
  | 'destinationRefusing'
  | 'destinationUnreachable'
  | 'lagClimbing'
  | 'sourceRowsBelow'
  | 'sourceRowsAbove'
  | 'groupNotDeployed'
  | 'workerNotDeployingItsTarget'
  | 'workerOnAnotherBuild'
  | 'workerMemoryHigh'
  | 'authorityExpiring'
  | 'authorityAging'
  | 'enrollmentCodeNamesOutgoingAuthority'
  | 'identityExpiring'
  | 'workerLoggedWarnings'
  | 'deployHeld'
  | 'remoteNotFollowed'
  | 'deployDoesNotLoad'
  | 'deployFailsTests'
  | 'declaredFieldFailing'
  | 'declaredFieldGone'
  | 'streamUnrouted'
  | 'newerRelease'
  | 'workerOnAnotherRelease'
  | 'secretsKeyUnusable'
  | 'secretNeedsValue'
  | 'acmeRenewalFailing'
  | 'leaderCertificateRunningOut';

/** *Failed*: the thing is not doing its job. *Warning*: it works and something is off. *Info*: a fact worth
 * knowing that asks nothing; the leader sends it only to a caller that asks (`listWarnings({ info: true })`). */
export type WarningSeverity = 'failed' | 'warning' | 'info';

/** A named condition about one object, derived fresh by the leader on every poll. Nothing
 * stores it and nothing acknowledges it: it is gone when its fact is. */
export interface WarningDto {
  condition: WarningCondition;
  subjectKind:
    | 'source'
    | 'destination'
    | 'worker'
    | 'group'
    | 'leader'
    | 'enrollmentCode'
    | 'identity'
    | 'pipeline'
    | 'secret';
  /** Unique only within `group`, not fleet-wide -- a worker's own id is the exception. */
  subjectId: string;
  /** Which group this is about, or null where there is none to report -- an enrolled worker
   * that never heartbeated has never said which group it belongs to. */
  group: string | null;
  severity: WarningSeverity;
  /** When the condition began, or null where the leader cannot know -- an enrolled worker
   * that never heartbeated may have just enrolled, or been quiet since before a restart. */
  sinceMillis: number | null;
  message: string;
  /** The log site a `workerLoggedWarnings` is about: one worker raises it once per site.
   * Absent on every other condition. */
  site?: string;
  /** The scope whose bundle declares a pipeline subject: `shared` or its group. Absent on every
   * other subject, and where the deployed commit does not say. */
  scope?: string;
}

/** What a reader scans before the sentence. */
export function conditionLabel(condition: WarningCondition): string {
  switch (condition) {
    case 'workerSilent':
      return 'Worker silent';
    case 'workerNeverHeartbeated':
      return 'Never heartbeated';
    case 'sourceBlocked':
      return 'Source blocked';
    case 'sourceRefused':
      return 'Blocked by destination refusals';
    case 'sourceWaiting':
      return 'Waiting on destination';
    case 'sourceUnreachable':
      return 'Source unreachable';
    case 'destinationRefusing':
      return 'Refusing writes';
    case 'destinationUnreachable':
      return 'Destination unreachable';
    case 'lagClimbing':
      return 'Lag climbing';
    case 'sourceRowsBelow':
      return 'Rows below normal';
    case 'sourceRowsAbove':
      return 'Rows above normal';
    case 'groupNotDeployed':
      return 'Group not deployed';
    case 'workerNotDeployingItsTarget':
      return 'Not deploying its target';
    case 'workerOnAnotherBuild':
      return 'On another build';
    case 'workerMemoryHigh':
      return 'Memory near its limit';
    case 'authorityExpiring':
      return 'Authority expiring';
    case 'authorityAging':
      return 'Authority running out';
    case 'enrollmentCodeNamesOutgoingAuthority':
      return 'Enrollment code names the outgoing authority';
    case 'identityExpiring':
      return 'Identity certificate running out';
    case 'workerLoggedWarnings':
      return 'Logged a warning';
    case 'deployHeld':
      return 'Deploy held';
    case 'remoteNotFollowed':
      return 'Remote not followed';
    case 'deployDoesNotLoad':
      return 'Deployed config does not load';
    case 'deployFailsTests':
      return 'Test failing';
    case 'declaredFieldFailing':
      return 'Field not its declared type';
    case 'declaredFieldGone':
      return 'Field no longer arriving';
    case 'streamUnrouted':
      return 'Stream unrouted';
    case 'newerRelease':
      return 'Newer release';
    case 'workerOnAnotherRelease':
      return 'On another release';
    case 'secretsKeyUnusable':
      return 'Secrets encryption key unusable';
    case 'secretNeedsValue':
      return 'Secret needs a new value';
    case 'acmeRenewalFailing':
      return 'ACME renewal failing';
    case 'leaderCertificateRunningOut':
      return 'Leader certificate running out';
  }
}

/** The catalog the caller sees. Info entries come only with `info`: every view that judges health reads without
 * it, so a notice can never make anything look unhealthy. */
export function listWarnings(opts: { info?: boolean } = {}): Promise<WarningDto[]> {
  return apiJson(opts.info ? '/api/warnings?info=true' : '/api/warnings');
}

/** One column of a captured batch, as sent. A column with `value` is constant: every row
 * has that value, and no row's `values` holds a place for it. */
export interface CaptureColumnDto {
  name: string;
  type: string;
  value?: unknown;
}

/** One captured row, as sent: the values of the batch's non-constant columns, in column
 * order. `failed` indexes into these `values`. */
export interface CapturedRowDto {
  values: unknown[];
  failed?: number[];
  /** The worker that sent it. */
  worker?: string;
}

/** Rows a capture took under one column list, the same at a source and at a destination
 * (`heartbeat::CaptureBatch`). */
export interface CaptureBatchDto {
  columns: CaptureColumnDto[];
  rows: CapturedRowDto[];
}

/** A captured row with every column's value. A failed value is kept as its source spelled
 * it, and `failed` holds the indexes of those columns. */
export interface CapturedRow {
  values: unknown[];
  failed: number[];
  worker?: string;
}

export interface CaptureBatch {
  columns: { name: string; type: string }[];
  rows: CapturedRow[];
}

/** A batch as sent, with each constant column's value put back on every row and `failed`
 * indexing the full column list. */
export function expandBatch(batch: CaptureBatchDto): CaptureBatch {
  /** For each value a row sends, the column it belongs to. */
  const varying: number[] = [];
  batch.columns.forEach((c, i) => {
    if (!('value' in c)) varying.push(i);
  });
  return {
    columns: batch.columns.map(({ name, type }) => ({ name, type })),
    rows: batch.rows.map((row) => {
      const values = new Array<unknown>(batch.columns.length);
      batch.columns.forEach((c, i) => {
        if ('value' in c) values[i] = c.value;
      });
      varying.forEach((col, i) => (values[col] = row.values[i] ?? null));
      const out: CapturedRow = { values, failed: (row.failed ?? []).map((i) => varying[i]) };
      if (row.worker !== undefined) out.worker = row.worker;
      return out;
    }),
  };
}

/** One worker in a capture, and where it stands. */
export interface CaptureWorkerDto {
  worker: string;
  /** `waiting` has sent no report the capture folded in. `filling` has, and has
   * not yet said why it stopped; the capture may hold none of its records.
   * `answered` has said why. */
  state: 'waiting' | 'filling' | 'answered';
}

export interface CaptureStatusDto {
  session: string;
  done: boolean;
  /** Ended by whoever was watching, before its deadline or its total. What it
   * holds is what it will ever hold. */
  stopped: boolean;
  /** How many records the capture asked for in all. */
  maxMatches: number;
  /** Every worker the capture knows of -- expected when it opened, or handed the
   * invitation since -- in name order. No worker leaves the list for the
   * capture's life. */
  workers: CaptureWorkerDto[];
  /** What the capture holds. Its record count is the rows across them. */
  batches: CaptureBatchDto[];
  /** Why each worker stopped, so a short sample says which bound it hit. A failure
   * carries the reason the worker had: the source it could not open, the filter that
   * would not compile. */
  ended: [string, CaptureEndDto][];
  secondsLeft: number;
  /**
   * Workers that were expected to answer and have not yet. Empty on a
   * finished capture that heard from everyone. Once a capture is over, only a
   * worker named here that had stopped before it ended may still say why;
   * the rest never answer.
   */
  waitingOn: string[];
  /** Whether the byte ceiling dropped records a worker sent. */
  truncated: boolean;
  /** What Run found out about an SQL source's query. */
  sql?: SqlRun;
  /** The fields of the newest Avro writer's schema a worker read with, in its order. */
  avro?: AvroField[];
}

/** One field an Avro writer's schema declares (`heartbeat::AvroField`). */
export interface AvroField {
  /** A nested record's field is named with dots. */
  name: string;
  /** As the schema spells it, such as `long` or `union[null, string]`. */
  avroType: string;
  /** The type it reads as unless declared otherwise. */
  suggested: string;
}

/** A capture's status with its batches expanded (`expandBatch`). */
export interface CaptureStatus extends Omit<CaptureStatusDto, 'batches'> {
  batches: CaptureBatch[];
}

export interface StartCaptureResult {
  ok: boolean;
  errors: ApiError[];
  session?: string;
  /** What the bounds actually became, which matters when a request was clamped. */
  maxMatches?: number;
  maxBytes?: number;
  deadlineSecs?: number;
}

export interface CaptureStatusResult {
  ok: boolean;
  errors: ApiError[];
  capture?: CaptureStatus;
}

async function expandStatus(
  res: Promise<Omit<CaptureStatusResult, 'capture'> & { capture?: CaptureStatusDto }>,
): Promise<CaptureStatusResult> {
  const { capture, ...rest } = await res;
  return capture
    ? { ...rest, capture: { ...capture, batches: capture.batches.map(expandBatch) } }
    : rest;
}

export interface SaveCaptureResult {
  ok: boolean;
  errors: ApiError[];
  commit?: string;
  records?: number;
}

/** What one sample holds (`config_file::MAX_SAMPLE_RECORDS`), and so the most a
 * capture may ask for: the leader clamps a larger total to it. */
export const MAX_SAMPLE_RECORDS = 1000;

/** The bounds a capture asks for; the leader clamps them and says what they became. */
interface CaptureBounds {
  /** How many records in all, at most MAX_SAMPLE_RECORDS. Each worker is handed
   * the same number as its own bound, so any one of them can fill the capture
   * alone. */
  maxMatches?: number;
  maxBytes?: number;
  deadlineSecs?: number;
}

/** A capture at a source or at a destination. `source` is the source as the
 * author has it: the invitation carries the configuration, since a worker
 * follows main and may never have seen it. `destination` names one the group's
 * workers already run: what it reads is what they send, and a worker that
 * does not send there answers so. Only a source's capture can be saved as a
 * sample. */
export function startCapture(
  body: CaptureBounds &
    (
      | {
          /** The worker group to ask. Only its workers are handed the source's
           * configuration. */
          group: string;
          source: SourceDto;
          /** The identity that source names. A worker may never have seen it either,
           * so it travels with the source. */
          identity?: IdentityDto;
          filter?: string;
          /** Where a Kafka or Event Hubs source's capture starts. Absent, it reads from the latest. */
          start?: StartPosition;
        }
      | {
          group: string;
          /** The destination, by the id its configuration gives it. */
          destination: string;
          /** Reads the row as its pipeline finished it: the fields the pipeline writes. */
          filter?: string;
        }
    ),
): Promise<StartCaptureResult> {
  return post('/api/captures', body, 'quiet');
}

export function captureStatus(session: string): Promise<CaptureStatusResult> {
  return expandStatus(apiJson(`/api/captures/${encodeURIComponent(session)}`, undefined, 'quiet'));
}

/** End a capture where it stands. What it holds can then be saved or discarded;
 * what arrives after is dropped. */
export function stopCapture(session: string): Promise<CaptureStatusResult> {
  return expandStatus(
    apiJson(`/api/captures/${encodeURIComponent(session)}/stop`, { method: 'POST' }, 'quiet'),
  );
}

/** Saving is a commit on a branch, so it goes through `configJson` like every
 * other save. `id` is the new sample's, minted by `mintSampleId`. */
export function saveCapture(
  session: string,
  id: string,
  name: string,
  /** The source the records stand for. None for a capture at a destination. */
  source?: string,
  /** As `configJson`'s: the commit the caller read, when it keeps its own. */
  base?: string | null,
  /** The scope the sample is saved into, when it is not the chip's: the source's own. */
  into?: string,
): Promise<SaveCaptureResult & { commit?: string }> {
  const url = `/api/captures/${encodeURIComponent(session)}/save`;
  return configJson(
    into ? `${url}?scope=${encodeURIComponent(into)}` : url,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id, name, source }),
    },
    // A capture says what went wrong in its own flow, from start to save.
    { base, scoped: !into, weight: 'quiet' },
  );
}

export function discardCapture(session: string): Promise<{ ok: boolean }> {
  return apiJson(
    `/api/captures/${encodeURIComponent(session)}/discard`,
    { method: 'POST' },
    'quiet',
  );
}

/** A pasted or uploaded file, as records. The whole text is tried as one
 * JSON value first: an array becomes one record per element, any other
 * value becomes one record. That covers a pretty-printed object and a JSON
 * array, which one-record-per-line does not. Anything that is not one JSON
 * value -- NDJSON, JSONL, a plain log file -- falls back to one record per
 * non-empty line. Either way each record is a byte record whose `_raw` is
 * one line: a parsed element is re-serialized with `JSON.stringify`. */
export function recordsFromText(text: string): SampleRecordDto[] {
  const elements = pastedElements(text);
  if (elements) return elements.map((el) => ({ _raw: JSON.stringify(el) }));
  return text
    .split('\n')
    .map((s) => s.trim())
    .filter((s) => s.length > 0)
    .map((line) => ({ _raw: line }));
}

/** `text` as one JSON value's records: an array's elements, or the value itself. `null` when `text` is not one JSON
 * value, and is read a line a record. */
export function pastedElements(text: string): unknown[] | null {
  try {
    const parsed: unknown = JSON.parse(text);
    return Array.isArray(parsed) ? parsed : [parsed];
  } catch {
    return null;
  }
}

// -- Cluster status (`src/bin/smeltr/cluster.rs`, served by the leader) ----

export type PartitionRef = { topic: string; partition: number };

export type MetricValueDto =
  | { kind: 'counter'; value: number }
  | { kind: 'float_counter'; value: number }
  | { kind: 'gauge'; value: number };

export type MetricSampleDto = { metric: string; labels: [string, string][]; value: MetricValueDto };

/** What a reporting thread does. Each stage is sized by something the
 * others cannot influence, so a worker's thread count is three numbers.
 * `worker` is the process itself, not a thread: a worker with nothing
 * deployed reports it alone, so it learns a directive, and a deployed one
 * reports its deployment-wide gauges under it. `null` is a worker older
 * than the pool, whose report is a thread. */
export type ThreadKind = 'reader' | 'transform' | 'egress' | 'worker';

/** What a worker process is spending against what it is allowed to run in, as reported on its
 * most recent heartbeat. Worker-level, like `commit`: repeated onto every thread the same way.
 * `cpuPercent` is `null` on a worker's first heartbeat -- a rate needs a previous reading. A
 * `Limited` flag true means the matching `Bytes`/`Percent` figure is measured against a real
 * container limit; false means it is measured against the whole host instead. */
/** What a worker's supervisor says it is doing. Only what the GUI reads is typed here. */
export type Supervising = {
  /** Every receiver the running deployment reads, and where it listens. Absent while nothing runs. */
  listens?: Listening[];
  /** The name the worker's host goes by. Absent where the host would not say. */
  hostName?: string;
};

/** One receiver's address, as its worker listens on it. */
export type Listening = {
  source: string;
  transport: 'tcp' | 'udp';
  /** `ip:port`, `[ip]:port` for IPv6. An unspecified IP is every address of the host. */
  address: string;
};

export type ResourceReport = {
  cpuPercent: number | null;
  /** How many cores `cpuPercent` is a percentage of. Fractional under a container quota, which is
   * what a k8s `cpu: 500m` limit is. Absent from a worker older than this field. */
  cpuLimitCores?: number;
  cpuLimited: boolean;
  memoryUsedBytes: number;
  memoryLimitBytes: number | null;
  memoryLimited: boolean;
  /** Free and total bytes of the filesystem under this worker's state directory -- the one volume
   * a worker owns. Absent from a worker older than these fields, and from one whose filesystem
   * would not answer. */
  diskFreeBytes?: number;
  diskTotalBytes?: number;
};

/** One worker thread as last heartbeated. `lastSeenMillisAgo` is the only
 * liveness signal: the leader keeps no online flag. `stopAfterUnreachableSecs`
 * is a worker startup setting (`smeltr.toml`), reported on every heartbeat and
 * repeated onto each thread the same way `groupId` already is -- one worker's
 * threads always agree with each other, but two workers in the same group can
 * disagree. `null` covers both "reported no limit" and "never heartbeated". */
export type ThreadHealth = {
  workerId: string;
  threadId: string;
  groupId: string;
  kind: ThreadKind | null;
  partitionsOwned: PartitionRef[];
  lastSeenMillisAgo: number;
  stopAfterUnreachableSecs: number | null;
  /** The commit whose bundle this worker reported running. `null` from a
   * worker running nothing: one that has not deployed yet, or whose
   * deployment stopped. Not read off `main` -- a worker that refused a
   * commit keeps running the one before it, and only it knows which. */
  commit: string | null;
  /** This worker's own build, as reported on its most recent heartbeat. Absent from a worker older than
   * this field. */
  smeltrVersion?: string;
  /** The release that build is, `1.2.0`. Absent on any other build, and from a worker older than this field. */
  release?: string;
  /** Absent from a worker older than this field. */
  resources?: ResourceReport;
  /** What this worker's supervisor says it is doing. Absent from a worker older than this field. */
  supervising?: Supervising;
  /** When this worker said it was stopping, where that is the last thing it
   * said. Absent from a worker that has said anything else since. */
  stoppedAtMillis?: number;
  metrics: MetricSampleDto[];
};

export type GroupHealth = {
  groupId: string;
  /** The pool: what `thread_limit` sizes, and the only one an operator sets. */
  transformThreads: number;
  readers: number;
  egressThreads: number;
  partitionsOwned: number;
};

export type ClusterHealth = {
  threads: ThreadHealth[];
  groups: GroupHealth[];
  /** How long a worker may go without a heartbeat before it is silent: the leader's own `workerSilent` window. */
  silentAfterMs: number;
};

/** A worker group as the repository holds it (`worker_groups_api::WorkerGroupOut`):
 * exists whether or not any thread reports into it. What it runs is not
 * stored: every group runs `main`'s head. */
export type WorkerGroupDto = {
  id: string;
  name: string;
  /** `true` once `main` holds this group's subtree, which is what makes it
   * real and live (design doc, Control plane). `false` is a group created on a branch
   * and not yet deployed. */
  deployed: boolean;
  /** The hue the group is drawn in, in degrees. Assigned when the group is
   * created and changed by an admin, so it is the group's own and never moves
   * under it. Absent on a group made before hues existed -- `scope.ts` gives
   * that one a slot. */
  hue?: number;
};

/** A worker credential. Every worker that has ever enrolled has one,
 * revoked or not, whether or not it is heartbeating. */
/** What a worker being gone means to the leader, from the enrollment code that brought it in.
 * `permanent`: missed until it comes back or is revoked. `ephemeral`: forgotten. */
export type WorkerKind = 'permanent' | 'ephemeral';

/** What an operator can ask of a worker, which it takes on its next heartbeat. */
export type DirectiveAction = 'restart' | 'shutdown';

export type WorkerCredentialDto = {
  workerId: string;
  revoked: boolean;
  /** Absent only where the roster has no entry for a worker that is heartbeating. */
  kind?: WorkerKind;
  /** What it does when asked, as of its last heartbeat to this leader. Absent from a worker older than
   * directives, and from one this leader has not heard from since it started. */
  accepts?: DirectiveAction[];
  /** What it has been asked and has not yet taken. */
  asked?: DirectiveAction;
};

export type ClusterStatus = {
  /** Set when the leader's store could not be read. `health` is in-memory
   * and still present; `workerGroups`/`workers` are `null`, never `[]`, so
   * an unreadable roster does not render as an empty one. */
  error: string | null;
  health: ClusterHealth | null;
  /** The commit every group runs. */
  deployedCommit: string | null;
  workerGroups: WorkerGroupDto[] | null;
  workers: WorkerCredentialDto[] | null;
};

export function getClusterStatus(): Promise<ClusterStatus> {
  return apiJson('/api/cluster');
}

// -- fleet management (`src/leader/mod.rs`, worker groups via
//    `src/bin/smeltr/worker_groups_api.rs`) --------------------------------
//
// Enrollment codes and revocation act on the fleet itself -- unbranched, and
// gated by the same operator cookie as everything else under `/api`. A worker
// group is different: it is a manifest in the config repository (design
// doc, Control plane), so listing and creating one names the branch,
// through `branchOnly()`
// -- never `scope=`, since a group's own id is its scope
// (`worker_groups_api::create_group_in`) and listing reads the whole tree.

/** What a worker-revoking call answers. */
export type AdminActionResult = { ok: boolean; errors: ApiError[] };

export interface ListWorkerGroupsResult {
  ok: boolean;
  errors: ApiError[];
  groups: WorkerGroupDto[];
  /** `shared`'s hue, in degrees. Absent where nothing has been chosen for it. */
  sharedHue?: number;
  /** The commit this list read the branch at. Adopted as the base for a write that follows without
   * one of its own to read first -- delete, in particular. */
  commit?: string;
}

/** Every group the branch being edited holds, each marked `deployed` when `main` holds it too.
 * `asBase` adopts the commit this reads as the base for a write that follows -- the Groups page's
 * own delete workflow, which has nothing else to read first. Every other caller -- a picker's
 * roster, the map -- leaves it false, so a read racing the page's own does not silently move the
 * base out from under an edit already in flight. */
export function listWorkerGroups(asBase = false): Promise<ListWorkerGroupsResult> {
  return configJson('/api/worker-groups', undefined, { scoped: false, adopt: asBase });
}

export interface CreateWorkerGroupResult {
  ok: boolean;
  errors: ApiError[];
  /** The commit this produced. Absent when the create was refused. */
  commit?: string;
  /** Present when the branch moving underneath it is why. */
  conflict?: SaveConflict;
}

/** Writes the group's manifest on the branch being edited. Does not open a
 * change -- the group is not live until one carrying it deploys (design
 * doc, Control plane). */
/** `hue` is the group's color in degrees; omitted takes the first default no
 * group holds. */
export function createWorkerGroup(
  id: string,
  name: string,
  hue?: number,
): Promise<CreateWorkerGroupResult> {
  return configJson(
    '/api/worker-groups',
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id, name, ...(hue === undefined ? {} : { hue }) }),
    },
    { scoped: false },
  );
}

/** Renames an existing group. A config write on a branch like any other, so
 * it is live once a change carrying it deploys. Creating and renaming are
 * separate calls because an id that is taken and an id that is free need
 * different answers. */
export function updateWorkerGroup(id: string, name: string): Promise<CreateWorkerGroupResult> {
  return configJson(
    `/api/worker-groups/${encodeURIComponent(id)}`,
    {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name }),
    },
    // Refused beside the name being edited.
    { scoped: false, weight: 'quiet' },
  );
}

/** Removes an existing group. A config write on a branch like any other. It
 * goes live once a change carrying it deploys, and the commit can be
 * reverted like any other. It does not touch workers: once it deploys, one
 * still running under this id has nothing to run and stops its pipelines
 * until it is moved to another group or shut down. */
export function deleteWorkerGroup(id: string): Promise<CreateWorkerGroupResult> {
  return configJson(
    `/api/worker-groups/${encodeURIComponent(id)}`,
    { method: 'DELETE' },
    {
      scoped: false,
    },
  );
}

export type SetGroupHueResult = { ok: boolean; errors: ApiError[] };

/** Sets `id`'s color, in degrees. Immediate: unlike every other worker-group
 * write, this is not a config write and needs no branch or deploy -- valid
 * for any id, including `SHARED_SCOPE`. */
export function updateGroupHue(id: string, hue: number): Promise<SetGroupHueResult> {
  return apiJson(
    `/api/worker-groups/${encodeURIComponent(id)}/hue`,
    {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ hue }),
    },
    'quiet',
  );
}

/** An enrollment code, as it is shown the once. The leader keeps only the hash
 * of what is inside `value`, so there is no reading it back. */
export interface MintedEnrollmentCode {
  id: string;
  /** The whole pasteable value: the secret and the authority to verify the
   * leader with, together. */
  value: string;
  groupId: string;
  kind: WorkerKind;
  /** `null`: no expiry of its own. */
  expiresAtMillis: number | null;
  /** `null`: no cap on how many workers it brings in. */
  maxUses: number | null;
}

export type MintEnrollmentCodeResult = {
  ok: boolean;
  errors: ApiError[];
  code: MintedEnrollmentCode | null;
};

/** What to mint. A permanent enrollment code names how many workers it brings in
 * and for how long; an ephemeral one has no cap and no expiry, and names neither. */
export type MintEnrollmentCodeRequest =
  | { groupId: string; kind: 'permanent'; maxUses: number; lifetimeHours: number }
  | { groupId: string; kind: 'ephemeral' };

export function mintEnrollmentCode(
  request: MintEnrollmentCodeRequest,
): Promise<MintEnrollmentCodeResult> {
  return apiJson('/api/enrollment-codes', {
    method: 'POST',
    body: JSON.stringify(request),
  });
}

/** An enrollment code still outstanding: not spent out, not expired, not withdrawn. */
export interface EnrollmentCodeDto {
  id: string;
  groupId: string;
  mintedBy: string;
  mintedAtMillis: number;
  kind: WorkerKind;
  /** `null`: no expiry of its own. */
  expiresAtMillis: number | null;
  /** `null`: no cap on how many workers it brings in. */
  maxUses: number | null;
  uses: number;
  /** The workers it brought in. When an enrollment code leaks, this is what to revoke. */
  usedBy: string[];
  /** When an operator withdrew it. Set means it brings nobody else in, whatever
   * its other dates say. */
  withdrawnAtMillis: number | null;
  /** Which authority it names, and so how long it goes on bringing a worker in.
   * `served`: the one the leader serves under and will still serve under once any
   * open rotation finishes. `untilCutover`: the one being rotated away from -- it
   * works until the cutover, which withdraws it. `replaced`: one the leader has
   * already stopped serving under, so it brings nobody in. */
  namesAuthority: 'served' | 'untilCutover' | 'replaced';
}

export interface ListEnrollmentCodesResult {
  ok: boolean;
  errors: ApiError[];
  codes: EnrollmentCodeDto[];
}

export function listEnrollmentCodes(): Promise<ListEnrollmentCodesResult> {
  return apiJson('/api/enrollment-codes');
}

export function withdrawEnrollmentCode(id: string, weight?: Weight): Promise<AdminActionResult> {
  return apiJson(`/api/enrollment-codes/${encodeURIComponent(id)}`, { method: 'DELETE' }, weight);
}

export function listWorkers(): Promise<WorkerCredentialDto[]> {
  return apiJson('/api/workers');
}

/** How long an ephemeral worker may be silent before the leader forgets it.
 * Seconds on the wire; the bounds come from the leader, so the form does not
 * restate them and drift. */
export interface WorkerSettingsDto {
  ephemeralForgetAfterSeconds: number;
  defaultSeconds: number;
  minSeconds: number;
  maxSeconds: number;
}

export type WorkerSettingsResult = {
  ok: boolean;
  errors: ApiError[];
  settings: WorkerSettingsDto | null;
};

export function getWorkerSettings(): Promise<WorkerSettingsResult> {
  return apiJson('/api/settings/workers');
}

export function putWorkerSettings(
  ephemeralForgetAfterSeconds: number,
): Promise<WorkerSettingsResult> {
  return apiJson('/api/settings/workers', {
    method: 'PUT',
    body: JSON.stringify({ ephemeralForgetAfterSeconds }),
  });
}

/** How much of a source's past throughput at this time of day its normal holds,
 * in percent. The bounds come from the leader, so the form does not restate them. */
export interface WarningSettingsDto {
  sourceRowsBandPercent: number;
  defaultPercent: number;
  minPercent: number;
  maxPercent: number;
}

export type WarningSettingsResult = {
  ok: boolean;
  errors: ApiError[];
  settings: WarningSettingsDto | null;
};

export function getWarningSettings(): Promise<WarningSettingsResult> {
  return apiJson('/api/settings/warnings');
}

export function putWarningSettings(sourceRowsBandPercent: number): Promise<WarningSettingsResult> {
  return apiJson('/api/settings/warnings', {
    method: 'PUT',
    body: JSON.stringify({ sourceRowsBandPercent }),
  });
}

/** Whether this leader looks for a newer release. */
export interface UpdateSettingsDto {
  check: boolean;
}

export type UpdateSettingsResult = {
  ok: boolean;
  errors: ApiError[];
  settings: UpdateSettingsDto | null;
};

export function getUpdateSettings(): Promise<UpdateSettingsResult> {
  return apiJson('/api/settings/updates');
}

export function putUpdateSettings(check: boolean): Promise<UpdateSettingsResult> {
  return apiJson('/api/settings/updates', { method: 'PUT', body: JSON.stringify({ check }) });
}

/** Why the last sync did not leave `main` at the remote's head. */
export interface RemoteFailureDto {
  commit: string | null;
  message: string;
  /** Unix millis. */
  since: number;
}

/** The git remote this leader follows (`src/leader/remote.rs`). The token is
 * never sent back, only whether one is stored. */
export interface RemoteSettingsDto {
  /** Empty when no remote is set. */
  url: string;
  user: string;
  tokenSet: boolean;
  /** 0 is off: Sync now only. */
  pollSeconds: number;
  /** Approve pushes to the remote's main. Off, changes are merged on the host. */
  approvePushes: boolean;
  minPollSeconds: number;
  maxPollSeconds: number;
  sync: {
    /** Unix millis. */
    syncedAt: number | null;
    remoteHead: string | null;
    failure: RemoteFailureDto | null;
  };
}

export type RemoteSettingsResult = {
  ok: boolean;
  errors: ApiError[];
  settings: RemoteSettingsDto | null;
};

export function getRemoteSettings(): Promise<RemoteSettingsResult> {
  return apiJson('/api/settings/git-remote');
}

/** `token` undefined keeps the stored one; empty removes it. An empty `url`
 * disconnects the remote and forgets its token. */
export function putRemoteSettings(req: {
  url: string;
  user: string;
  token?: string;
  pollSeconds: number;
  approvePushes: boolean;
}): Promise<RemoteSettingsResult> {
  return apiJson('/api/settings/git-remote', { method: 'PUT', body: JSON.stringify(req) });
}

export function syncRemote(): Promise<RemoteSettingsResult> {
  return apiJson('/api/settings/git-remote/sync', { method: 'POST' });
}

/** Asks a heartbeating worker to restart its deployment, on its next heartbeat. */
export function restartWorker(workerId: string): Promise<AdminActionResult> {
  return apiJson(
    `/api/workers/${encodeURIComponent(workerId)}/restart`,
    { method: 'POST' },
    'quiet',
  );
}

/** Asks a heartbeating worker to drain and exit, on its next heartbeat. */
export function shutdownWorker(workerId: string): Promise<AdminActionResult> {
  return apiJson(
    `/api/workers/${encodeURIComponent(workerId)}/shutdown`,
    { method: 'POST' },
    'quiet',
  );
}

/** Removes a worker that is not heartbeating, for good: revoked, and gone from
 * every list. Its id is never enrolled again. */
export function removeWorker(workerId: string): Promise<AdminActionResult> {
  return apiJson(
    `/api/workers/${encodeURIComponent(workerId)}/remove`,
    { method: 'POST' },
    'quiet',
  );
}

/** Soft revocation: refuses the credential from its next request. Does not
 * touch a process already running on it. */
export function revokeWorker(workerId: string): Promise<AdminActionResult> {
  return apiJson(
    `/api/workers/${encodeURIComponent(workerId)}/revoke`,
    { method: 'POST' },
    'quiet',
  );
}

// -- the secret catalog, `src/leader/mod.rs` -----------------------------
//
// A secret is a name with a declared custody. Nothing here carries a value:
// there is no route that returns one, and `verify` is how an admin checks
// one without being shown it.

/** One stored value. */
export type SecretVersionDto = {
  version: number;
  setAt: number;
  /** Whether the leader can hand this value out. `false` is a credential
   * the fleet cannot obtain, which refuses a deploy the way an unset
   * variable does. */
  usable: boolean;
};

/** One catalog row. */
export type SecretDto = {
  name: string;
  custody: 'worker-local' | 'leader-stored';
  description: string;
  /** Every version still held, oldest first. Empty for worker-local
   * custody, whose value the leader never had. */
  versions: SecretVersionDto[];
  /** The version each deployed group's bundle quotes. Empty means nothing
   * running names it -- which is what says a value just stored has not
   * reached the fleet. */
  deployed: [string, number][];
};

/** The newest version held, which is what a reference written today quotes. */
export const currentVersion = (s: { versions: SecretVersionDto[] }): SecretVersionDto | undefined =>
  s.versions.at(-1);

export function listSecrets(): Promise<SecretDto[]> {
  return apiJson('/api/secrets');
}

/** `value` is required for leader-stored custody and refused for
 * worker-local, whose value is never the leader's to hold. */
export function createSecret(
  name: string,
  custody: SecretDto['custody'],
  description: string,
  value?: string,
): Promise<AdminActionResult> {
  return apiJson('/api/secrets', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, custody, description, ...(value === undefined ? {} : { value }) }),
  });
}

/** Stores a value as a new version, keeping the ones still in use. It is
 * staged, not live: what deploys it is a bundle quoting that version. */
export function storeSecretValue(name: string, value: string): Promise<AdminActionResult> {
  return apiJson(`/api/secrets/${encodeURIComponent(name)}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ value }),
  });
}

/** What a rotation's second half is: a change, drafted on `branch`, pointing
 * every reference to this secret at `version`. Any version the leader holds,
 * so naming an older one is a credential rollback. */
export type PointSecretResult = {
  ok: boolean;
  errors: ApiError[];
  commit: string | null;
  files: string[];
  groups: string[];
  references: number;
  openedChange: boolean;
};

export async function pointSecretAt(
  name: string,
  version: number,
  branch: string,
): Promise<PointSecretResult> {
  const result = await apiJson<PointSecretResult>(
    `/api/secrets/${encodeURIComponent(name)}/references`,
    {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ version, branch }),
    },
  );
  // A config write like any other -- it drafts or advances a change -- but it goes through
  // apiJson rather than configJson, since it takes its branch in the body and carries no base
  // commit for configJson's compare-and-swap to track. That must not also cost it the one other
  // thing a write's response does: telling the review bar a change just moved.
  if (result.ok && result.commit && typeof window !== 'undefined') {
    window.dispatchEvent(new CustomEvent(SAVED_EVENT));
  }
  return result;
}

/** Points every reference to `name` at `version`, on the branch already
 * being edited -- asking for one first if that is `main` (W1) and something
 * names it. What an admin storing a new value triggers, so neither the
 * version nor the branch is theirs to pick. */
export async function pointSecretNow(name: string, version: number): Promise<PointSecretResult> {
  if (needsBranch(branch, 'PUT')) {
    // Asked on main, the leader writes nothing, and refuses only when there is
    // something to move: a value nothing names needs no branch.
    const onMain = await pointSecretAt(name, version, MAIN_BRANCH);
    if (onMain.ok) return onMain;
    const chosen = askForBranch ? await askForBranch(funnyBranchName()) : null;
    if (!chosen) {
      return {
        ...noBranchChosen<PointSecretResult>(),
        commit: null,
        files: [],
        groups: [],
        references: 0,
        openedChange: false,
      };
    }
    startBranch(chosen);
  }
  return pointSecretAt(name, version, branch);
}

/** Refused while a deployed bundle names it: the value is gone afterward,
 * and those workers could not get it back. */
export function deleteSecret(name: string): Promise<AdminActionResult> {
  return apiJson(`/api/secrets/${encodeURIComponent(name)}`, { method: 'DELETE' });
}

/** One certificate in the list: a chain and its private key, kept like a leader-stored secret. */
export type CertificateDto = {
  name: string;
  description: string;
  /** Every version still held, oldest first. */
  versions: SecretVersionDto[];
  /** The newest version's chain, leaf first. Empty where this leader cannot open it. */
  chain: DescribedCertificate[];
  /** Whether the leader serves a copy of it. */
  served: boolean;
  /** The names this leader issues it for, where this leader issued it and renews it. */
  issuedFor?: string[];
};

export function listCertificates(): Promise<CertificateDto[]> {
  return apiJson('/api/certificates');
}

/** `certificate` is PEM, leaf first, ending with its root for a private authority; `privateKey` is its PEM key. */
export function createCertificate(
  name: string,
  description: string,
  certificate: string,
  privateKey: string,
): Promise<AdminActionResult> {
  return apiJson('/api/certificates', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, description, certificate, privateKey }),
  });
}

/** Has this leader issue a certificate for `names` under its own authority, which it then renews. */
export function issueCertificate(
  name: string,
  description: string,
  names: string[],
  validityDays: number,
): Promise<AdminActionResult> {
  return apiJson('/api/certificates/issued', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, description, names, validityDays }),
  });
}

/** The lifetimes offered for a certificate this leader issues. Each renewal lasts the same. */
export const ISSUED_VALIDITY_DAYS = [90, 365, 2 * 365, 3 * 365] as const;

/** Stores a new version, which the leader serves too where it serves this certificate. */
export function replaceCertificate(
  name: string,
  certificate: string,
  privateKey: string,
): Promise<AdminActionResult> {
  return apiJson(`/api/certificates/${encodeURIComponent(name)}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ certificate, privateKey }),
  });
}

/** Whether a candidate is the stored value -- what answers "does this still
 * need rotating" without the value reaching the browser. Bounded to a few
 * attempts a minute per secret, and every attempt is logged by the leader. */
export function verifySecret(
  name: string,
  version: number,
  candidate: string,
): Promise<{ matches: boolean }> {
  return apiJson(`/api/secrets/${encodeURIComponent(name)}/verify`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ version, candidate }),
  });
}

/** Where the secrets encryption key stands (`EncryptionKeyOut`). `missing` and
 * `mismatched` refuse every stored secret until the key is restored or
 * regenerated. */
export interface EncryptionKeyDto {
  state: 'absent' | 'held' | 'missing' | 'mismatched';
  /** Absent where there is no key file. */
  keyId: string | null;
  path: string;
  /** Each value whose newest copy the key cannot open: what to enter again. */
  unopenable: { kind: 'secret' | 'setting'; name: string; label: string }[];
}

export function getEncryptionKey(): Promise<EncryptionKeyDto> {
  return apiJson('/api/encryption-key');
}

/** Writes a new key in place of a missing or mismatched one. */
export function regenerateEncryptionKey(): Promise<
  { ok: boolean; errors: ApiError[] } & Partial<EncryptionKeyDto>
> {
  return apiJson('/api/encryption-key/regenerate', { method: 'POST' });
}

// -- flow (E2), `src/leader/mod.rs` ----------------------------------------
//
// Rows in per source and rows out per destination, over a window the caller
// chooses -- the leader's own durable series, so it survives a reload.

/** One bucket of a series. Absent from `points` entirely when nothing
 * landed in it -- a gap, not a zero: an outage reads as no data, not as
 * throughput that dropped to nothing. */
export type FlowPointDto = { bucketMillis: number; value: number };

/** One worker's source or destination series within a `FlowResponseDto`. Group and
 * worker identity keep independently reported samples separate; older retained
 * series can lack the worker identity. */
export type FlowSeriesDto = {
  group: string;
  workerId?: string | null;
  subjectId: string;
  points: FlowPointDto[];
};

/** `bucketMillis` is whichever tier the window fit into -- the finest tier
 * that covers it, so a longer window is coarser rather than several tiers
 * stitched together. */
export type FlowResponseDto = {
  bucketMillis: number;
  rowsIn: FlowSeriesDto[];
  rowsOut: FlowSeriesDto[];
  bytesIn: FlowSeriesDto[];
  /** Payload bytes a destination acknowledged, which is a queue later than the
   * moment `rowsOut` counts a row. The two part company while a destination
   * falls behind, so this is the weight of what landed, not of what `rowsOut`
   * says left. */
  bytesOut: FlowSeriesDto[];
};

/** Clamped leader-side to at least one minute and at most thirty days. */
/** One log the fleet delivered. `fields` is the list of
 * `[name, {kind, value}]` pairs the call site passed, carried through
 * rather than reshaped -- see `logs.ts`'s `fields` for reading it. */
export type LogRowDto = {
  id: number;
  atMillis: number;
  workerId: string;
  group?: string | null;
  thread?: string | null;
  level: string;
  site: string;
  message: string;
  fields: unknown[];
};

export type LogsResponseDto = {
  rows: LogRowDto[];
  /** The page size actually used, after clamping. */
  limit: number;
  /** How long a message may be before the heartbeat cuts it, so the screen
   * can say that search stops there. */
  messageLimit: number;
  /** The cursor for "older", present only while a row exists below the page.
   * Absent means these are the last rows the filter has. */
  nextBefore: number | null;
  /** The cursor for "newer", present only while a row exists above the page.
   * Absent from the head, which a page reached with no cursor already is. */
  nextAfter: number | null;
};

/** `query` is `logs.ts`'s `toQuery` output: the filters plus a limit. */
export function getLogs(query: string): Promise<LogsResponseDto> {
  return apiJson(`/api/logs?${query}`);
}

export function getFlow(windowMinutes: number): Promise<FlowResponseDto> {
  return apiJson(`/api/flow?windowMinutes=${windowMinutes}`);
}

// -- the topology map (design doc "The map", `src/leader/map.rs`) ---------
//
// The whole fleet's compiled topology, one band per worker group: every source, pipeline and
// destination a route or delivery rule actually names, marked by the warning catalog. Reads what
// is committed, not an editor's in-progress buffer -- there is no draft form of a map, and no
// `scope=` either: the map has its own group filter, below the branch chip, unrelated to the
// scope an editor is working in (design doc "One fleet, many groups": "a group is a filter on that view rather than a mode that hides the rest").

export type MapNodeKind = 'source' | 'pipeline' | 'destination';

/** The system behind a source or destination -- what its glyph on the map says. Mirrors
 * `leader::map::System`; a variant added there is added here. */
export type MapSystem =
  | 'kafka'
  | 'eventHubs'
  | 'elasticsearch'
  | 'splunkHec'
  | 'http'
  | 's3'
  | 's3Sqs'
  | 'azureBlobQueue'
  | 'gcsPubSub'
  | 'gcs'
  | 'azureBlob'
  | 'sftp'
  | 'postgres'
  | 'mysql'
  | 'metrics'
  | 'logs'
  | 'iceberg'
  | 'delta';

/** One source, pipeline or destination, compiled inside one worker group. `authorId` and `name`
 * coincide for a source or destination -- neither has a display name apart from the id its
 * author gave it; a pipeline's differ.
 *
 * `group` is part of a node's identity, not a label on it: an author id is unique only within
 * one group plus `shared/`, never across the fleet, so two groups holding a source of the same
 * id are two nodes here. `${group} ${authorId}` (`map.ts`'s `nodeKey`) is this map's actual key. */
export type MapNode = {
  group: string;
  authorId: string;
  name: string;
  kind: MapNodeKind;
  /** Absent for a pipeline. */
  system?: MapSystem;
  /** Declared in `shared/` rather than in this group's own subtree -- still one running thing
   * per group that merges it in, just not authored here. */
  shared: boolean;
  /** The warning catalog names this object in this group. Always `false` for a pipeline: no
   * catalog entry has one as its subject. */
  warning: boolean;
};

/** A source→pipeline edge: one route, inside one group. */
export type RouteEdge = {
  group: string;
  route: string;
  condition?: string;
  source: string;
  pipeline: string;
};

/** A pipeline→destination edge: one destination out of one delivery rule on one route, inside
 * one group. A rule naming several destinations is several edges. */
export type DeliverEdge = {
  group: string;
  route: string;
  when?: string;
  pipeline: string;
  destination: string;
};

/** A source→destination edge: one of the deployment's unmatched destinations, for a source a row can fall
 * through. No route and no pipeline sit on this path -- the row arrives as it was read, skipping
 * the map's middle column.
 *
 * Drawn from configuration: an unmatched row is an ongoing disposition, so the path exists
 * whether or not anything is on it right now. */
export type UnmatchedEdge = {
  group: string;
  source: string;
  destination: string;
};

/** A source→destination edge: a destination the deployment's `unprocessable` answer names, for a source that
 * has actually diverted a batch to it.
 *
 * Drawn from traffic, unlike `UnmatchedEdge`: every source can panic on a batch, so
 * configuration says "all of them", and every source lined up against one node says nothing. */
export type PoisonEdge = {
  group: string;
  source: string;
  destination: string;
  /** Batches, not rows: nothing processed a poisoned batch, so it has none to count. Always
   * present -- a reading is what draws this edge. */
  throughput: number;
};

/** A node→destination edge: an exception answer delivering there -- from a pipeline, for the kinds
 * its route answers with deliver; from a destination, for what it refuses. Drawn from
 * configuration, like `UnmatchedEdge`. */
export type ExceptionEdge = {
  group: string;
  fromKind: 'pipeline' | 'destination';
  from: string;
  /** The route whose answer this is; absent for a destination's. */
  route?: string;
  destination: string;
  kinds: string[];
};

/** Why one group has no graph: every problem `src/leader/map.rs` raised for it, as plain strings:
 * there is no second, structured form of the same message. */
export type MapGroupError = {
  group: string;
  errors: string[];
};

/** The fleet's map, or why it has none at all. `ok` is false only for a problem that kept every
 * group from being read at all; one group failing to compile does not clear `nodes` -- it is named
 * in `groupErrors` while every other group still draws. */
export type MapResponse = {
  ok: boolean;
  errors: ApiError[];
  nodes: MapNode[];
  routes: RouteEdge[];
  delivers: DeliverEdge[];
  unmatched: UnmatchedEdge[];
  exceptions: ExceptionEdge[];
  poison: PoisonEdge[];
  groupErrors: MapGroupError[];
  /** The commit this was compiled from. `MeasurementsResponse.commit` moving away from it means
   * the topology has changed since. */
  commit?: string;
};

/** Branch only -- never `scope=`, which names an editor's own bundle and has nothing to do with
 * what the fleet runs. */
export function getMap(): Promise<MapResponse> {
  return apiJson(`/api/map?branch=${encodeURIComponent(branch)}`, undefined, 'quiet');
}

// -- map measurements (design doc "The map") -------------------------------
//
// What each source and destination reports now, read off the registry alone so the map can poll
// it without recompiling the fleet (`src/leader/measurements.rs`). A node with nothing to report
// is absent rather than zeroed.

/** One destination's queues, by its busiest current worker -- never an average, which would hide
 * one full worker among idle ones. */
export type QueueSummaryDto = {
  /** The fullest current worker's held bytes over its budget. Can pass 1. Absent when no current
   * worker reports. */
  busiest?: number;
  /** Current workers at or past 90% of their budget. */
  nearCapacity: number;
  /** Workers whose current heartbeat reports this queue. */
  fresh: number;
  /** Workers that reported it but whose last heartbeat is stale. */
  stale: number;
  /** Every worker the leader knows in the group, silent ones included. */
  workers: number;
};

/** One source or destination in one group, keyed like `MapNode` by group, kind and author id. */
export type NodeReadingDto = {
  group: string;
  kind: MapNodeKind;
  id: string;
  /** Rows per second over the newest closed bucket: read for a source, acknowledged for a
   * destination. Absent when that bucket is not current. */
  rate?: number;
  /** When the newest bucket with any traffic closed. Present without `rate`, it dates a stale
   * reading. */
  lastSeenMillis?: number;
  queue?: QueueSummaryDto;
  /** A source's Kafka lag in records, over its partitions reporting now. */
  lag?: number;
  /** The worker holding a listing source, which alone reads it. Absent when no worker holds it. */
  holder?: string;
  /** When the leader last heard from `holder`. Absent with `holder` present: not heard since the leader
   * started. */
  holderHeardMillis?: number;
  /** How many entries the held listing source's ledger has. Absent when no worker holds it. */
  ledgerEntries?: number;
  /** A source's rows per second that no route claimed, over the same bucket as `rate`. Absent when
   * that bucket is not current. */
  unmatched?: number;
  /** A source's normal rows per second at this time of day, learned from the last 7 days.
   * Absent while it has too little history to have one. */
  expected?: { low: number; high: number };
  /** The warning catalog's warnings naming this node, one per condition. */
  warnings: WarningDto[];
};

export type MeasurementsResponse = {
  ok: boolean;
  errors: ApiError[];
  /** The commit the branch is at now. */
  commit?: string;
  /** The leader's clock when it answered. */
  nowMillis: number;
  /** How many milliseconds a rate is read over. */
  bucketMillis: number;
  nodes: NodeReadingDto[];
  /** The catalog's warnings naming a group but no source or destination in it: a worker's, an
   * identity's. */
  groupWarnings: WarningDto[];
};

export function getMapMeasurements(): Promise<MeasurementsResponse> {
  return apiJson(`/api/map/measurements?branch=${encodeURIComponent(branch)}`, undefined, 'quiet');
}

// -- streams (design doc, Observability: Streams) ----------------------------

/** One stream of rows a source received that no route took (`series::StoredStreamKey`). */
export type StreamKeyDto = {
  key: string;
  rows: number;
  firstSeenMs: number;
  lastSeenMs: number;
  /** The route condition that takes this stream. Absent where the group's deployed bundle does not declare the
   * source. */
  condition?: string;
  /** Rows came under it within the hour: its source's `streamUnrouted` warning names it. */
  recent: boolean;
};

/** What one group's workers saw arrive at one source that no route took (`series::StoredUnmatched`). */
export type UnmatchedStreamsDto = {
  groupId: string;
  source: string;
  rows: number;
  keysDropped: number;
  firstSeenMs: number;
  lastSeenMs: number;
  keys: StreamKeyDto[];
};

/** One field a Parser produced in production (`series::StoredFieldShape`). */
export type FieldShapeDto = {
  name: string;
  declared: boolean;
  suggested: string;
  present: number;
  bools: number;
  ints: number;
  floats: number;
  strings: number;
  arrays: number;
  objects: number;
  failed: number;
  times: number;
  firstSeenMs: number;
  lastSeenMs: number;
};

/** What one group's workers saw one Parser step produce, on one source's route (`series::StoredParserShape`). */
export type ParserShapeDto = {
  groupId: string;
  source: string;
  route: string;
  pipeline: string;
  step: string;
  rows: number;
  fieldsDropped: number;
  firstSeenMs: number;
  lastSeenMs: number;
  fields: FieldShapeDto[];
};

type StreamsDto = { parsers: ParserShapeDto[]; unmatched: UnmatchedStreamsDto[] };

/** `GET /api/streams` at the groups that run `scope`: its own group, or every group for `shared`. */
function streamsAt(scope: string): Promise<StreamsDto> {
  const at = (g: { groupId: string }) => scope === SHARED_SCOPE || g.groupId === scope;
  // Best-effort everywhere it is read: what production saw is offered, never needed.
  return apiJson<StreamsDto>('/api/streams', undefined, 'quiet').then((r) => ({
    parsers: r.parsers.filter(at),
    unmatched: r.unmatched.filter(at),
  }));
}

/** What arrived that no route took, at the groups that run `scope`. */
export function streamsIn(scope: string): Promise<UnmatchedStreamsDto[]> {
  return streamsAt(scope).then((r) => r.unmatched);
}

/** What the Parser steps of `scope`'s pipelines produced in production, at the groups that run `scope`. */
export function parsersIn(scope: string): Promise<ParserShapeDto[]> {
  return streamsAt(scope).then((r) => r.parsers);
}

// -- object stats (design doc "The map": the focus view) -------------------
//
// One node's counts over a window, for the view a stats control opens. Only one ratio here is
// exact: `unmatched / polled` counts the same rows twice at the same place. A route's `matched`
// is an upper bound on what it delivered -- the pipeline can drop a row and a delivery rule's
// `when` can decline it -- and every route sees every row, so the shares can sum past `polled`.
// That is routing, not an error to reconcile.

/** One route out of a source, and how much of that source it took. Named by the pipeline it
 * feeds, since a route's own id is minted rather than authored (`mintRouteId`). */
export type RouteShareDto = {
  route: string;
  pipeline: string;
  condition?: string;
  matched: number;
};

/** One bucket of a spread: when it started and what it measured. */
export type SpreadPointDto = { bucketMillis: number; value: number };

/** How one figure moved across the window's buckets, oldest first. The bucket holding now is left
 * out, since a rate off a part of a bucket reads low; a bucket nothing wrote is absent, so `mean`
 * is over the buckets present. Absent from its DTO when no bucket qualifies. */
export type SpreadDto = {
  min: number;
  max: number;
  mean: number;
  buckets: SpreadPointDto[];
};

export type SourceStatsDto = {
  polled: number;
  unmatched: number;
  unmatchedPolicy: Answer;
  routes: RouteShareDto[];
  /** Rows read per second, bucket by bucket. */
  rate?: SpreadDto;
  /** Payload bytes read per second, bucket by bucket. */
  bytesRate?: SpreadDto;
  /** Microseconds inside the source's own read per row read, over the buckets that read a row. */
  pollPerRow?: SpreadDto;
  /** What the source was expected to read per second, span by span, oldest first. Absent while it
   * has too little history (`throughput::expected_over`). */
  expected?: ExpectedPointDto[];
};

/** The rows per second a source was expected to read from `fromMillis` to `toMillis`. */
export type ExpectedPointDto = {
  fromMillis: number;
  toMillis: number;
  low: number;
  high: number;
};

/** One step of a pipeline, and what it did to the rows that reached it. `errored` is summed across
 * every kind of error; which kind is not broken out. */
export type StepShareDto = {
  step: string;
  dropped: number;
  errored: number;
  micros: number;
};

/** Both counts are exact: a route's matched mask is the batch the pipeline runs over, and a row
 * leaves only through a step that drops it. */
export type PipelineStatsDto = {
  rowsIn: number;
  rowsOut: number;
  /** Absent when no row reached it, where a rate would be a division by zero dressed as an
   * answer. Per row rather than a total, which would only say how busy the window was. */
  microsPerRow?: number;
  steps: StepShareDto[];
  /** Rows handed to it per second, bucket by bucket. */
  rate?: SpreadDto;
  /** Microseconds its steps spent per row, over the buckets that took a row. */
  perRow?: SpreadDto;
};

/** Rows one source had refused by this destination -- the only count carrying both ends. */
export type SourceRefusalsDto = { source: string; rejected: number };

export type DestinationStatsDto = {
  /** Rows handed to this destination. Counted before dispatch, before any attempt has left. */
  received: number;
  /** Sends that failed for a reason that may pass: retried, not lost. */
  sendFailed: number;
  /** Refused for good, which no retry would change. */
  rejected: number;
  /** Acknowledged by the destination. The only count here that means a row actually arrived. */
  delivered: number;
  stalls: number;
  /** Worst first; empty when nothing was refused. What *arrived* cannot be split this way:
   * `received` names the destination alone. */
  rejectedBySource: SourceRefusalsDto[];
  /** What each worker's queue for this destination holds right now, fullest first. Empty where no
   * worker running it has heartbeated recently. */
  queues: QueueFillDto[];
  /** The mean round trip to the brokers in milliseconds, from the freshest worker reporting one.
   * `null` for a destination that does not measure it, or that nobody is running. */
  roundTripMs: number | null;
  /** Milliseconds the far end held requests back over the window, summed over concurrent requests. Above zero the
   * far end binds. */
  throttledMs: number;
  /** Rows acknowledged per second, bucket by bucket. */
  rate?: SpreadDto;
  /** Bytes acknowledged per second, bucket by bucket, as the queue measured them. */
  bytesRate?: SpreadDto;
  /** The round trip in milliseconds, bucket by bucket. A gauge's bucket keeps its highest reading,
   * so each point is that bucket's peak and `min` is the lowest peak, not the lowest sample. */
  roundTrip?: SpreadDto;
};

/** One worker's queue for a destination, as of its freshest heartbeat.
 *
 * The one figure on this panel that is not a total over the window. A queue's fill is a level: it
 * is true at the instant the worker read it and means nothing added up, so the leader takes it off
 * the latest beat rather than the series (`src/leader/stats.rs`, `Live`).
 *
 * That instant is not the beat. The egress thread reads its own queue, and stops while it sits out
 * a retry backoff, so a stalling destination reports the least it was holding. */
export type QueueFillDto = {
  /** The worker holding it. A destination has one queue per worker running it. */
  worker: string;
  /** Bytes held: queued, plus sent and not yet acknowledged. */
  bytes: number;
  /** What that worker may hold before it refuses an offer. */
  budget: number;
  /** The worker's last heartbeat is too old to count: this is the last fill it reported, not
   * what it holds now. Stale workers come after every current one. */
  stale: boolean;
};

/** `ok` is false for a subject the leader cannot answer for -- one the group does not declare --
 * rather than zeroes, which would read as "nothing happened". Exactly one of
 * `source`/`pipeline`/`destination` is present, the one matching `kind`. */
export type StatsResponse = {
  ok: boolean;
  errors: ApiError[];
  group: string;
  kind: MapNodeKind;
  id: string;
  windowMillis: number;
  /** The leader's clock when it answered: the clock the buckets are on, and where a spread's grid
   * ends. The browser's clock may sit a bucket off it. */
  nowMillis: number;
  /** The bucket width behind the totals: how coarse the answer is. */
  bucketMillis: number;
  source?: SourceStatsDto;
  pipeline?: PipelineStatsDto;
  destination?: DestinationStatsDto;
  /** The warnings about a source or destination that held at any time in the window, oldest
   * first (`warnings::WarningSpan`). */
  warnings?: WarningSpanDto[];
};

/** When one warning held. `endedMillis` is absent while it still holds. */
export type WarningSpanDto = {
  condition: WarningCondition;
  severity: WarningSeverity;
  startedMillis: number;
  endedMillis?: number;
};

export function getStats(
  group: string,
  kind: MapNodeKind,
  id: string,
  windowMinutes: number,
): Promise<StatsResponse> {
  const q = new URLSearchParams({
    branch,
    group,
    kind,
    id,
    windowMinutes: String(windowMinutes),
  });
  return apiJson(`/api/stats?${q}`);
}

// -- pipeline callers -----------------------------------------------------
//
// Direct callers only, read off `Call` steps alone (`config_file::Links`), so a pipeline
// mid-edit that does not currently compile still answers this. The map's `calls:` search reads it.

export type PipelineCallersResponse = {
  ok: boolean;
  errors: ApiError[];
  /** Pipeline id -> the pipelines that call it, directly. */
  callers: Record<string, string[]>;
};

export function getPipelineCallers(): Promise<PipelineCallersResponse> {
  return apiJson(branched('/api/pipeline-callers'), undefined, 'quiet');
}

// -- connections (blast radius, user story A5) ----------------------------
//
// What a change to one object reaches, on the branch being edited (`leader::connections`).

export type ConnectionKind =
  'source' | 'route' | 'pipeline' | 'destination' | 'lookupTable' | 'grokPattern' | 'identity';

export type Connected = {
  kind: ConnectionKind;
  id: string;
  /** The group that declares it, or `shared`. */
  scope: string;
};

export type ConnectionsResponse = {
  ok: boolean;
  errors: ApiError[];
  /** What it holds a contract with on fields, though upstream: a pipeline's sources, a destination's
   * pipelines. Empty for every other kind. */
  upstream: Connected[];
  downstream: Connected[];
  /** The groups that run it, of those the caller may see. */
  groups: string[];
  /** Of `groups`, those whose map draws it. */
  drawnIn: string[];
};

export function getConnections(kind: ConnectionKind, id: string): Promise<ConnectionsResponse> {
  const q = new URLSearchParams({ kind, id });
  return apiJson(branched(`/api/connections?${q}`), undefined, 'quiet');
}

// -- branches -------------------------------------------------------------

/** A branch opened for review. `state` is `open` or `closed`; `reason` is why
 * it was closed, and is empty while it is open. Times are unix seconds. */
export type ChangeDto = {
  state: string;
  title: string;
  description: string;
  opened: number;
  updated: number;
  reason: string;
  /** Who opened the change, as their name. */
  author: string;
  /** The opener's account, when the change was opened by a signed-in account. */
  authorId?: string;
};

/** `commits` is what the branch has that `main` does not, `behind` what `main`
 * has that it does not -- which is what makes a change stale. */
export type BranchDto = {
  name: string;
  head: string;
  /** The commit the branch left `main` at: what its objects are compared against. */
  base: string;
  lastSave: number;
  commits: number;
  behind: number;
  touches: string[];
  change?: ChangeDto;
  /** Present only while a git remote is set. */
  push?: PushDto;
};

/** Where a branch stands against the host's branch of the same name. `waiting` is not pushed yet, or
 * a push that failed and will be retried; its `message` is why it failed, and empty when it is simply
 * next in line. `refused` is the host's branch holding commits this leader did not push; `hostTip` is
 * the host's commit, absent when the host has no such branch. */
export type PushDto = {
  state: 'pushed' | 'waiting' | 'refused';
  message: string;
  hostTip?: string;
};

export interface ListBranchesResult {
  ok: boolean;
  errors: ApiError[];
  branches: BranchDto[];
  /** The remote's URL, present only while one is set. Review and merge then happen on the host, unless
   * `approvePushes`. */
  remote?: string;
  /** Approve pushes to the remote's main. */
  approvePushes: boolean;
}

/** Every branch but `main`, which is what deploys move rather than work in
 * flight. */
export function listBranches(): Promise<ListBranchesResult> {
  return apiJson('/api/branches');
}

/** What every branch, change and deploy call answers. `head` is where the ref
 * ended up, so an editor can send it as its base on the next save. `warning`
 * is about the state the call succeeded into -- a merge that landed on a tree
 * that no longer loads -- and is not an error. */
export interface BranchMutationResult {
  ok: boolean;
  errors: ApiError[];
  head?: string;
  warning?: string;
  /** What bringing a change current needs a side chosen for; nothing was written. */
  conflicts?: MergeConflicts;
  /** Each group that would not load at the commit a deploy was asked for.
   * Nothing was deployed; `deployCommit` with `confirm` deploys it. */
  doesNotLoad?: { group: string; why: string }[];
}

/** A side of a merge: `main`, or the branch being brought current with it. */
export type MergeSide = 'main' | 'work';

/** One file both sides changed, or one object (`id`) of a section file. A
 * version is `null` on a side that does not have it. */
export interface MergeConflict {
  path: string;
  id?: string;
  base: string | null;
  main: string | null;
  work: string | null;
}

/** The conflicts, and the two tips they were found at. A choice is only good
 * against the same two. */
export interface MergeConflicts {
  main: string;
  work: string;
  conflicts: MergeConflict[];
}

function post<T>(url: string, body?: unknown, weight?: Weight): Promise<T> {
  return apiJson(
    url,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body ?? {}),
    },
    weight,
  );
}

export function createBranch(name: string): Promise<BranchMutationResult> {
  return post('/api/branches', { name });
}

export function deleteBranch(name: string): Promise<BranchMutationResult> {
  return apiJson(`/api/branches/${encodeURIComponent(name)}`, { method: 'DELETE' });
}

export interface OverwriteResult {
  ok: boolean;
  errors: ApiError[];
}

/** Push a refused branch at `tip` over the host's, but only while the branch is still at `tip`
 * and the host at `hostTip` -- the commits the person was shown and agreed to. `null` is the host
 * having no such branch. */
export function overwriteBranch(
  name: string,
  tip: string,
  hostTip: string | null,
): Promise<OverwriteResult> {
  return post(`/api/branches/${encodeURIComponent(name)}/overwrite`, { tip, hostTip }, 'quiet');
}

/** `status` is `A`, `M` or `D`. */
export type FileChangeDto = {
  path: string;
  status: string;
  added: number;
  removed: number;
};

export interface DiffResult {
  ok: boolean;
  errors: ApiError[];
  files: FileChangeDto[];
  /** Unified diff, for the whole change or for `path` alone. */
  diff: string;
}

export function branchDiff(name: string, path?: string): Promise<DiffResult> {
  const q = path ? `?path=${encodeURIComponent(path)}` : '';
  return apiJson(`/api/branches/${encodeURIComponent(name)}/diff${q}`);
}

/** What landed between two commits. A refused save names both, and this is
 * what the writer holding the only copy of their change reads. */
export function commitDiff(from: string, to: string, path?: string): Promise<DiffResult> {
  const q = new URLSearchParams({ from, to });
  if (path) q.set('path', path);
  return apiJson(`/api/diff?${q}`);
}

/** A change was opened, closed or retitled. */
export const CHANGE_EVENT = 'smeltr:change';

/** The shell's review bar says whether a branch is open for review, and the
 * page that changed that is still on screen, so no hash change tells it. */
async function announced(call: Promise<BranchMutationResult>): Promise<BranchMutationResult> {
  const res = await call;
  if (res.ok && typeof window !== 'undefined') window.dispatchEvent(new CustomEvent(CHANGE_EVENT));
  return res;
}

/** Open a branch for review against `main`. The title becomes the commit
 * message the deploy carries onto `main`. */
export function openChange(
  branch: string,
  title: string,
  description: string,
): Promise<BranchMutationResult> {
  return announced(post('/api/changes', { branch, title, description }, 'quiet'));
}

/** Closing keeps the branch, so no work is lost. A reason is offered and
 * never required (W6). */
export function closeChange(branch: string, reason: string): Promise<BranchMutationResult> {
  return announced(post(`/api/changes/${encodeURIComponent(branch)}/close`, { reason }, 'quiet'));
}

/** Rename an open change. The title is `main`'s commit subject and the
 * description its body, so this is the last chance to say what shipped in
 * words a person chose. */
export function retitleChange(
  branch: string,
  title: string,
  description: string,
): Promise<BranchMutationResult> {
  return announced(
    apiJson(
      `/api/changes/${encodeURIComponent(branch)}`,
      {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ title, description }),
      },
      'quiet',
    ),
  );
}

/** What an inline comment sits on: an object of the change (and, for a
 * pipeline, one of its steps, by the step's id), or a line of a file as it
 * stood at the branch head the comment was written at. */
export type CommentAnchor =
  | { kind: 'object'; scope: string; section: ConfigSection; objectId: string; step?: string }
  | { kind: 'line'; path: string; line: number };

/** One thing that happened to a change. `opened`, `renamed` and `reopened`
 * carry the title and description they left it with; `closed` carries the
 * reason as `text`, `comment` and the two review verdicts their body (a
 * verdict's may be empty), and `saved` the commit's subject and `commit`.
 *
 * A comment that starts a thread may carry an `anchor`; a reply carries
 * `replyTo`, the id of the comment that started its thread, and sits where
 * that one does. `resolved` and `unresolved` carry `replyTo` too, and the
 * latest of them says whether the thread is resolved. */
export interface TimelineEvent {
  kind:
    | 'opened'
    | 'renamed'
    | 'reopened'
    | 'closed'
    | 'comment'
    | 'saved'
    | 'approved'
    | 'changes-requested'
    | 'resolved'
    | 'unresolved';
  /** The marker commit that records the event; a save has none. */
  id?: string;
  /** Unix seconds. */
  time: number;
  author: string;
  authorId?: string;
  title?: string;
  description?: string;
  text?: string;
  commit?: string;
  /** The commit a save was made on. */
  parent?: string;
  anchor?: CommentAnchor;
  /** The branch head when the comment was written. */
  at?: string;
  replyTo?: string;
  /** A later save changed what the comment sits on. */
  outdated?: boolean;
}

export interface TimelineResult {
  ok: boolean;
  errors: ApiError[];
  /** Oldest first. */
  events: TimelineEvent[];
}

/** What happened to the change on `branch`, oldest first. */
export function changeTimeline(branch: string): Promise<TimelineResult> {
  return apiJson(`/api/changes/${encodeURIComponent(branch)}/timeline`);
}

/** Comment on the change on `branch`: on the change as a whole, on what
 * `anchor` names, or as a reply in the thread that `replyTo` started. */
export function commentOnChange(
  branch: string,
  body: string,
  opts: { anchor?: CommentAnchor; replyTo?: string } = {},
): Promise<BranchMutationResult> {
  return post(`/api/changes/${encodeURIComponent(branch)}/comments`, { body, ...opts }, 'quiet');
}

/** Resolve the thread comment `id` started, or reopen it. */
export function resolveThread(
  branch: string,
  id: string,
  resolved: boolean,
): Promise<BranchMutationResult> {
  return post(
    `/api/changes/${encodeURIComponent(branch)}/threads/${encodeURIComponent(id)}`,
    {
      resolved,
    },
    'quiet',
  );
}

/** A reviewer's verdict on a change. */
export type ReviewVerdict = 'approve' | 'request-changes';

/** Review the change on `branch`. Refused on a closed change, for the change's own author, and for
 * a request for changes with no body. */
export function reviewChange(
  branch: string,
  verdict: ReviewVerdict,
  body: string,
): Promise<BranchMutationResult> {
  return post(`/api/changes/${encodeURIComponent(branch)}/reviews`, { verdict, body }, 'quiet');
}

/** Approve: squash-merges onto `main` and deletes the branch. Merging is
 * deploying -- there is no separate deploy step (W5). */
export function deployChange(branch: string): Promise<BranchMutationResult> {
  return post(`/api/changes/${encodeURIComponent(branch)}/deploy`, undefined, 'severe');
}

/** Bring a change current with `main`, which a change behind it must be
 * before it can deploy (W4). */
export function bringCurrent(
  branch: string,
  chosen?: { conflicts: MergeConflicts; sides: MergeSide[] },
): Promise<BranchMutationResult> {
  const choices = chosen && {
    main: chosen.conflicts.main,
    work: chosen.conflicts.work,
    picks: chosen.conflicts.conflicts.map((c, i) => ({
      path: c.path,
      id: c.id,
      side: chosen.sides[i],
    })),
  };
  return post(`/api/changes/${encodeURIComponent(branch)}/bring-current`, { choices });
}

export type CommitDto = {
  commit: string;
  /** A deployed change's title, which is what the merge carried onto `main`. */
  subject: string;
  body: string;
  /** Unix seconds. */
  time: number;
  files: string[];
  /** Who wrote it, as they were called then. `smeltr-leader` for a commit no
   * person asked for, and for every commit written before smeltr recorded an
   * author at all. */
  author: string;
  /** The account behind that name, where the leader wrote the commit itself. */
  authorId?: string;
  /** Who deployed it. Present on a deploy commit and nowhere else; equal to
   * the author when somebody deployed their own change. */
  deployer?: string;
  deployerId?: string;
};

/** A rollback holding the fleet behind `main`'s head. */
export interface HeldDto {
  /** `main`'s head, which the fleet does not run. */
  head: string;
  /** Commits on `main` the fleet does not run. */
  behind: number;
  /** Who held it, and when, unix seconds. */
  by: string;
  byId?: string;
  since: number;
  /** A rollback holds it, which Approve & deploy does not end. */
  rolledBack: boolean;
}

export interface HistoryResult {
  ok: boolean;
  errors: ApiError[];
  /** What the fleet is running. */
  deployed?: string | null;
  /** Present while a rollback holds the fleet behind `main`'s head. */
  held?: HeldDto;
  commits: CommitDto[];
}

/** `main`'s history, newest first, with what the fleet runs and any hold. */
export function history(limit?: number): Promise<HistoryResult> {
  return apiJson(`/api/history${limit ? `?limit=${limit}` : ''}`);
}

/** Deploy a commit already in `main`'s history. A rollback is this and
 * nothing else (W7); deploying `main`'s head releases a hold. One where a
 * group does not load is refused with `doesNotLoad` unless `confirm`. */
export function deployCommit(commit: string, confirm = false): Promise<BranchMutationResult> {
  return post('/api/deploy', { commit, confirm }, 'severe');
}

// -- accounts ---------------------------------------------------------------
//
// The roster and what an administrator does to it (`users_api.rs`). Every
// mutation but a rename re-asks for the acting administrator's own password:
// a session alone is not enough to create an administrator or end somebody's
// access, which is what stands between a walked-away-from browser and either.

/** An account as the roster shows it. No verifier, and no password state
 * beyond whether one must be changed. */
export interface UserDto {
  id: string;
  username?: string;
  displayName: string;
  enabled: boolean;
  administrator: boolean;
  /** How the account signs in: `local` (a password) or `oidc` (the provider). */
  authSource: 'local' | 'oidc';
  /** What the provider last called an OIDC account: its email, else its preferred username. */
  providerLogin?: string;
  createdAt: number;
  updatedAt: number;
  /** The roles this account holds: its provider groups a role exists for. Empty for a local account. */
  roles: string[];
  /** Every group the provider named at the last sign-in or refresh, exactly as named. Empty for a local account. */
  groups: string[];
  access: AccessDto;
}

/** A generated password, shown once and never retrievable. */
export interface TemporaryPasswordDto {
  userId: string;
  username: string;
  temporaryPassword: string;
}

/** What a refused account call answers, so a form can say why rather than
 * that something went wrong. */
export type AccountRefusal = { status: number; error: string };

/** The roster, which is a bare JSON array rather than an envelope with `ok`.
 *
 * `apiJson` hands back whatever parsed, which for a `503` is the error object:
 * it would be assigned as though it were the roster and then fail to iterate,
 * somewhere far from the call. A shape that is not an array is an error here,
 * where it can still say so. */
export async function listUsers(): Promise<UserDto[]> {
  const body = await apiJson<UserDto[] | { error?: string }>('/api/users');
  if (Array.isArray(body)) return body;
  throw new Error(body.error || 'the roster could not be read');
}

/** `POST`, `PATCH` and the two actions all answer a body with `error` on a
 * refusal, which the form shows as it is. */
async function accountCall<T>(
  url: string,
  init: RequestInit,
  weight: Weight = 'bar',
): Promise<T | AccountRefusal> {
  const headers = new Headers(init.headers);
  const token = csrf();
  if (token !== null) headers.set('x-smeltr-csrf', token);
  headers.set('content-type', 'application/json');

  const call = errors.call(init.method ?? 'GET', url, weight);
  let res: Response;
  try {
    res = await fetch(url, { ...init, headers });
  } catch (e) {
    throw failed(call, e);
  }
  if (res.status === 401) {
    signedOut();
    return { status: 401, error: 'your session ended -- sign in again' };
  }
  if (res.status === 403) void recheck();
  const body = (await res.json().catch(() => ({}))) as { error?: string };
  if (!res.ok) {
    const error = body.error || 'that did not work';
    errors.report(call, [bareError(error)]);
    return { status: res.status, error };
  }
  // A setting the leader read and would not take answers as a refusal of its own.
  answered(call, body);
  return body as T;
}

export function createUser(
  username: string,
  displayName: string,
): Promise<TemporaryPasswordDto | AccountRefusal> {
  return accountCall('/api/users', {
    method: 'POST',
    body: JSON.stringify({ username, displayName }),
  });
}

export function renameUser(id: string, displayName: string): Promise<UserDto | AccountRefusal> {
  return accountCall(`/api/users/${encodeURIComponent(id)}`, {
    method: 'PATCH',
    body: JSON.stringify({ displayName }),
  });
}

export function setUserEnabled(id: string, enabled: boolean): Promise<UserDto | AccountRefusal> {
  return accountCall(`/api/users/${encodeURIComponent(id)}`, {
    method: 'PATCH',
    body: JSON.stringify({ enabled }),
  });
}

export function resetUserPassword(id: string): Promise<TemporaryPasswordDto | AccountRefusal> {
  return accountCall(`/api/users/${encodeURIComponent(id)}/reset-password`, {
    method: 'POST',
  });
}

/** `ended` is the wire field: how many sessions this ended. */
export function revokeUserSessions(id: string): Promise<{ ended: number } | AccountRefusal> {
  return accountCall(`/api/users/${encodeURIComponent(id)}/revoke-sessions`, {
    method: 'POST',
  });
}

// -- roles --------------------------------------------------------------------
//
// A role is one provider group, by the name the provider gives it, and what holding it gives
// (`roles_api.rs`). Administrators only, like the roster.

export interface RoleDto {
  name: string;
  administrator: boolean;
  /** Empty for an administrator role, which holds every area. */
  areas: Record<string, AccessLevel>;
}

/** Every role, sorted by name. */
export function listRoles(): Promise<RoleDto[]> {
  return apiJson('/api/roles');
}

/** Creates the role or replaces what it gives. */
export function putRole(
  name: string,
  administrator: boolean,
  areas: Record<string, AccessLevel>,
): Promise<{ ok: boolean; error?: string }> {
  return apiJson(
    `/api/roles/${encodeURIComponent(name)}`,
    {
      method: 'PUT',
      body: JSON.stringify({ administrator, areas }),
    },
    'quiet',
  );
}

export function deleteRole(name: string): Promise<{ ok: boolean; error?: string }> {
  return apiJson(`/api/roles/${encodeURIComponent(name)}`, { method: 'DELETE' }, 'quiet');
}

// -- agents -------------------------------------------------------------------
//
// The leader's agent, acting for a person within their agent permissions (`agents_api.rs`).

/** What a person lets their own agent use: everything they hold, or up to `default` in each area and the level
 * `areas` names in the ones it names. The agent holds the lower of this and what the person holds. */
export type AgentPermissionsDto =
  | { kind: 'everything' }
  | { kind: 'upto'; default: AccessLevel; areas: Record<string, AccessLevel> };

export function getAgentPermissions(): Promise<AgentPermissionsDto> {
  return apiJson('/api/me/agent-permissions');
}

export function putAgentPermissions(
  permissions: AgentPermissionsDto,
): Promise<AgentPermissionsDto | AccountRefusal> {
  return accountCall('/api/me/agent-permissions', {
    method: 'PUT',
    body: JSON.stringify(permissions),
  });
}

// -- what belongs to the leader rather than to a person ----------------------

/** The General tab of Admin -> System: this leader's own build. */
export interface SystemInfoDto {
  smeltrVersion: string;
  /** The release this build is, `1.2.0`; absent on any other build. */
  release?: string;
  /** The newest published release, when it is newer than this one. */
  newerRelease?: { release: string; url: string };
  /** The last look for a newer release: when, and why it failed if it did. Absent until the first, and on a build
   * that is no release. */
  releaseCheck?: { checkedAt: number; failure?: string };
  /** The SASL mechanisms this leader's build can select. A form offering
   * one that is not here would be offering a save the leader refuses. */
  saslMechanisms?: string[];
}

export function getSystemInfo(): Promise<SystemInfoDto> {
  return apiJson('/api/system/info');
}

/** What a source's or destination's form picks from: the secret catalog, so a credential can be pointed at one
 * the leader holds, and the SASL mechanisms this leader's build can select. A failure offers none rather than
 * failing the form: a variable can still be named, and no mechanism is offered that the save would refuse. */
export async function fieldCatalogs(): Promise<{ secrets: SecretDto[]; mechanisms: string[] }> {
  const [secrets, mechanisms] = await Promise.all([
    listSecrets().catch((): SecretDto[] => []),
    getSystemInfo()
      .then((info) => info.saslMechanisms ?? [])
      .catch((): string[] => []),
  ]);
  return { secrets, mechanisms };
}

/** The Sessions tab of Admin -> System. Seconds on the wire; the form
 * converts to the minutes and hours a person thinks in. The bounds come from
 * the leader so the form does not restate them and drift. */
export interface SessionSettingsDto {
  idleSeconds: number;
  absoluteSeconds: number;
  minSeconds: number;
  maxSeconds: number;
}

export function getSessionSettings(): Promise<SessionSettingsDto> {
  return apiJson('/api/settings/sessions');
}

export function putSessionSettings(
  idleSeconds: number,
  absoluteSeconds: number,
): Promise<SessionSettingsDto | AccountRefusal> {
  return accountCall('/api/settings/sessions', {
    method: 'PUT',
    body: JSON.stringify({ idleSeconds, absoluteSeconds }),
  });
}

/** The Single sign-on tab of Admin -> Accounts (`oidc_api.rs`). The client
 * secret is never sent back, only whether one is stored. */
export interface OidcSettingsDto {
  enabled: boolean;
  issuer: string;
  clientId: string;
  clientSecretSet: boolean;
  groupsClaim: string;
  /** What the administrator registers at the provider as the sign-in redirect. */
  redirectUri: string;
}

export type OidcSettingsResult = {
  ok: boolean;
  errors: ApiError[];
  settings: OidcSettingsDto | null;
};

export function getOidcSettings(): Promise<OidcSettingsResult> {
  return apiJson('/api/settings/oidc');
}

/** `clientSecret` undefined keeps the stored one, which the leader forgets on
 * its own when the issuer or client id changes. */
export function putOidcSettings(req: {
  enabled: boolean;
  issuer: string;
  clientId: string;
  clientSecret?: string;
  groupsClaim: string;
}): Promise<OidcSettingsResult | AccountRefusal> {
  return accountCall('/api/settings/oidc', { method: 'PUT', body: JSON.stringify(req) });
}

/** Which API the model endpoint speaks: Anthropic's Messages API, OpenAI's, or another service that speaks
 * OpenAI's. */
export type ModelProvider = 'anthropic' | 'openai' | 'openai-compatible';

/** The AI agents tab of Admin -> System (`agent_settings_api.rs`). The API key is never sent back, only whether
 * one is stored. */
export interface AgentSettingsDto {
  enabled: boolean;
  provider: ModelProvider;
  endpoint: string;
  model: string;
  apiKeySet: boolean;
  /** How many days a conversation nobody adds to is kept. */
  keepDays: number;
  /** The tokens the model's context holds. */
  contextTokens: number;
}

export type AgentSettingsResult = {
  ok: boolean;
  errors: ApiError[];
  settings: AgentSettingsDto | null;
};

export function getAgentSettings(): Promise<AgentSettingsResult> {
  return apiJson('/api/settings/agents');
}

/** `apiKey` undefined keeps the stored one, which the leader forgets on its own when the provider or endpoint
 * changes. */
export function putAgentSettings(req: {
  enabled: boolean;
  provider: ModelProvider;
  endpoint: string;
  model: string;
  apiKey?: string;
  keepDays?: number;
  contextTokens?: number;
}): Promise<AgentSettingsResult | AccountRefusal> {
  return accountCall('/api/settings/agents', { method: 'PUT', body: JSON.stringify(req) });
}

/** What one certificate says about itself. Epoch milliseconds, like every
 * other time the leader sends. */
export interface DescribedCertificate {
  subject: string;
  fingerprint: string;
  names: string[];
  notBeforeMillis: number;
  notAfterMillis: number;
}

/** An authority being moved to. Workers are given it beside the one this leader
 * signs under today; nothing is signed under it until the leader cuts over. */
/** Where a served certificate comes from: this leader's own authority, an upload, or an order from a
 * certificate authority over ACME. */
export type CertificateSource = 'own' | 'upload' | 'acme';

export interface RotationDto {
  incoming: DescribedCertificate;
  startedMillis: number;
  /** When this leader signs under `incoming` on its own, whether or not the
   * fleet has taken it by then. */
  deadlineMillis: number;
  /** The latest `deadlineMillis` can move to. */
  latestDeadlineMillis: number;
  /** What the cutover puts in force: another authority of the leader's own,
   * an uploaded certificate, or the leader's own again. */
  installs: 'nextKey' | 'supplied' | 'own';
  /** Where that comes from. */
  source: CertificateSource;
  /** Workers that verify this leader by an authority their operator gave
   * them, and so take none from it. The cutover does not wait for them. */
  notFollowing: string[];
  /** Each worker the cutover waits for, and whether it holds `incoming` yet. */
  workers: { id: string; holdsIncoming: boolean }[];
}

export interface CertificatesDto {
  /** Whether this leader terminates TLS itself. False behind a proxy, where the
   * certificate a browser checked belongs to the proxy, not to this process. */
  servingTls: boolean;
  /** What a worker must be given to verify this leader. `null` with no TLS here. */
  authority: DescribedCertificate | null;
  /** What a browser and a worker are shown. `null` with no TLS here. */
  served: DescribedCertificate | null;
  /** Whether an operator supplied what is being served. */
  supplied: boolean;
  source: CertificateSource;
  /** The name of the list entry an upload was copied from, where one is recorded. */
  entry: string | null;
  /** The names this leader answers to. A supplied certificate covers at least one. */
  names: string[];
  /** The name the worker inside this leader reaches it by, where one runs. A supplied certificate covers it. */
  workerName: string | null;
  /** Set when a certificate reached the leader's disk and its socket refused it.
   * Everything above is then what the disk holds rather than what a client is
   * shown, and only a restart closes the gap. */
  restartRequired: boolean;
  /** Set while this leader is rotating to a new authority. */
  rotation: RotationDto | null;
}

export function getCertificates(): Promise<CertificatesDto> {
  return apiJson('/api/settings/certificates');
}

/** The authority's own download URL. A plain link rather than a fetch: the
 * browser names the file and puts it where downloads go. Ungated, because a
 * worker holding an enrollment code fetches the same bytes with no session. */
export const AUTHORITY_URL = '/api/authority';

/** Serves a copy of the certificate `name` from the list. */
export function putCertificate(name: string): Promise<CertificatesDto | AccountRefusal> {
  return accountCall('/api/settings/certificates', {
    method: 'PUT',
    body: JSON.stringify({ certificate: name }),
  });
}

export function withdrawCertificate(): Promise<CertificatesDto | AccountRefusal> {
  return accountCall('/api/settings/certificates/supplied', {
    method: 'DELETE',
  });
}

export function reissueCertificate(): Promise<CertificatesDto | AccountRefusal> {
  return accountCall('/api/settings/certificates/reissue', {
    method: 'POST',
  });
}

/** How long a rotation may wait for the fleet, mirroring
 * `certificates_api::MOST_WAIT_HOURS`. The leader is the authority; this
 * keeps the input from offering an hour count it would refuse anyway. */
export const ROTATION_WAIT_HOURS = { min: 1, max: 14 * 24 };

/** Supported lifetimes for a newly minted authority. */
export const AUTHORITY_VALIDITY_DAYS = [365, 3 * 365, 5 * 365, 10 * 365, 20 * 365] as const;

/** Mints a new authority with the selected lifetime and publishes it beside the
 * one this leader signs under today. Nothing served changes yet. */
export function beginRotation(
  waitHours: number,
  authorityValidityDays: number,
): Promise<CertificatesDto | AccountRefusal> {
  return accountCall('/api/settings/certificates/rotation', {
    method: 'POST',
    body: JSON.stringify({ waitHours, authorityValidityDays }),
  });
}

/** Signs under the incoming authority now. A worker that has not yet taken it
 * stops being able to verify this leader. */
export function cutOverRotation(): Promise<CertificatesDto | AccountRefusal> {
  return accountCall('/api/settings/certificates/rotation/cutover', {
    method: 'POST',
  });
}

/** Moves when the open rotation cuts over without the fleet. */
export function setRotationDeadline(
  deadlineMillis: number,
): Promise<CertificatesDto | AccountRefusal> {
  return accountCall('/api/settings/certificates/rotation/deadline', {
    method: 'PUT',
    body: JSON.stringify({ deadlineMillis }),
  });
}

/** Drops the incoming authority. This leader keeps serving what it serves today. */
export function abandonRotation(): Promise<CertificatesDto | AccountRefusal> {
  return accountCall('/api/settings/certificates/rotation', {
    method: 'DELETE',
  });
}

/** The ACME settings, and what the last order left. */
export interface AcmeDto {
  enabled: boolean;
  directory: string;
  contact: string;
  names: string[];
  challenge: AcmeChallenge;
  authority: string;
  eabKeyId: string;
  /** Whether an external account HMAC key is stored. The key itself is never sent back. */
  eabHmacSet: boolean;
  dnsProvider: AcmeDnsProvider;
  /** Whether a Cloudflare API token is stored. The token itself is never sent back. */
  cloudflareTokenSet: boolean;
  /** Where this leader answers an authority's HTTP-01 challenge. */
  httpPort: number;
  /** The certificate the last order installed, by fingerprint, and its dates. `null` before any. */
  issued: string | null;
  notAfterMillis: number | null;
  renewAfterMillis: number | null;
  lastAttemptMillis: number | null;
  lastError: string | null;
}

export type AcmeChallenge = 'tls-alpn-01' | 'http-01' | 'dns-01';

/** Who publishes a DNS-01 record. */
export type AcmeDnsProvider = 'cloudflare';

export interface AcmeSettings {
  enabled: boolean;
  directory: string;
  contact: string;
  names: string[];
  challenge: AcmeChallenge;
  authority: string;
  eabKeyId: string;
  /** `null` keeps the stored key; empty removes it. */
  eabHmac: string | null;
  dnsProvider: AcmeDnsProvider;
  /** `null` keeps the stored token; empty removes it. */
  cloudflareToken: string | null;
}

/** The directories the page offers by name. Any other is typed as a URL. */
export const ACME_DIRECTORIES = [
  {
    value: 'https://acme-v02.api.letsencrypt.org/directory',
    label: "Let's Encrypt",
    terms: 'https://letsencrypt.org/repository/',
  },
] as const;

export function getAcme(): Promise<AcmeDto> {
  return apiJson('/api/settings/acme');
}

export function putAcme(settings: AcmeSettings): Promise<AcmeDto | AccountRefusal> {
  return accountCall('/api/settings/acme', {
    method: 'PUT',
    body: JSON.stringify(settings),
  });
}

/** Orders a certificate from the authority the settings name, and serves it: at once under the root in force,
 * else as the switch an upload under another root begins. */
export function orderAcme(): Promise<CertificatesDto | AccountRefusal> {
  return accountCall('/api/settings/acme/order', {
    method: 'POST',
  });
}

/** Narrows a call's answer to the refusal side. */
export function refused<T extends object>(result: T | AccountRefusal): result is AccountRefusal {
  return 'status' in result && 'error' in result;
}

/** One of the caller's conversations with their agent (`chat_api.rs`). */
export interface ConversationSummaryDto {
  id: string;
  title: string;
  createdAt: number;
  updatedAt: number;
  running: boolean;
}

/** What was said, asked and answered in a conversation, in order. */
export type ConversationItem =
  | { kind: 'message'; text: string }
  | { kind: 'answer'; text: string }
  | {
      kind: 'call';
      id: string;
      input: { method?: string; path?: string; query?: Record<string, string>; body?: unknown };
    }
  | { kind: 'result'; callId: string; output: string; isError: boolean }
  | { kind: 'unreadable'; error: string }
  /** Where the agent's reading begins again: from this summary, or from nothing where it was cleared. */
  | { kind: 'fresh'; summary: string | null };

export interface ConversationDto extends ConversationSummaryDto {
  /** How the last turn ended: `answered`, `stopped` or `rounds`. */
  ended: 'answered' | 'stopped' | 'rounds' | null;
  error: string | null;
  items: ConversationItem[];
  /** What its last answer was made with, so the next message starts from them. */
  model?: string;
  effort?: Effort;
  /** How much of its model's context the last request filled, in tokens. */
  context?: { used: number; window: number };
  /** While a turn runs, the answer its model is writing, so far. */
  writing?: string;
}

/** How hard the model works at an answer. Unset, the provider's default. */
export type Effort = 'low' | 'medium' | 'high' | 'xhigh' | 'max';

/** A model the stored key reaches. Only Anthropic says how much it holds and which efforts it takes. */
export interface AgentModelDto {
  id: string;
  name: string;
  contextTokens?: number;
  efforts?: Effort[];
}

/** The models the agent may answer with, and the one the administrator set, which a new conversation starts
 * from. */
export function listAgentModels(): Promise<
  { models: AgentModelDto[]; default: string } | AccountRefusal
> {
  return accountCall('/api/agent/models', { method: 'GET' });
}

/** What a message is answered with: the model and effort picked for it, or the conversation's last. */
export interface Asked {
  model?: string;
  effort?: Effort;
}

/** Where one conversation, or what is under it, is read and changed. */
function conversationPath(id: string, under = ''): string {
  return `/api/agent/conversations/${encodeURIComponent(id)}${under}`;
}

export function listConversations(): Promise<
  { conversations: ConversationSummaryDto[] } | AccountRefusal
> {
  return accountCall('/api/agent/conversations', { method: 'GET' });
}

export function getConversation(
  id: string,
): Promise<{ conversation: ConversationDto } | AccountRefusal> {
  return accountCall(conversationPath(id), { method: 'GET' });
}

export function startConversation(
  message: string,
  asked: Asked = {},
): Promise<{ id: string } | AccountRefusal> {
  return accountCall('/api/agent/conversations', {
    method: 'POST',
    body: JSON.stringify({ message, ...asked }),
  });
}

export function sendToConversation(
  id: string,
  message: string,
  asked: Asked = {},
): Promise<object | AccountRefusal> {
  return accountCall(conversationPath(id, '/messages'), {
    method: 'POST',
    body: JSON.stringify({ message, ...asked }),
  });
}

export function stopConversation(id: string): Promise<object | AccountRefusal> {
  return accountCall(conversationPath(id, '/stop'), { method: 'POST' });
}

/** Begin the agent's reading of the conversation again: from a summary of it the model writes, or from nothing.
 * A summary is written in the background, like a turn. */
export function restartConversation(
  id: string,
  summarize: boolean,
): Promise<object | AccountRefusal> {
  return accountCall(conversationPath(id, '/context'), {
    method: 'POST',
    body: JSON.stringify({ summarize }),
  });
}

export function deleteConversation(id: string): Promise<object | AccountRefusal> {
  return accountCall(conversationPath(id), { method: 'DELETE' });
}
