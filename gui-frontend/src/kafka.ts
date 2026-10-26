// What a Kafka client's security and read position are, in the terms the
// form asks them in and the two property maps the bundle carries them in.
//
// The Rust side is the authority. `config_file::load` refuses a property
// smeltr owns, a credential-valued property in the committed map, and a
// property that answers what an identity already answers; `io::kafka`
// refuses a mechanism the build cannot select. Everything here says the same thing earlier, in the
// field that caused it.

import type { KafkaProps, SecretRef } from './api';
import { onLoopback, UNENCRYPTED } from './http';

// -- the read position ------------------------------------------------------

/** Where a source starts. Mirrors `config_file::dto::StartPositionDto`;
 * `earliest` is what a source that says nothing gets. */
export type StartPosition =
  { type: 'earliest' } | { type: 'latest' } | { type: 'timestamp'; millis: number };

/** Where a source stops, exclusive. Absent is a stream that keeps reading. */
export type StopPosition = { type: 'timestamp'; millis: number };

// -- security ---------------------------------------------------------------

/** How a client reaches its brokers. Mirrors librdkafka's own names, which
 * is what the property takes. */
export const PROTOCOLS = ['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'] as const;
export type Protocol = (typeof PROTOCOLS)[number];

/** The protocols that negotiate TLS. Under the other two nothing an identity
 * holds is used. Mirrors `config_file::load::TLS_PROTOCOLS`. */
export const TLS_PROTOCOLS: readonly Protocol[] = ['SSL', 'SASL_SSL'];

/** What to say when the client's credentials would reach brokers off this host without TLS, or nothing. */
export function brokersWarning(brokers: string, props: KafkaProps): string | undefined {
  if (Object.keys(props.credentials ?? {}).length === 0) return undefined;
  const { protocol } = readSecurity(props);
  if (protocol !== undefined && TLS_PROTOCOLS.includes(protocol)) return undefined;
  const hosts = brokers
    .split(',')
    .map((b) => b.trim())
    .filter((b) => b !== '');
  return hosts.length > 0 && !hosts.every(onLoopback) ? UNENCRYPTED : undefined;
}

/** The protocols that authenticate with SASL, and so ask for a mechanism. */
export const SASL_PROTOCOLS: readonly Protocol[] = ['SASL_PLAINTEXT', 'SASL_SSL'];

/** One thing a mechanism asks for. `secret` decides which map it is written
 * to: a committed value, or a name saying where the value lives. */
export type MechanismField = {
  prop: string;
  label: string;
  secret: boolean;
  note: string;
  optional?: boolean;
};

const USERNAME: MechanismField = {
  prop: 'sasl.username',
  label: 'username',
  secret: false,
  note: 'Committed to the config repository.',
};
const PASSWORD: MechanismField = {
  prop: 'sasl.password',
  label: 'password',
  secret: true,
  note: 'A secret, by name. The value never reaches git.',
};

/** What each mechanism asks for, in the order it is asked. A mechanism not
 * listed here is one this form does not write; the properties table still
 * takes it by hand. */
export const MECHANISM_FIELDS: Record<string, MechanismField[]> = {
  PLAIN: [USERNAME, PASSWORD],
  'SCRAM-SHA-256': [USERNAME, PASSWORD],
  'SCRAM-SHA-512': [USERNAME, PASSWORD],
  OAUTHBEARER: [
    {
      prop: 'sasl.oauthbearer.token.endpoint.url',
      label: 'token endpoint',
      secret: false,
      note: 'Where a token is fetched from.',
    },
    {
      prop: 'sasl.oauthbearer.client.id',
      label: 'client id',
      secret: false,
      note: 'Committed to the config repository.',
    },
    {
      prop: 'sasl.oauthbearer.client.secret',
      label: 'client secret',
      secret: true,
      note: 'A secret, by name.',
    },
    {
      prop: 'sasl.oauthbearer.scope',
      label: 'scope',
      secret: false,
      note: 'Leave it empty and the authorization server decides whether it wants one.',
      optional: true,
    },
  ],
  GSSAPI: [
    {
      prop: 'sasl.kerberos.service.name',
      label: 'service name',
      secret: false,
      note: 'The broker’s own principal name.',
    },
    {
      prop: 'sasl.kerberos.principal',
      label: 'principal',
      secret: false,
      note: 'Who this client authenticates as.',
    },
    {
      prop: 'sasl.kerberos.keytab',
      label: 'keytab',
      secret: false,
      note: 'A path on every worker’s own host. Linux only; a Windows worker uses its own ticket cache.',
      optional: true,
    },
  ],
};

/** The OAUTHBEARER flow this form writes. librdkafka's other one asks the
 * application for a token, and nothing here is an application that could. */
const OAUTH_METHOD = { prop: 'sasl.oauthbearer.method', value: 'oidc' };

/** Every property the section writes, whatever is selected. The properties
 * table hides these: one place each can come from. */
export const SECTION_PROPS: readonly string[] = [
  'security.protocol',
  'sasl.mechanism',
  'sasl.mechanisms',
  OAUTH_METHOD.prop,
  ...Object.values(MECHANISM_FIELDS).flatMap((fields) => fields.map((f) => f.prop)),
];

/** What the section is showing: the two choices, and nothing else. The
 * values themselves stay in the props maps, which are what is saved. */
export type Security = {
  protocol: Protocol | undefined;
  mechanism: string | undefined;
};

/** What the maps already say. librdkafka's default is plaintext, so a client
 * naming no protocol is one. */
export function readSecurity(props: KafkaProps): Security {
  const p = props.properties ?? {};
  const protocol = PROTOCOLS.find((k) => k === p['security.protocol']?.toUpperCase());
  const mechanism = p['sasl.mechanism'] ?? p['sasl.mechanisms'];
  return { protocol, mechanism };
}

/** Writes the choice into the two maps, and takes out what the choice no
 * longer asks for: a client switched from SASL to plaintext that kept its
 * password would be carrying a credential nothing reads. */
export function writeSecurity(props: KafkaProps, next: Security): void {
  const literal: Record<string, string> = { ...(props.properties ?? {}) };
  const credentials: Record<string, SecretRef> = { ...(props.credentials ?? {}) };

  const keep = new Set(propsFor(next));
  for (const key of SECTION_PROPS) {
    if (!keep.has(key)) {
      delete literal[key];
      delete credentials[key];
    }
  }

  if (next.protocol === undefined) delete literal['security.protocol'];
  else literal['security.protocol'] = next.protocol;

  // One spelling. librdkafka takes both, and two names for one thing is one
  // name too many.
  delete literal['sasl.mechanisms'];
  if (asksForMechanism(next.protocol) && next.mechanism !== undefined) {
    literal['sasl.mechanism'] = next.mechanism;
    if (next.mechanism === 'OAUTHBEARER') literal[OAUTH_METHOD.prop] = OAUTH_METHOD.value;
  } else {
    delete literal['sasl.mechanism'];
  }

  assign(props, literal, credentials);
}

/** Whether a mechanism is a question this protocol asks at all. */
export function asksForMechanism(protocol: Protocol | undefined): boolean {
  return protocol !== undefined && SASL_PROTOCOLS.includes(protocol);
}

/** The fields the selection asks for, which is what the section draws. */
export function fieldsFor(security: Security): MechanismField[] {
  if (!asksForMechanism(security.protocol) || security.mechanism === undefined) return [];
  return MECHANISM_FIELDS[security.mechanism] ?? [];
}

/** Every property this selection is entitled to hold. The condition on the
 * OIDC method is the one `writeSecurity` writes it under, so what one writes
 * the other keeps: a protocol that stopped authenticating would otherwise
 * leave the method behind, in neither table and reachable from nowhere. */
function propsFor(security: Security): string[] {
  const own = fieldsFor(security).map((f) => f.prop);
  if (asksForMechanism(security.protocol) && security.mechanism === 'OAUTHBEARER') {
    own.push(OAUTH_METHOD.prop);
  }
  return ['security.protocol', 'sasl.mechanism', ...own];
}

/** Sets one of a mechanism's committed fields. An empty value is the field
 * left unset, not a property set to nothing. */
export function setField(props: KafkaProps, prop: string, value: string): void {
  const literal: Record<string, string> = { ...(props.properties ?? {}) };
  if (value.trim() === '') delete literal[prop];
  else literal[prop] = value;
  assign(props, literal, { ...(props.credentials ?? {}) });
}

/** Points one of a mechanism's credential fields at a secret, or at none. */
export function setCredential(props: KafkaProps, prop: string, ref: SecretRef | undefined): void {
  const credentials: Record<string, SecretRef> = { ...(props.credentials ?? {}) };
  if (ref === undefined) delete credentials[prop];
  else credentials[prop] = ref;
  assign(props, { ...(props.properties ?? {}) }, credentials);
}

/** An empty map is omitted, as the DTO serializes it. */
function assign(
  props: KafkaProps,
  literal: Record<string, string>,
  credentials: Record<string, SecretRef>,
): void {
  if (Object.keys(literal).length > 0) props.properties = literal;
  else delete props.properties;
  if (Object.keys(credentials).length > 0) props.credentials = credentials;
  else delete props.credentials;
}

// -- what a save refuses ----------------------------------------------------

/** Properties smeltr sets from a field of its own. Mirrors
 * `config_file::load::RESERVED_KAFKA_PROPS`, shortened to the reason a form
 * can act on. */
const RESERVED: Record<string, string> = {
  'bootstrap.servers': 'set from this connector’s own brokers field',
  'metadata.broker.list': 'librdkafka’s other spelling of bootstrap.servers',
  'group.id': 'set from this source’s own group id',
  'client.rack': 'each worker sets it from its zone setting',
  'group.instance.id': 'each worker sets it from its own id',
  'auto.offset.reset': 'set from this source’s own start position',
  'enable.auto.commit':
    'smeltr commits once a batch’s destinations have flushed; auto-commit would acknowledge rows that were never delivered',
  'auto.commit.enable': 'a deprecated spelling the high-level consumer ignores',
  'max.poll.interval.ms':
    'chosen with the destination stall threshold, so a failing destination cannot rebalance the group',
  'compression.type': 'set from this destination’s own compression field',
  'compression.codec': 'librdkafka’s other spelling of compression.type',
  acks: 'set from this destination’s own acks field',
  'request.required.acks': 'librdkafka’s other spelling of acks',
  'max.in.flight.requests.per.connection':
    'set from this destination’s own producer buffer and its link',
  'max.in.flight': 'librdkafka’s other spelling of max.in.flight.requests.per.connection',
  'queued.max.messages.kbytes': 'set from this source’s own fetch buffer',
  'queue.buffering.max.kbytes': 'set from this destination’s own producer buffer',
  'queue.buffering.max.messages': 'set to its maximum so the producer buffer bounds the queue',
  'statistics.interval.ms': 'smeltr reports librdkafka’s statistics as metrics of its own',
  'enable.idempotence':
    'smeltr runs the producer with it off: an idempotent producer keeps too few requests in flight for a far link',
  'transactional.id':
    'smeltr never begins a transaction, and a transactional producer refuses every record sent outside one',
  'partition.assignment.strategy':
    'smeltr runs cooperative rebalancing, and cooperative and eager members must not share a group',
  'sasl.kerberos.kinit.cmd':
    'librdkafka runs it through system() on every worker, and a bundle carries no shell command',
};

/** Why this property cannot be set here, or nothing. */
export function reservedReason(key: string): string | undefined {
  return RESERVED[key.trim().toLowerCase()];
}

/** Properties that answer what an identity answers: what this client
 * presents, and what it trusts. Mirrors `config_file::load::IDENTITY_KAFKA_PROPS`. */
const IDENTITY_PROPS = new Set([
  'ssl.certificate.pem',
  'ssl.certificate.location',
  'ssl.key.pem',
  'ssl.key.location',
  'ssl.ca.pem',
  'ssl.ca.location',
  'ssl.keystore.location',
  'ssl.keystore.password',
  'ssl.key.password',
  'enable.ssl.certificate.verification',
  'ssl.endpoint.identification.algorithm',
]);

/** Why this property cannot be set while an identity is named, or nothing. */
export function identityReason(key: string, hasIdentity: boolean): string | undefined {
  if (!hasIdentity || !IDENTITY_PROPS.has(key.trim().toLowerCase())) return undefined;
  return 'the identity above already answers this, and librdkafka would take whichever was set last';
}

/** Why a row of the properties editor cannot be saved, or nothing: the refusals `config_file::load` makes of a
 * property, in the row that caused each. `credential` is the row's table. */
export function propertyRefusal(
  key: string,
  credential: boolean,
  hasIdentity: boolean,
): string | undefined {
  const trimmed = key.trim();
  if (trimmed === '') return undefined;
  if (SECTION_PROPS.includes(trimmed.toLowerCase())) return 'the security section above sets this';
  const reserved = reservedReason(trimmed);
  if (reserved !== undefined) return `smeltr owns this: ${reserved}`;
  const identity = identityReason(trimmed, hasIdentity);
  if (identity !== undefined) return identity;
  if (!credential && isCredentialValued(trimmed))
    return 'its value is a credential, and these are committed to the config repository. Name it below instead';
  return undefined;
}

/** Whether a property's value is a credential, and so may only be named
 * through the credentials map. Classified by shape rather than listed, as
 * `config_file::load::is_credential_valued` is: librdkafka gains properties
 * with each release, and an unrecognized key saying password must still be
 * refused. */
export function isCredentialValued(key: string): boolean {
  const lower = key.trim().toLowerCase();
  if (['password', 'passphrase', 'secret'].some((w) => lower.includes(w))) return true;
  return lower === 'ssl.key.pem' || lower === 'sasl.oauthbearer.assertion.private.key.pem';
}
