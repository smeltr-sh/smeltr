import { describe, expect, it } from 'vitest';
import {
  brokersWarning,
  fieldsFor,
  identityReason,
  isCredentialValued,
  MECHANISM_FIELDS,
  propertyRefusal,
  PROTOCOLS,
  readSecurity,
  reservedReason,
  SECTION_PROPS,
  setCredential,
  setField,
  writeSecurity,
  type Security,
} from './kafka';
import type { KafkaProps } from './api';

describe('the security section', () => {
  it('reads what the maps already say, and nothing they do not', () => {
    expect(readSecurity({})).toEqual({ protocol: undefined, mechanism: undefined });
    expect(
      readSecurity({ properties: { 'security.protocol': 'SASL_SSL', 'sasl.mechanism': 'PLAIN' } }),
    ).toEqual({ protocol: 'SASL_SSL', mechanism: 'PLAIN' });
    // librdkafka's other spelling is still a mechanism.
    expect(readSecurity({ properties: { 'sasl.mechanisms': 'GSSAPI' } }).mechanism).toBe('GSSAPI');
  });

  it('asks for a mechanism only under a SASL protocol', () => {
    expect(fieldsFor({ protocol: 'SSL', mechanism: 'PLAIN' })).toEqual([]);
    expect(
      fieldsFor({ protocol: 'SASL_SSL', mechanism: 'SCRAM-SHA-512' }).map((f) => f.prop),
    ).toEqual(['sasl.username', 'sasl.password']);
  });

  it('writes the choice and picks one spelling of the mechanism', () => {
    const props: KafkaProps = { properties: { 'sasl.mechanisms': 'PLAIN' } };
    writeSecurity(props, { protocol: 'SASL_SSL', mechanism: 'SCRAM-SHA-512' });
    expect(props.properties).toEqual({
      'security.protocol': 'SASL_SSL',
      'sasl.mechanism': 'SCRAM-SHA-512',
    });
  });

  it('names the OIDC flow, since nothing here can hand librdkafka a token', () => {
    const props: KafkaProps = {};
    writeSecurity(props, { protocol: 'SASL_SSL', mechanism: 'OAUTHBEARER' });
    expect(props.properties?.['sasl.oauthbearer.method']).toBe('oidc');
  });

  it('takes the OIDC method out with the mechanism that asked for it', () => {
    const props: KafkaProps = {};
    writeSecurity(props, { protocol: 'SASL_SSL', mechanism: 'OAUTHBEARER' });
    // The protocol stops authenticating; the mechanism is still named, and
    // the method it wrote must not outlive it in a map nothing shows.
    writeSecurity(props, { protocol: 'SSL', mechanism: 'OAUTHBEARER' });
    expect(props.properties).toEqual({ 'security.protocol': 'SSL' });
  });

  it('takes out what the new choice does not ask for', () => {
    const props: KafkaProps = {
      properties: {
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'u',
      },
      credentials: { 'sasl.password': { secret: 'p', version: 2 } },
    };
    writeSecurity(props, { protocol: 'SSL', mechanism: undefined });
    expect(props.properties).toEqual({ 'security.protocol': 'SSL' });
    // A credential nothing reads is a credential nobody rotates.
    expect(props.credentials).toBeUndefined();
  });

  it('leaves a property it does not own alone', () => {
    const props: KafkaProps = { properties: { 'socket.timeout.ms': '5000' } };
    writeSecurity(props, { protocol: 'PLAINTEXT', mechanism: undefined });
    expect(props.properties?.['socket.timeout.ms']).toBe('5000');
  });

  it('sets a field, and an emptied field is unset rather than blank', () => {
    const props: KafkaProps = {};
    setField(props, 'sasl.username', 'smeltr-west');
    expect(props.properties).toEqual({ 'sasl.username': 'smeltr-west' });
    setField(props, 'sasl.username', '  ');
    expect(props.properties).toBeUndefined();
  });

  it('points a credential at a secret by name, never at a value', () => {
    const props: KafkaProps = {};
    setCredential(props, 'sasl.password', { secret: 'kafka-west-sasl', version: 7 });
    expect(props.credentials).toEqual({
      'sasl.password': { secret: 'kafka-west-sasl', version: 7 },
    });
    setCredential(props, 'sasl.password', undefined);
    expect(props.credentials).toBeUndefined();
  });

  it('claims every key it writes, and a switch keeps only what the new choice asks for', () => {
    const choices: Security[] = [undefined, ...PROTOCOLS].flatMap((protocol) =>
      [undefined, ...Object.keys(MECHANISM_FIELDS)].map((mechanism) => ({ protocol, mechanism })),
    );
    // The choice written and every field it draws filled in: all it can ask for.
    const filled = (choice: Security): KafkaProps => {
      const props: KafkaProps = {};
      writeSecurity(props, choice);
      for (const f of fieldsFor(choice)) {
        if (f.secret) setCredential(props, f.prop, { secret: 's', version: 1 });
        else setField(props, f.prop, 'v');
      }
      return props;
    };
    const keys = (props: KafkaProps) => [
      ...Object.keys(props.properties ?? {}),
      ...Object.keys(props.credentials ?? {}),
    ];

    for (const from of choices) {
      // The properties table hides these keys, and a switch takes out only these.
      expect(SECTION_PROPS).toEqual(expect.arrayContaining(keys(filled(from))));
      for (const to of choices) {
        const props = filled(from);
        writeSecurity(props, to);
        // A credential left behind rides along in a map nothing shows.
        expect(keys(filled(to))).toEqual(expect.arrayContaining(keys(props)));
      }
    }
  });
});

describe('what a save refuses', () => {
  it('names the field a reserved property is set from', () => {
    expect(reservedReason('group.id')).toContain('group id');
    expect(reservedReason('GROUP.ID')).toContain('group id');
    expect(reservedReason('socket.timeout.ms')).toBeUndefined();
  });

  it('refuses a TLS property only while an identity answers for it', () => {
    expect(identityReason('ssl.ca.location', true)).toContain('identity');
    expect(identityReason('ssl.ca.location', false)).toBeUndefined();
    expect(identityReason('socket.timeout.ms', true)).toBeUndefined();
  });

  it('classifies a credential by shape, so a property it has never seen is still refused', () => {
    expect(isCredentialValued('sasl.password')).toBe(true);
    expect(isCredentialValued('ssl.key.password')).toBe(true);
    expect(isCredentialValued('some.future.passphrase')).toBe(true);
    expect(isCredentialValued('ssl.key.pem')).toBe(true);
    expect(isCredentialValued('ssl.ca.location')).toBe(false);
  });

  it('says one reason per row, the security section first', () => {
    expect(propertyRefusal('', false, false)).toBeUndefined();
    expect(propertyRefusal('sasl.password', true, false)).toContain('security section');
    expect(propertyRefusal('group.id', false, false)).toContain('smeltr owns this');
    expect(propertyRefusal('ssl.ca.location', false, true)).toContain('identity');
    expect(propertyRefusal('ssl.keystore.password', false, false)).toContain('Name it below');
    expect(propertyRefusal('ssl.keystore.password', true, false)).toBeUndefined();
    expect(propertyRefusal('socket.timeout.ms', false, false)).toBeUndefined();
  });
});

describe('brokers a credential reaches', () => {
  const props = (protocol?: string): KafkaProps => ({
    properties: protocol ? { 'security.protocol': protocol } : {},
    credentials: { 'sasl.password': 'KAFKA_PASSWORD' },
  });

  it('warns when a credential reaches brokers off this host without TLS', () => {
    expect(brokersWarning('b1:9092,b2:9092', props())).toMatch(/unencrypted/);
    expect(brokersWarning('localhost:9092, b2:9092', props('SASL_PLAINTEXT'))).toMatch(
      /unencrypted/,
    );
    expect(brokersWarning('b1:9092', props('sasl_ssl'))).toBeUndefined();
    expect(brokersWarning('localhost:9092,127.0.0.1:9093', props())).toBeUndefined();
    expect(brokersWarning('b1:9092', { properties: {}, credentials: {} })).toBeUndefined();
    expect(brokersWarning(' ', props())).toBeUndefined();
  });
});
