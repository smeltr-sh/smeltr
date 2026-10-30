import { describe, expect, it } from 'vitest';
import { render } from 'svelte/server';
import { createRawSnippet } from 'svelte';
import PropsEditor from './PropsEditor.svelte';
import { headerRefusal } from './http';
import KafkaPropsEditor from './KafkaPropsEditor.svelte';
import type { KafkaProps } from './api';

describe('the Kafka properties editor', () => {
  it('shows what was typed by hand, and leaves the security section its own', () => {
    const value: KafkaProps = {
      properties: { 'security.protocol': 'SSL', 'socket.timeout.ms': '60000' },
      credentials: { 'sasl.password': 'KAFKA_PASSWORD' },
    };
    const { body } = render(KafkaPropsEditor, { props: { value, reserved: new Map() } });
    expect(body).toContain('value="socket.timeout.ms"');
    expect(body).not.toContain('value="security.protocol"');
    expect(body).not.toContain('value="sasl.password"');
    expect(body).toContain('+ property');
    expect(body).toContain('aria-label="property name"');
  });

  it('says in the row why a save would refuse it', () => {
    const value: KafkaProps = { properties: { 'group.id': 'g' } };
    const reserved = new Map([['group.id', 'set from this source’s groupId']]);
    const { body } = render(KafkaPropsEditor, { props: { value, reserved } });
    expect(body).toContain('smeltr owns this');
  });
});

describe('the shared editor, on an HTTP destination’s headers', () => {
  const help = createRawSnippet(() => ({ render: () => '<span>help</span>' }));
  const value = {
    headers: { 'X-Source': 'smeltr', Host: 'collector.internal' },
    credentials: { 'X-Api-Key': 'API_KEY' },
  };
  const { body } = render(PropsEditor<'headers'>, {
    props: {
      value,
      literalKey: 'headers',
      labels: {
        literal: 'headers',
        credentials: 'credentials',
        noun: 'header',
        literalName: 'X-Source',
        literalValue: 'smeltr',
        credentialName: 'Authorization',
      },
      literalHelp: help,
      credentialsHelp: help,
      refusal: (key: string) => headerRefusal(key, value),
    },
  });

  it('takes its labels and refusals from the caller', () => {
    expect(body).toContain('+ header');
    expect(body).toContain('aria-label="header name"');
    expect(body).toContain('value="X-Api-Key"');
    expect(body).toContain('smeltr sets this header itself');
    expect(body.match(/class="refused/g)).toHaveLength(1);
  });
});
