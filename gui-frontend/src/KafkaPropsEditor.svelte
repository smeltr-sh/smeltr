<!--
  A Kafka client's extra librdkafka properties, in the two maps of `api.ts`'s
  `KafkaProps`, on the shared two-table editor.

  What the security section writes is not shown here and cannot be typed
  here: one place each property comes from.
-->
<script lang="ts">
  import PropsEditor from './PropsEditor.svelte';
  import type { KafkaFacts, KafkaProps, SecretDto } from './api';
  import { propertyRefusal, SECTION_PROPS } from './kafka';

  let {
    value,
    secrets = [],
    kafka,
    hasTls = false,
  }: {
    value: KafkaProps;
    secrets?: SecretDto[];
    kafka: KafkaFacts;
    hasTls?: boolean;
  } = $props();
</script>

<PropsEditor
  {value}
  literalKey="properties"
  labels={{
    literal: 'properties',
    credentials: 'credentials',
    noun: 'property',
    literalName: 'socket.timeout.ms',
    literalValue: '60000',
    credentialName: 'sasl.password',
  }}
  {secrets}
  hidden={(key) => SECTION_PROPS.includes(key.trim().toLowerCase())}
  refusal={(key, credential) => propertyRefusal(key, credential, hasTls, kafka)}
>
  {#snippet literalHelp()}
    Any other librdkafka property, by name and value, committed to the config repo.
  {/snippet}
  {#snippet credentialsHelp()}
    A librdkafka property whose value is secret, such as <code>sasl.password</code>; the value never
    reaches git.
  {/snippet}
</PropsEditor>
