<!--
  A source's one `lineBreaker`: the pattern that ends a record, as Splunk's `LINE_BREAKER`, and why a save would
  refuse it. Blank writes none, which breaks at each line end, as the empty box shows. A receiver's line breakers by
  sourcetype are `ReceiverSourcetypes`.
-->
<script lang="ts">
  import Field from './Field.svelte';
  import LineBreakerRefusal from './LineBreakerRefusal.svelte';
  import { DEFAULT_LINE_BREAKER } from './ReceiverSourcetypes.svelte';

  let {
    value,
    onchange,
  }: {
    value: string | undefined;
    onchange: (pattern: string | undefined) => void;
  } = $props();
</script>

<Field field="lineBreaker" label="line breaker" optional>
  {#snippet help()}
    Where a record ends. Empty, each line is a record. The first capture group is required and will
    be dropped. E.g.
    <code>{'</event>(\\s*)'}</code> ends a record after each <code>{'</event>'}</code> and drops the
    whitespace that follows, and <code>{'()<event>'}</code> starts a new record at each
    <code>{'<event>'}</code> and drops nothing.
  {/snippet}
  <input
    type="text"
    placeholder={DEFAULT_LINE_BREAKER}
    aria-label="line breaker"
    value={value ?? ''}
    oninput={(e) => onchange(e.currentTarget.value.trim() ? e.currentTarget.value : undefined)}
  />
  <LineBreakerRefusal pattern={value ?? ''} />
</Field>
