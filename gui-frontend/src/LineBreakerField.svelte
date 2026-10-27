<!--
  A source's one `lineBreaker`: the pattern that ends a record, as Splunk's `LINE_BREAKER`, and why a save would
  refuse it. Blank writes none: a record per line. The box hints no pattern for that, since none breaks exactly as no
  pattern does (a lone CR, a blank line). A receiver's line breakers by sourcetype are `ReceiverSourcetypes`.
-->
<script lang="ts">
  import Field from './Field.svelte';
  import LineBreakerRefusal from './LineBreakerRefusal.svelte';

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
    Where a record ends. Left empty, each line is a record. The first capture group is required and
    will be dropped. E.g.
    <code>{'</event>(\\s*)'}</code> ends a record after each <code>{'</event>'}</code> and drops the
    whitespace that follows, and <code>{'()<event>'}</code> starts a new record at each
    <code>{'<event>'}</code> and drops nothing.
  {/snippet}
  <input
    type="text"
    aria-label="line breaker"
    value={value ?? ''}
    oninput={(e) => onchange(e.currentTarget.value.trim() ? e.currentTarget.value : undefined)}
  />
  <LineBreakerRefusal pattern={value ?? ''} />
</Field>
