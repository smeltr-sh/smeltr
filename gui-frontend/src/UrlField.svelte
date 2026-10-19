<!--
  A URL a connector or the leader reaches: a `Field` holding one text input, with
  what `urlWarning` says about the address beside it. Whether the address is a
  URL at all is the save's to say: the leader refuses it on this `field`
  (`config_file::load::web_url`), and the `Field` shows that like any save error.
  `sends` is whether a credential goes with a request besides one in the URL;
  `instead` is where this form names a user and password. `optional` leaves a
  cleared field unset rather than empty.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';
  import Field from './Field.svelte';
  import { urlWarning } from './http';

  let {
    label,
    field,
    value = $bindable(),
    sends,
    instead,
    optional = false,
    error = null,
    placeholder,
    mono = false,
    help,
    note,
  }: {
    label: string;
    field?: string;
    value: string | undefined;
    sends: boolean;
    instead?: string;
    optional?: boolean;
    error?: string | null;
    placeholder?: string;
    mono?: boolean;
    help?: Snippet;
    note?: Snippet | string;
  } = $props();

  const warning = $derived(urlWarning(value ?? '', sends, instead) ?? null);
</script>

<Field {label} {field} {error} {warning} {help} {note}>
  <input
    type="text"
    class:mono
    value={value ?? ''}
    oninput={(e) => {
      const typed = e.currentTarget.value;
      value = optional && typed.trim() === '' ? undefined : typed;
    }}
    {placeholder}
  />
</Field>

<style>
  .mono {
    font-family: var(--mono);
  }
</style>
