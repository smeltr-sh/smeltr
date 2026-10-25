<!--
  A field whose value is any JSON, typed as text. Text that does not parse is
  shown as an error under it and leaves the value as it was; empty text is no
  value.
-->
<script lang="ts">
  import { untrack, type Snippet } from 'svelte';
  import Field from './Field.svelte';

  let {
    label,
    field,
    value,
    set,
    help,
    note,
    placeholder = '',
    rows = 4,
    optional = false,
  }: {
    label: string;
    field?: string;
    value: unknown;
    set: (value: unknown) => void;
    help?: Snippet;
    note?: Snippet | string;
    placeholder?: string;
    rows?: number;
    optional?: boolean;
  } = $props();

  const written = (v: unknown) => (v === undefined ? '' : JSON.stringify(v, null, 2));

  // The value it starts from; the effect below follows it after.
  let text = $state(untrack(() => written(value)));
  let error = $state<string | null>(null);

  // A value changed from elsewhere (an undo, the JSON page) replaces the text, unless the text already says it.
  $effect(() => {
    const now = JSON.stringify(value);
    let typed: string | undefined;
    try {
      typed = text.trim() === '' ? undefined : JSON.stringify(JSON.parse(text));
    } catch {
      return;
    }
    if (typed !== now) {
      text = written(value);
      error = null;
    }
  });

  function edited(next: string) {
    text = next;
    if (next.trim() === '') {
      error = null;
      set(undefined);
      return;
    }
    try {
      const parsed = JSON.parse(next);
      error = null;
      set(parsed);
    } catch (e) {
      error = `This is not JSON: ${(e as Error).message}`;
    }
  }
</script>

<Field {label} {field} {help} {note} {error} {optional}>
  <textarea
    {rows}
    spellcheck="false"
    {placeholder}
    value={text}
    oninput={(e) => edited(e.currentTarget.value)}></textarea>
</Field>

<style>
  textarea {
    width: 100%;
    box-sizing: border-box;
    font-family: var(--mono);
    resize: vertical;
  }
</style>
