<!--
  One separator character -- a CSV delimiter, a Key/value parser's pair or key/value separator. The engine takes one
  byte, so the field holds at most one character, and the characters an input shows as blank show as text instead: a
  space as `␣`, a tab as `\t`. Either form typed or pasted in is read back as the character.
-->
<script module lang="ts">
  /** `value` as the field shows it. */
  export function showSeparator(value: string): string {
    return value.replaceAll(' ', '␣').replaceAll('\t', '\\t');
  }

  /**
   * The separator the field's text stands for: the character just typed, since the field holds one. That is the last
   * character before `caret`, where the input leaves the caret after an insert, wherever in the text it went.
   */
  export function readSeparator(text: string, caret = text.length): string {
    const read = (part: string) => [...part.replaceAll('\\t', '\t').replaceAll('␣', ' ')].at(-1);
    return read(text.slice(0, caret)) ?? read(text) ?? '';
  }
</script>

<script lang="ts">
  let { value = $bindable() }: { value: string } = $props();
</script>

<input
  type="text"
  spellcheck="false"
  value={showSeparator(value)}
  oninput={(e) => {
    value = readSeparator(e.currentTarget.value, e.currentTarget.selectionStart ?? undefined);
    // Written back by hand: when the value does not change (a second `,` over a `,`), nothing re-renders the field.
    e.currentTarget.value = showSeparator(value);
  }}
/>

<style>
  input {
    font-family: var(--mono);
  }
</style>
