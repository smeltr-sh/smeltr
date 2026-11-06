<!--
  Copies `text` to the clipboard, and reads Copied for two seconds after, or until `text` changes. Without clipboard
  access it does nothing: whatever it copies is on screen, or in its title, to select by hand.
-->
<script lang="ts">
  let {
    text,
    label = 'Copy',
    title,
    link = false,
  }: {
    text: string;
    label?: string;
    title?: string;
    /** Styled as a link, for use inside text rather than among buttons. */
    link?: boolean;
  } = $props();

  let copied = $state(false);
  let reset: ReturnType<typeof setTimeout> | undefined;

  $effect(() => {
    void text;
    copied = false;
  });
  $effect(() => () => clearTimeout(reset));

  async function copy() {
    try {
      await navigator.clipboard.writeText(text);
      copied = true;
      clearTimeout(reset);
      reset = setTimeout(() => (copied = false), 2000);
    } catch {
      // No clipboard access.
    }
  }
</script>

<button
  type="button"
  class={link ? 'link-button' : undefined}
  {title}
  aria-live="polite"
  onclick={copy}>{copied ? 'Copied' : label}</button
>
