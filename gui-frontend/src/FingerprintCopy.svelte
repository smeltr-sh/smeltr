<!--
  Copies a certificate's fingerprint, as RFC 5425 writes one (`shownFingerprint`), which is what a syslog sender
  pins and a receiver's sender fingerprints take. The whole of it is its title, for a reader with no clipboard.
-->
<script lang="ts">
  import { shownFingerprint } from './api';

  let {
    fingerprint,
    label = 'Copy fingerprint',
    link = false,
  }: {
    /** A `DescribedCertificate`'s. */
    fingerprint: string;
    label?: string;
    /** Set in text, as a link button, rather than among buttons. */
    link?: boolean;
  } = $props();

  let copied = $state(false);

  async function copy() {
    try {
      await navigator.clipboard.writeText(shownFingerprint(fingerprint));
      copied = true;
    } catch {
      // No clipboard access: the fingerprint is the title, to read by hand.
    }
  }
</script>

<button
  type="button"
  class={link ? 'link-button' : undefined}
  title={shownFingerprint(fingerprint)}
  onclick={copy}>{copied ? 'Copied' : label}</button
>
