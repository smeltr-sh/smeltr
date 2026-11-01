<!--
  One warning, its mark and its words: a schema finding, said the same under a step and under the steps.
-->
<script lang="ts">
  import Ticked from './Ticked.svelte';

  let { message }: { message: string } = $props();
</script>

<div class="warning-line">
  <svg viewBox="0 0 24 24" fill="none" aria-hidden="true">
    <path d="M12 3.5 L22 20.5 H2 Z" fill="currentColor" />
    <rect x="11" y="9" width="2" height="6" rx="1" class="cutout" />
    <rect x="11" y="16.5" width="2" height="2" rx="1" class="cutout" />
  </svg>
  <span><Ticked text={message} /></span>
</div>

<style>
  .warning-line {
    display: flex;
    gap: 0.5rem;
    align-items: flex-start;
    font-size: var(--text-note);
    color: var(--warn);
    line-height: 1.45;
  }

  svg {
    width: 15px;
    height: 15px;
    flex-shrink: 0;
    margin-top: 0.1rem;
  }

  .cutout {
    fill: var(--field);
  }
</style>
