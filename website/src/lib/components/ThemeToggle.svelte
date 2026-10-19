<script lang="ts">
  import { onMount } from 'svelte';

  type Theme = 'system' | 'light' | 'dark';

  // "System" is the absence of an override, not a third stored value: it
  // removes the attribute and lets the media query in app.css decide.
  let theme = $state<Theme>('system');
  let mounted = $state(false);

  onMount(() => {
    try {
      const stored = localStorage.getItem('smeltr-theme');
      if (stored === 'light' || stored === 'dark') theme = stored;
    } catch {
      // Unavailable storage just means the system preference stands.
    }
    mounted = true;
  });

  function choose(next: Theme) {
    theme = next;
    if (next === 'system') {
      document.documentElement.removeAttribute('data-theme');
    } else {
      document.documentElement.setAttribute('data-theme', next);
    }
    try {
      if (next === 'system') localStorage.removeItem('smeltr-theme');
      else localStorage.setItem('smeltr-theme', next);
    } catch {
      // The choice still applies to this page; it just will not persist.
    }
  }

  const options: { value: Theme; label: string }[] = [
    { value: 'system', label: 'Auto' },
    { value: 'light', label: 'Light' },
    { value: 'dark', label: 'Dark' },
  ];
</script>

<!-- Reserve the control's space until the saved preference is available. -->
<div class="toggle" class:ready={mounted}>
  <svg
    viewBox="0 0 24 24"
    width="20"
    height="20"
    fill="none"
    stroke="currentColor"
    stroke-width="1.6"
    stroke-linecap="round"
    stroke-linejoin="round"
    aria-hidden="true"
  >
    {#if theme === 'light'}
      <circle cx="12" cy="12" r="4" />
      <path d="M12 2v2m0 16v2M2 12h2m16 0h2M5 5l1.5 1.5m11 11L19 19M5 19l1.5-1.5m11-11L19 5" />
    {:else if theme === 'dark'}
      <path d="M20.5 13A8.5 8.5 0 0 1 11 3.5 8.5 8.5 0 1 0 20.5 13Z" />
    {:else}
      <rect x="3" y="4" width="18" height="13" rx="2" />
      <path d="M12 17v4m-4 0h8" />
    {/if}
  </svg>
  <select
    aria-label="Color theme"
    title="Color theme"
    value={theme}
    onchange={(event) => choose(event.currentTarget.value as Theme)}
  >
    {#each options as option (option.value)}
      <option value={option.value}>{option.label}</option>
    {/each}
  </select>
</div>

<style>
  .toggle {
    position: relative;
    display: grid;
    place-items: center;
    width: 44px;
    height: 44px;
    flex: 0 0 44px;
    border: 1px solid var(--border);
    border-radius: 50%;
    color: var(--dim);
    visibility: hidden;
  }
  .toggle.ready {
    visibility: visible;
  }
  /* A tint of the ink, not --panel, so the hover reads on the dark band. */
  .toggle:hover {
    background: color-mix(in oklab, var(--ink) 12%, transparent);
    color: var(--ink);
  }
  .toggle:focus-within {
    outline: 2px solid var(--accent);
    outline-offset: 2px;
  }
  svg {
    pointer-events: none;
  }
  select {
    position: absolute;
    inset: 0;
    width: 100%;
    height: 100%;
    opacity: 0;
    cursor: pointer;
    font: inherit;
    /* The open list is the browser's, painted in the page's color scheme.
       Its text must be too, not the band ink the toggle inherits. */
    color: CanvasText;
  }
</style>
