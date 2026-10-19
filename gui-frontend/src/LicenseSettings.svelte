<!--
  Admin -> System -> License: who must buy a license. Nothing here gates a
  feature; the license in `LICENSE` sets who may use it in production.
-->
<script lang="ts">
  const GET_URL = 'https://smeltr.sh/get/';
  const LICENSE_URL = 'https://github.com/smeltr-sh/smeltr/blob/main/LICENSE';
</script>

<div class="panel">
  <div class="head"><h2>License</h2></div>
  <dl>
    <dt>Individual or fewer than 250 employees</dt>
    <dd><a href={LICENSE_URL} target="_blank" rel="noopener">Free ↗</a></dd>
    <dt>Organization with 250 or more employees</dt>
    <dd><a href={GET_URL} target="_blank" rel="noopener">Buy a license for this leader ↗</a></dd>
  </dl>
</div>

<style>
  .panel {
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    padding: 1.2rem;
    max-width: 52rem;
    display: grid;
    gap: 0.9rem;
  }

  .head {
    padding-bottom: 0.9rem;
    border-bottom: 1px solid var(--border);
  }

  .head h2 {
    font-family: var(--font-display);
    font-size: var(--text-title);
    font-weight: 600;
    margin: 0;
  }

  dl {
    display: grid;
    grid-template-columns: max-content auto;
    gap: 0.6rem 1.5rem;
    margin: 0;
  }

  dd {
    margin: 0;
  }
</style>
