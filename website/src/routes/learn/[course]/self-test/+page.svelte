<script lang="ts">
  import { parts } from '#lib/videos.ts';

  let { data } = $props();
  const course = $derived(data.course);
  const test = $derived(data.test);
  const listed = $derived(data.listed);
  const chapters = $derived(parts(course));

  // The choice picked for each question, by position. A pick is final: the page shows the answer at once.
  let picks = $state<(number | undefined)[]>([]);
  const answered = $derived(picks.filter((p) => p !== undefined).length);
  const right = $derived(test.questions.filter((q, i) => picks[i] === q.right).length);
  const done = $derived(answered === test.questions.length);
  // The chapters of the questions missed, in course order, each once.
  const review = $derived(
    [
      ...new Set(test.questions.filter((q, i) => picks[i] !== q.right).map((q) => q.chapter)),
    ].toSorted((a, b) => a - b),
  );
</script>

<svelte:head><title>Self-test: {course.title} — smeltr</title></svelte:head>

<div class="marketing">
  <section class="self-test">
    <p class="eyebrow">
      <a href="/learn">Learn</a> · {#if listed}<a href="/learn/{course.slug}">{course.title}</a
        >{:else}{course.title}{/if}
    </p>
    <h1>Self-test</h1>
    <ol class="questions">
      {#each test.questions as q, i (i)}
        {@const pick = picks[i]}
        <li>
          <p class="eyebrow">Chapter {q.chapter} · {chapters[q.chapter - 1]}</p>
          <h2>{q.question}</h2>
          <div class="choices">
            {#each q.choices as choice, c (c)}
              <button
                class:right={pick !== undefined && c === q.right}
                class:wrong={pick === c && c !== q.right}
                aria-disabled={pick !== undefined}
                aria-pressed={pick === c}
                onclick={() => (picks[i] ??= c)}
              >
                <span class="mark" aria-hidden="true"
                  >{pick !== undefined && c === q.right ? '✓' : pick === c ? '✗' : ''}</span
                >{choice}</button
              >
            {/each}
          </div>
          {#if pick !== undefined}
            <p class="answer">
              <strong class:right={pick === q.right} class:wrong={pick !== q.right}
                >{pick === q.right ? 'Right.' : 'Wrong.'}</strong
              >
              {q.answer}
            </p>
          {/if}
        </li>
      {/each}
    </ol>
    {#if done}
      <div class="score" role="status">
        <h2>{right} of {test.questions.length} right</h2>
        {#if !review.length}
          <p>Nothing to watch again.</p>
        {:else}
          <p>
            Watch again:
            {#each review as n, k (n)}{@const chapter = `${n}. ${chapters[n - 1]}`}{k
                ? ', '
                : ''}{#if listed}<a href="/learn/{course.slug}">{chapter}</a
                >{:else}{chapter}{/if}{/each}
          </p>
        {/if}
        <button
          onclick={() => {
            picks = [];
            scrollTo(0, 0);
          }}>Start over</button
        >
      </div>
    {/if}
  </section>
</div>

<style>
  .marketing .self-test {
    padding-top: 2.5rem;
    max-width: 48rem;
  }
  .eyebrow a {
    color: inherit;
  }
  .marketing h1 {
    font-size: clamp(2rem, 4vw, 3rem);
    margin-bottom: 1.5rem;
  }
  .questions {
    list-style: none;
    margin: 0;
    padding: 0;
    display: grid;
    gap: 1rem;
  }
  .questions li,
  .score {
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
    padding: 1.3rem 1.4rem 1.5rem;
  }
  .questions .eyebrow {
    margin-bottom: 0.5rem;
  }
  .marketing h2 {
    max-width: none;
    font-size: 1.2rem;
    margin: 0 0 1rem;
  }
  .choices {
    display: grid;
    gap: 0.5rem;
  }
  .choices button {
    display: grid;
    grid-template-columns: 1.2rem 1fr;
    gap: 0.5rem;
    text-align: left;
  }
  /* aria-disabled, not disabled, so an answered question's choices stay readable and focusable; the cursor stops inviting a press. */
  .choices button[aria-disabled='true'] {
    cursor: default;
  }
  .choices button[aria-disabled='true']:not(.right, .wrong):hover {
    border-color: var(--border);
  }
  .right {
    border-color: var(--added);
  }
  .right .mark,
  strong.right {
    color: var(--added);
  }
  .wrong {
    border-color: var(--error);
  }
  .wrong .mark,
  strong.wrong {
    color: var(--error);
  }
  .answer {
    margin: 1rem 0 0;
  }
  .score {
    margin-top: 1rem;
  }
</style>
