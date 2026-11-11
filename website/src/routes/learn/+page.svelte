<script lang="ts">
  import VideoPlayer from '#lib/components/VideoPlayer.svelte';
  import { courseLength, courses, levels, released, roles, topics } from '#lib/videos.ts';

  const groups = [
    { key: 'level', name: 'Level', values: levels },
    { key: 'role', name: 'Role', values: roles },
    { key: 'topic', name: 'Topic', values: topics },
  ] as const;
  type Group = (typeof groups)[number]['key'];

  const videos = courses
    .flatMap((course) =>
      course.videos.map((video, i) => {
        const tags: Record<Group, readonly string[]> = {
          level: [video.level],
          role: video.roles,
          topic: video.topics,
        };
        return { course, video, n: i + 1, tags };
      }),
    )
    // A stable sort, so a course's videos keep their order within a level.
    .toSorted((a, b) => levels.indexOf(a.video.level) - levels.indexOf(b.video.level));
  type Entry = (typeof videos)[number];
  type Pick = { group: Group; value: string };

  let picked = $state<Pick[]>([]);
  let query = $state('');

  const isPicked = (group: Group, value: string) =>
    picked.some((p) => p.group === group && p.value === value);
  const toggle = (group: Group, value: string) =>
    (picked = isPicked(group, value)
      ? picked.filter((p) => p.group !== group || p.value !== value)
      : [...picked, { group, value }]);

  // Within a group a video needs one of the picked tags; across groups it needs them all.
  function matches(entry: Entry, tags: Pick[]) {
    const words = query.trim().toLowerCase().split(/\s+/).filter(Boolean);
    const text = [entry.video.title, entry.course.title, ...entry.video.topics]
      .join(' ')
      .toLowerCase();
    return (
      groups.every(({ key }) => {
        const wanted = tags.filter((p) => p.group === key);
        return !wanted.length || wanted.some((p) => entry.tags[key].includes(p.value));
      }) && words.every((w) => text.includes(w))
    );
  }

  const filtering = $derived(picked.length > 0 || query.trim() !== '');
  const found = $derived(videos.filter((entry) => matches(entry, picked)));

  // What picking the tag would show: the other picks in its own group do not narrow it.
  const count = (group: Group, value: string) =>
    videos.filter(
      (entry) =>
        entry.tags[group].includes(value) &&
        matches(
          entry,
          picked.filter((p) => p.group !== group),
        ),
    ).length;
</script>

<svelte:head><title>Learn — smeltr</title></svelte:head>

<div class="marketing">
  <section class="page-hero">
    <h1>Learn</h1>
  </section>

  <div class="find">
    <input type="search" aria-label="Find a video" placeholder="Find a video…" bind:value={query} />
    {#each groups as group (group.key)}
      <div class="group" role="group" aria-labelledby="tags-{group.key}">
        <span class="name" id="tags-{group.key}">{group.name}</span>
        <div class="tags">
          {#each group.values as value (value)}
            {#if isPicked(group.key, value) || count(group.key, value)}
              <button
                class:primary={isPicked(group.key, value)}
                aria-pressed={isPicked(group.key, value)}
                onclick={() => toggle(group.key, value)}
                >{value} <span class="count">{count(group.key, value)}</span></button
              >
            {/if}
          {/each}
        </div>
      </div>
    {/each}
  </div>

  {#if filtering}
    <div class="library">
      {#each found as entry (`${entry.course.slug}/${entry.video.slug}`)}
        {@const { course, video, n } = entry}
        <article class="card">
          <VideoPlayer {video} href="/learn/{course.slug}/{video.slug}" />
          <div class="copy">
            <p class="eyebrow">
              <a href="/learn/{course.slug}">{course.title}</a> · {n}/{course.videos
                .length}{video.minutes ? ` · ${video.minutes} min` : ''}
            </p>
            <h2>
              {#if video.youtube}<a href="/learn/{course.slug}/{video.slug}">{video.title}</a
                >{:else}{video.title}{/if}
            </h2>
            <div class="tags">
              {#each groups as group (group.key)}
                {#each entry.tags[group.key] as value (value)}
                  <button
                    class="compact"
                    class:primary={isPicked(group.key, value)}
                    aria-pressed={isPicked(group.key, value)}
                    onclick={() => toggle(group.key, value)}>{value}</button
                  >
                {/each}
              {/each}
            </div>
          </div>
        </article>
      {/each}
    </div>
  {:else}
    <div class="library">
      {#each courses as course (course.slug)}
        <article class="card">
          <VideoPlayer
            video={released(course)[0] ?? course.videos[0]}
            href="/learn/{course.slug}"
          />
          <div class="copy">
            <p class="eyebrow">{courseLength(course)}</p>
            <h2><a href="/learn/{course.slug}">{course.title}</a></h2>
            {#if course.demo}
              <a class="demo" href="/demo/try/" rel="external">Try it yourself in the demo →</a>
            {/if}
          </div>
        </article>
      {/each}
    </div>
  {/if}
</div>

<style>
  .marketing .page-hero {
    padding-bottom: 1.5rem;
  }
  .find {
    display: grid;
    gap: 0.6rem;
    margin-bottom: 1.5rem;
  }
  .find input {
    width: 100%;
    font-size: 1.05rem;
    padding: 0.7rem 0.9rem;
    margin-bottom: 0.3rem;
  }
  .group {
    display: grid;
    grid-template-columns: 4.5rem minmax(0, 1fr);
    align-items: baseline;
  }
  .name {
    font-size: 0.85rem;
    font-weight: 600;
    color: var(--dim);
  }
  .tags {
    display: flex;
    flex-wrap: wrap;
    gap: 0.4rem;
  }
  .count {
    margin-left: 0.2rem;
    color: var(--dim);
    font-family: var(--mono);
    font-size: 0.75rem;
  }
  .primary .count {
    color: inherit;
  }
  .library {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 1rem;
  }
  .card {
    display: flex;
    flex-direction: column;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
    overflow: hidden;
  }
  .copy {
    padding: 1.3rem 1.4rem 1.5rem;
  }
  .copy .eyebrow {
    margin-bottom: 0.5rem;
  }
  .eyebrow a {
    color: inherit;
  }
  .copy h2 {
    font-size: 1.3rem;
    margin: 0 0 0.9rem;
  }
  .copy h2 a {
    color: inherit;
    text-decoration: none;
  }
  .demo {
    font-weight: 600;
    font-size: 0.92rem;
  }
  @media (max-width: 46rem) {
    .library,
    .group {
      grid-template-columns: 1fr;
    }
  }
</style>
