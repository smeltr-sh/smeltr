<script lang="ts">
  import diagrams from '#lib/workflow-diagrams.json';
  import { diagramGlyphs } from '#lib/diagram-glyphs.ts';
  type Diagram = {
    title: string;
    description: string;
    caption: string;
    layouts: {
      width: number;
      height: number;
      nodes: {
        x: number;
        y: number;
        w: number;
        title: string;
        detail: string[];
        role?: string;
        stacked?: boolean;
      }[];
      paths: string[];
      controlPaths?: string[];
      storagePaths?: string[];
      boundaries?: { x: number; y: number; w: number; h: number; label: string }[];
      labels: { x: number; y: number; text: string }[];
    }[];
  };
  let { kind }: { kind: keyof typeof diagrams } = $props();
  const uid = $props.id();
  const diagram: Diagram = $derived(diagrams[kind]);
</script>

<figure>
  <div class="layouts">
    {#each diagram.layouts as layout, index (index)}
      <svg
        class:compact={index === 1}
        class:wide={index === 0}
        viewBox={`0 0 ${layout.width} ${layout.height}`}
        role="img"
        aria-labelledby={`${uid}-${index}-title ${uid}-${index}-description`}
      >
        <title id={`${uid}-${index}-title`}>{diagram.title}</title>
        <desc id={`${uid}-${index}-description`}>{diagram.description}</desc>
        <defs>
          <marker
            id={`${uid}-${index}-arrow`}
            viewBox="0 0 10 10"
            refX="9"
            refY="5"
            markerWidth="6"
            markerHeight="6"
            orient="auto-start-reverse"
          >
            <path d="M0 0L10 5L0 10Z" fill="var(--accent)" />
          </marker>
        </defs>
        {#each layout.boundaries ?? [] as boundary (boundary.label)}
          <rect
            class="boundary"
            x={boundary.x}
            y={boundary.y}
            width={boundary.w}
            height={boundary.h}
            rx="12"
          />
          <text class="boundary-label" x={boundary.x + 16} y={boundary.y + 25}
            >{boundary.label}</text
          >
        {/each}
        {#each layout.storagePaths ?? [] as path (path)}
          <path d={path} class="storage-link" />
        {/each}
        {#each layout.controlPaths ?? [] as path (path)}
          <path d={path} class="connection control" marker-end={`url(#${uid}-${index}-arrow)`} />
        {/each}
        {#each layout.paths as path (path)}
          <path d={path} class="connection" marker-end={`url(#${uid}-${index}-arrow)`} />
        {/each}
        {#each layout.labels as label (label.text)}
          <text class="node-detail" x={label.x} y={label.y}>{label.text}</text>
        {/each}
        {#each layout.nodes as node (node.title)}
          {@const glyph = node.role ? diagramGlyphs[node.role] : undefined}
          <g>
            {#if node.stacked}
              {#each [16, 8] as offset (offset)}
                <rect
                  x={node.x + offset}
                  y={node.y - offset}
                  width={node.w}
                  height="80"
                  rx="8"
                  aria-hidden="true"
                />
              {/each}
            {/if}
            <rect x={node.x} y={node.y} width={node.w} height="80" rx="8" />
            {#if glyph}
              <path
                class="node-glyph"
                d={glyph}
                transform={`translate(${node.x + 17} ${node.y + 9})`}
                aria-hidden="true"
              />
            {/if}
            <text
              class="node-title"
              class:with-glyph={!!glyph}
              x={node.x + (glyph ? 52 : node.w / 2)}
              y={node.y + 29}>{node.title}</text
            >
            {#each node.detail as line, lineIndex (lineIndex)}
              <text class="node-detail" x={node.x + node.w / 2} y={node.y + 51 + lineIndex * 18}
                >{line}</text
              >
            {/each}
          </g>
        {/each}
      </svg>
    {/each}
  </div>
  <figcaption>{diagram.caption}</figcaption>
</figure>

<style>
  figure {
    margin: 1.5rem 0;
  }
  .layouts {
    container-type: inline-size;
    padding: 0.5rem;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
  }
  svg {
    display: block;
    width: 100%;
    height: auto;
  }
  .compact {
    display: none;
  }
  rect {
    fill: var(--bg);
    stroke: var(--border);
    stroke-width: 1.5;
  }
  text {
    text-anchor: middle;
    font-family: var(--font-body);
  }
  .node-title {
    fill: var(--ink);
    font-size: 18px;
    font-weight: 600;
  }
  .node-detail {
    fill: var(--dim);
    font-size: 14px;
  }
  .node-title.with-glyph {
    text-anchor: start;
  }
  .node-glyph {
    fill: none;
    stroke: var(--dim);
    stroke-width: 1.8;
    stroke-linecap: round;
    stroke-linejoin: round;
  }
  .connection {
    fill: none;
    stroke: var(--accent);
    stroke-width: 2;
    stroke-linejoin: round;
  }
  .control {
    stroke-dasharray: 7 5;
  }
  .boundary {
    fill: none;
    stroke-dasharray: 6 5;
  }
  .boundary-label {
    text-anchor: start;
    fill: var(--dim);
    font-size: 14px;
    font-weight: 600;
  }
  .storage-link {
    fill: none;
    stroke: var(--dim);
    stroke-width: 2;
    stroke-dasharray: 2 5;
  }
  figcaption {
    margin-top: 0.8rem;
    color: var(--ink);
    font-size: 0.9rem;
    line-height: 1.6;
    max-width: 68ch;
  }
  @container (max-width: 45rem) {
    .wide {
      display: none;
    }
    .compact {
      display: block;
      max-width: 26rem;
      margin-inline: auto;
    }
    .node-title {
      font-size: 17px;
    }
  }
</style>
