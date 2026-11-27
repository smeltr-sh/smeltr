<script lang="ts">
  import { diagrams, type DiagramNode } from '#lib/workflow-diagrams.ts';
  import { diagramGlyphs } from '#lib/diagram-glyphs.ts';
  let { kind }: { kind: keyof typeof diagrams } = $props();
  const uid = $props.id();
  const diagram = $derived(diagrams[kind]);

  /** A node's outline. A `joined` node continues the node just above it, so the corners where they meet are square
   * and the two read as one box with a divider. */
  function outline(node: DiagramNode, nodes: DiagramNode[]): string {
    const { x, y, w } = node;
    const h = 80;
    const top = node.joined ? 0 : 8;
    const below = nodes.some((n) => n.joined && n.x === x && n.y === y + h);
    const bottom = below ? 0 : 8;
    return `M${x + top} ${y}H${x + w - top}a${top} ${top} 0 0 1 ${top} ${top}V${y + h - bottom}a${bottom} ${bottom} 0 0 1 -${bottom} ${bottom}H${x + bottom}a${bottom} ${bottom} 0 0 1 -${bottom} -${bottom}V${y + top}a${top} ${top} 0 0 1 ${top} -${top}Z`;
  }
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
        {#each layout.boundaries as boundary (boundary.label)}
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
        {#each layout.storagePaths as path (path)}
          <path d={path} class="storage-link" />
        {/each}
        {#each layout.controlPaths as path (path)}
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
                  class={node.role}
                  x={node.x + offset}
                  y={node.y - offset}
                  width={node.w}
                  height="80"
                  rx="8"
                  aria-hidden="true"
                />
              {/each}
            {/if}
            <path class="shape {node.role}" d={outline(node, layout.nodes)} />
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
  rect,
  .shape {
    fill: var(--bg);
    stroke: var(--border);
    stroke-width: 1.5;
  }
  /* Smeltr's own parts are drawn in the accent, the systems it connects in a dashed outline, and a person without a box. */
  .leader,
  .worker {
    stroke: var(--accent);
  }
  .source,
  .destination,
  .storage {
    fill: none;
    stroke-dasharray: 6 4;
  }
  .administrator {
    fill: none;
    stroke: none;
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
    fill: color-mix(in oklch, var(--accent) 7%, transparent);
    stroke: none;
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
