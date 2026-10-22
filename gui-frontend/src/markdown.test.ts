import { describe, expect, it } from 'vitest';
import { renderMarkdown } from './markdown';

describe('renderMarkdown', () => {
  it('renders what an agent writes', () => {
    const html = renderMarkdown(
      '**What it means:** see `main`.\n\n- one\n- two\n\n1. first\n\n| a | b |\n|---|---|\n| 1 | 2 |\n\n```json\n{"a": 1}\n```',
    );
    expect(html).toContain('<strong>What it means:</strong>');
    expect(html).toContain('<code>main</code>');
    expect(html).toContain('<li>one</li>');
    expect(html).toContain('<ol>');
    expect(html).toContain('<td>2</td>');
    expect(html).toContain('<pre><code class="language-json">{&quot;a&quot;: 1}\n</code></pre>');
  });

  it('keeps the lines an answer was written in', () => {
    expect(renderMarkdown('one\ntwo')).toBe('<p>one<br>\ntwo</p>\n');
  });

  it('shows raw HTML as text', () => {
    const html = renderMarkdown('<img src=x onerror=alert(1)> <script>alert(1)</script>');
    expect(html).not.toContain('<img');
    expect(html).not.toContain('<script');
    expect(html).toContain('&lt;script&gt;');
  });

  it('shows an image as its alt text, so nothing is fetched', () => {
    const html = renderMarkdown('![the <secret>](https://example.com/x.png?d=1)');
    expect(html).toBe('<p>the &lt;secret&gt;</p>\n');
  });

  it('links only to the web, mail and the GUI', () => {
    expect(renderMarkdown('[a](javascript:alert(1))')).not.toContain('<a');
    expect(renderMarkdown('[a](data:text/html,x)')).not.toContain('<a');
    expect(renderMarkdown('[a](/api/me)')).not.toContain('<a');
    expect(renderMarkdown('[a](mailto:x@example.com)')).toContain('href="mailto:x@example.com"');
  });

  it('opens a link out of the GUI beside it, and a GUI link in place', () => {
    expect(renderMarkdown('[docs](https://smeltr.sh/docs)')).toContain(
      '<a href="https://smeltr.sh/docs" target="_blank" rel="noopener noreferrer">docs</a>',
    );
    expect(renderMarkdown('[pipelines](#/pipelines)')).toContain(
      '<a href="#/pipelines">pipelines</a>',
    );
  });

  it('renders a half-written code block to its end', () => {
    expect(renderMarkdown('Here:\n\n```\nlet a')).toContain('<pre><code>let a</code></pre>');
  });
});
