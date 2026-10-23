import MarkdownIt from 'markdown-it';

/** Markdown from the agent, made safe to show. The agent writes after reading records other systems sent, so the
 * text is untrusted: raw HTML stays text, an image shows only its alt text so showing a reply fetches nothing, and
 * a link goes only to the web, mail or the GUI's own routes. */
const md = new MarkdownIt({ html: false, linkify: false, breaks: true });

md.validateLink = (url) => /^(https?:|mailto:|#\/)/i.test(url.trim());

md.renderer.rules.image = (tokens, idx, options, env, self) =>
  md.utils.escapeHtml(self.renderInlineAsText(tokens[idx].children ?? [], options, env));

// A link out of the GUI opens beside it, so the conversation stays where it was.
md.renderer.rules.link_open = (tokens, idx, options, _env, self) => {
  const link = tokens[idx];
  if (!String(link.attrGet('href')).startsWith('#')) {
    link.attrSet('target', '_blank');
    link.attrSet('rel', 'noopener noreferrer');
  }
  return self.renderToken(tokens, idx, options);
};

/** The HTML for `text`. Half-written Markdown renders too: an open code fence runs to the end. */
export function renderMarkdown(text: string): string {
  return md.render(text);
}

/** The text of `text`'s first paragraph, heading or code block, without its markup: a title in one line. */
export function firstLineText(text: string): string {
  for (const token of md.parse(text, {})) {
    if (token.type === 'inline')
      return md.renderer
        .renderInlineAsText(token.children ?? [], md.options, {})
        .replace(/\s+/g, ' ')
        .trim();
    if (token.type === 'fence' || token.type === 'code_block')
      return token.content.trim().split('\n')[0];
  }
  return text.trim();
}
