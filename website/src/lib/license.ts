// COMMERCIAL-LICENSE.md, at the repository root, is the one text of the commercial license. The page renders
// it: this cuts the file into its blocks, and `Prose` renders each block's inline Markdown.

export type Block =
  | { kind: 'heading'; text: string }
  | { kind: 'paragraph'; text: string }
  | { kind: 'list'; items: string[] };

/** The file's blocks: headings below the title, paragraphs and bullet lists, which is all it uses. The title
 * heading is left out, as the page carries its own. */
export function blocks(markdown: string): Block[] {
  const out: Block[] = [];
  let paragraph: string[] = [];
  let list: string[] = [];
  const flush = () => {
    if (paragraph.length) out.push({ kind: 'paragraph', text: paragraph.join(' ') });
    if (list.length) out.push({ kind: 'list', items: list });
    paragraph = [];
    list = [];
  };
  for (const line of markdown.split('\n')) {
    const heading = /^(#+) (.*)$/.exec(line);
    const item = /^- (.*)$/.exec(line);
    if (heading) {
      flush();
      if (heading[1].length > 1) out.push({ kind: 'heading', text: heading[2] });
    } else if (item) {
      if (paragraph.length) flush();
      list.push(item[1]);
    } else if (line.trim() === '') {
      flush();
    } else {
      paragraph.push(line);
    }
  }
  flush();
  return out;
}
