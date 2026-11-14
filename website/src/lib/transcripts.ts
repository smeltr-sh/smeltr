// Each released video's captions, as uploaded to YouTube, at `transcripts/{course}/{video}.srt`.
const files = import.meta.glob<string>('./transcripts/*/*.srt', {
  query: '?raw',
  import: 'default',
});

/** A pause this long or longer between captions starts a new paragraph. */
const PAUSE = 1;

const seconds = (time: string) => {
  const [h, m, s] = time.replace(',', '.').split(':').map(Number);
  return h * 3600 + m * 60 + s;
};

/** The spoken text of a course's video, in paragraphs split where the narrator pauses. Every released video has
 * one: the build fails for a video without one. */
export async function transcript(course: string, video: string) {
  const load = files[`./transcripts/${course}/${video}.srt`];
  if (!load) throw new Error(`No transcript at src/lib/transcripts/${course}/${video}.srt`);
  const paragraphs: string[][] = [];
  let end = -Infinity;
  for (const cue of (await load()).replaceAll('\r\n', '\n').trim().split(/\n\n+/)) {
    // A cue is its number, its `start --> end` line, then its text.
    const [, times, ...text] = cue.split('\n');
    const [from, to] = times.split(' --> ').map(seconds);
    if (from - end >= PAUSE) paragraphs.push([]);
    paragraphs.at(-1)!.push(text.join(' '));
    end = to;
  }
  return paragraphs.map((lines) => lines.join(' '));
}
