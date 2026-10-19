/** The points an SVG path passes through, one list per subpath, each rounded to one decimal.
 * Tests read a drawn path with it, so they pin where the pen goes and not how the path text is
 * spelled. It reads the absolute M, L, C and Z the charts write. A repeated point stays: a dot is
 * a zero-length segment and must read as two points. Z adds no point. A fill closes each subpath
 * whether or not the path says so, but a stroke draws Z as a line back to the subpath's start, so
 * a test of a stroke checks for Z itself. */
export function pathPoints(d: string): [number, number][][] {
  const subpaths: [number, number][][] = [];
  for (const segment of d.match(/[MLCZ][^MLCZ]*/g) ?? []) {
    const command = segment[0];
    const n = (segment.match(/-?[\d.]+/g) ?? []).map(Number);
    if (command === 'M') subpaths.push([]);
    // A curve passes through the last of its three pairs. The first two only pull on it.
    const stride = command === 'C' ? 6 : 2;
    for (let i = stride - 2; i < n.length; i += stride) {
      subpaths[subpaths.length - 1].push([round(n[i]), round(n[i + 1])]);
    }
  }
  return subpaths;
}

const round = (v: number) => Math.round(v * 10) / 10;
