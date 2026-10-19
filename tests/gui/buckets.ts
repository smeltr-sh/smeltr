// Sources a test stands up in its own process, so a source can read without the store it reads running.
import http from 'node:http';
import type net from 'node:net';

/** A fake S3 on loopback that answers what a listing source asks (`src/io/listing.rs`): ListObjectsV2 and
 * GetObject, path style, over `objects`. It checks no signature, so any key signs in. */
export class FakeS3 {
  private constructor(
    private server: http.Server,
    readonly url: string,
    /** Each bucket's objects, by key. */
    readonly objects: Map<string, Map<string, Buffer>>,
  ) {}

  static async start(objects: Map<string, Map<string, Buffer>>): Promise<FakeS3> {
    let store: FakeS3;
    const server = http.createServer((req, res) => store.answer(req, res));
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
    const { port } = server.address() as net.AddressInfo;
    store = new FakeS3(server, `http://127.0.0.1:${port}`, objects);
    return store;
  }

  private answer(req: http.IncomingMessage, res: http.ServerResponse) {
    const url = new URL(req.url ?? '/', this.url);
    const [bucket, ...rest] = url.pathname.slice(1).split('/');
    const key = decodeURIComponent(rest.join('/'));
    const objects = this.objects.get(bucket);
    if (!objects) {
      res.writeHead(404, { 'content-type': 'application/xml' }).end(error('NoSuchBucket'));
      return;
    }
    if (key === '') {
      const prefix = url.searchParams.get('prefix') ?? '';
      const contents = [...objects]
        .filter(([k]) => k.startsWith(prefix))
        .sort(([a], [b]) => a.localeCompare(b))
        .map(
          ([k, body]) =>
            `<Contents><Key>${escape(k)}</Key><LastModified>${MODIFIED.toISOString()}</LastModified>` +
            `<ETag>"${etag(k)}"</ETag><Size>${body.length}</Size><StorageClass>STANDARD</StorageClass></Contents>`,
        );
      res
        .writeHead(200, { 'content-type': 'application/xml' })
        .end(
          `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>${escape(bucket)}</Name>` +
            `<Prefix>${escape(prefix)}</Prefix><KeyCount>${contents.length}</KeyCount><MaxKeys>1000</MaxKeys>` +
            `<IsTruncated>false</IsTruncated>${contents.join('')}</ListBucketResult>`,
        );
      return;
    }
    const body = objects.get(key);
    if (!body) {
      res.writeHead(404, { 'content-type': 'application/xml' }).end(error('NoSuchKey'));
      return;
    }
    const range = /^bytes=(\d+)-(\d*)$/.exec(req.headers.range ?? '');
    const from = range ? Number(range[1]) : 0;
    const to = range && range[2] ? Math.min(Number(range[2]) + 1, body.length) : body.length;
    const part = body.subarray(from, to);
    const headers: http.OutgoingHttpHeaders = {
      'content-type': 'application/octet-stream',
      'content-length': part.length,
      'last-modified': MODIFIED.toUTCString(),
      etag: `"${etag(key)}"`,
    };
    if (range) headers['content-range'] = `bytes ${from}-${to - 1}/${body.length}`;
    res.writeHead(range ? 206 : 200, headers).end(req.method === 'HEAD' ? undefined : part);
  }

  async stop() {
    this.server.closeAllConnections();
    await new Promise((r) => this.server.close(r));
  }
}

/** Every object's time: a listing source reads what it has not read, whenever that was written. */
const MODIFIED = new Date('2026-01-01T00:00:00Z');

const etag = (key: string) => Buffer.from(key).toString('hex').slice(0, 32);
const escape = (s: string) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
const error = (code: string) =>
  `<?xml version="1.0" encoding="UTF-8"?><Error><Code>${code}</Code></Error>`;

/** A value as an Avro `long`: zigzag, then seven bits a byte. */
function long(n: number): Buffer {
  let z = BigInt.asUintN(64, (BigInt(n) << 1n) ^ (BigInt(n) >> 63n));
  const out: number[] = [];
  while (z > 0x7fn) {
    out.push(Number(z & 0x7fn) | 0x80);
    z >>= 7n;
  }
  out.push(Number(z));
  return Buffer.from(out);
}

const bytes = (b: Buffer) => Buffer.concat([long(b.length), b]);
const string = (s: string) => bytes(Buffer.from(s));

/** An Avro value of the schema `avroFile` is given, encoded as the file holds it. */
export const avro = {
  long,
  string,
  double: (n: number) => {
    const b = Buffer.alloc(8);
    b.writeDoubleLE(n);
    return b;
  },
  boolean: (v: boolean) => Buffer.from([v ? 1 : 0]),
  /** An array of one block, then the empty block that ends it. */
  array: (items: Buffer[]) =>
    items.length ? Buffer.concat([long(items.length), ...items, long(0)]) : long(0),
};

/** An Avro object container file of `schema`, uncompressed, holding `records`, each already encoded. */
export function avroFile(schema: object, records: Buffer[]): Buffer {
  const sync = Buffer.from('smeltr-gui-sync!');
  const meta = Buffer.concat([
    long(2),
    string('avro.schema'),
    bytes(Buffer.from(JSON.stringify(schema))),
    string('avro.codec'),
    bytes(Buffer.from('null')),
    long(0),
  ]);
  const block = Buffer.concat(records);
  return Buffer.concat([
    Buffer.from('Obj\x01', 'latin1'),
    meta,
    sync,
    long(records.length),
    long(block.length),
    block,
    sync,
  ]);
}
