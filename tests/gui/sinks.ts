// Destinations a test stands up in its own process, so rows can flow without Elasticsearch running.
import http from 'node:http';
import type net from 'node:net';

/** How the fake answers a bulk request. `accept` indexes every document, `refuse` rejects every document for
 * good (a mapping conflict, as a real cluster reports one), `down` drops the connection unanswered, as a cluster that
 * cannot be reached. A 503 would not do: it is a busy cluster, sent again as throttled. */
export type SinkMode = 'accept' | 'refuse' | 'down';

/** A fake Elasticsearch on loopback that answers the Bulk API (`src/io/elastic.rs`) and counts what it took. */
export class FakeElasticsearch {
  /** Documents answered with success. */
  indexed = 0;
  /** Bulk requests seen, whatever the answer. */
  requests = 0;
  /** The documents answered with success, in the order they came. */
  documents: Record<string, unknown>[] = [];

  private constructor(
    private server: http.Server,
    readonly url: string,
    public mode: SinkMode,
  ) {}

  static async start(mode: SinkMode = 'accept'): Promise<FakeElasticsearch> {
    let sink: FakeElasticsearch;
    const server = http.createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on('data', (c) => chunks.push(c));
      req.on('end', () => sink.answer(req, Buffer.concat(chunks).toString(), res));
    });
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
    const { port } = server.address() as net.AddressInfo;
    sink = new FakeElasticsearch(server, `http://127.0.0.1:${port}`, mode);
    return sink;
  }

  private answer(req: http.IncomingMessage, body: string, res: http.ServerResponse) {
    if (req.method !== 'POST' || !req.url?.startsWith('/_bulk')) {
      res.writeHead(200, { 'content-type': 'application/json' }).end('{}');
      return;
    }
    this.requests++;
    // An action line and a document line per document.
    const lines = body.split('\n').filter((l) => l.trim() !== '');
    const docs = Math.floor(lines.length / 2);
    if (this.mode === 'down') {
      req.socket.destroy();
      return;
    }
    if (this.mode === 'refuse') {
      const item = {
        index: {
          status: 400,
          error: { type: 'mapper_parsing_exception', reason: 'failed to parse field [value]' },
        },
      };
      res
        .writeHead(200, { 'content-type': 'application/json' })
        .end(JSON.stringify({ errors: true, items: Array.from({ length: docs }, () => item) }));
      return;
    }
    this.indexed += docs;
    for (let i = 1; i < lines.length; i += 2) this.documents.push(JSON.parse(lines[i]));
    res.writeHead(200, { 'content-type': 'application/json' }).end('{"errors":false}');
  }

  async stop() {
    this.server.closeAllConnections();
    await new Promise((r) => this.server.close(r));
  }
}
