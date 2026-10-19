// A model endpoint in the test process speaking the Anthropic Messages API, as `tests/gate-tests.rs` has one. It
// answers with what a test scripted, in order, then with a short reply, and refuses any key but `MODEL_KEY`, so
// turning AI agents on has an endpoint to check against.
import http from 'node:http';
import type { AddressInfo } from 'node:net';

export const MODEL_KEY = 'sk-model-key';

/** The context the endpoint says each of its models holds. */
export const MODEL_CONTEXT = 1_000_000;

/** The models the endpoint lists, newest first, as Anthropic's Models API writes them. */
export const MODELS = [
  {
    id: 'claude-opus-5-5',
    display_name: 'Claude Opus 5.5',
    max_input_tokens: MODEL_CONTEXT,
    capabilities: {
      effort: {
        supported: true,
        low: { supported: true },
        medium: { supported: true },
        high: { supported: true },
        xhigh: { supported: true },
        max: { supported: true },
      },
    },
  },
  { id: 'claude-opus-5', display_name: 'Claude Opus 5', max_input_tokens: MODEL_CONTEXT },
];

/** One block of a model's answer. */
export type Block =
  { type: 'text'; text: string } | { type: 'tool_use'; id: string; name: string; input: object };

let calls = 0;

/** The agent's call of its one tool, asking the leader `method path`. */
export function request(method: string, path: string, query?: Record<string, string>): Block {
  return { type: 'tool_use', id: `tu_${++calls}`, name: 'request', input: { method, path, query } };
}

/** The agent's answer to the person. */
export function say(text: string): Block {
  return { type: 'text', text };
}

export class FakeModel {
  private server = http.createServer((req, res) => this.respond(req, res));
  base = '';
  private script: Block[][] = [];
  /** The body of each request answered, in order. */
  asked: { messages: { role: string; content: unknown }[] }[] = [];

  /** An endpoint on a free loopback port. */
  static async start(): Promise<FakeModel> {
    const m = new FakeModel();
    await new Promise<void>((r) => m.server.listen(0, '127.0.0.1', r));
    m.base = `http://127.0.0.1:${(m.server.address() as AddressInfo).port}`;
    return m;
  }

  /** Answers the next requests with these, one each, before the short reply. */
  answer(...replies: Block[][]) {
    this.script.push(...replies);
  }

  async stop() {
    this.server.closeAllConnections();
    await new Promise((r) => this.server.close(r));
  }

  private respond(req: http.IncomingMessage, res: http.ServerResponse) {
    const chunks: Buffer[] = [];
    req.on('data', (c: Buffer) => chunks.push(c));
    req.on('end', () => {
      const allowed = req.headers['x-api-key'] === MODEL_KEY;
      const body =
        req.method === 'POST' ? JSON.parse(Buffer.concat(chunks).toString() || '{}') : {};
      res.writeHead(allowed ? 200 : 401, {
        'content-type': allowed && body.stream ? 'text/event-stream' : 'application/json',
      });
      if (!allowed) {
        res.end(JSON.stringify({ type: 'error', error: { message: 'invalid x-api-key' } }));
        return;
      }
      // Anthropic's Models API: the models a person picks from, and the context each holds.
      if (req.method === 'GET' && req.url?.startsWith('/v1/models?')) {
        res.end(JSON.stringify({ data: MODELS, has_more: false }));
        return;
      }
      if (req.method === 'GET') {
        res.end(
          JSON.stringify({
            type: 'model',
            id: req.url?.split('/').pop(),
            max_input_tokens: MODEL_CONTEXT,
          }),
        );
        return;
      }
      this.asked.push(body);
      const content = this.script.shift() ?? [{ type: 'text', text: 'ok' }];
      const stop_reason = content.some((b) => b.type === 'tool_use') ? 'tool_use' : 'end_turn';
      if (body.stream) {
        res.end(streamed(content, stop_reason));
        return;
      }
      res.end(
        JSON.stringify({ content, stop_reason, usage: { input_tokens: 5, output_tokens: 1 } }),
      );
    });
  }
}

/** An answer as Anthropic streams it: each block opened, its text in two pieces or its input whole, and closed. */
function streamed(content: Block[], stop_reason: string): string {
  const events: object[] = [
    {
      type: 'message_start',
      message: { role: 'assistant', content: [], usage: { input_tokens: 5, output_tokens: 0 } },
    },
  ];
  content.forEach((block, index) => {
    if (block.type === 'text') {
      const half = Math.floor(block.text.length / 2);
      events.push({
        type: 'content_block_start',
        index,
        content_block: { type: 'text', text: '' },
      });
      for (const text of [block.text.slice(0, half), block.text.slice(half)]) {
        events.push({ type: 'content_block_delta', index, delta: { type: 'text_delta', text } });
      }
    } else {
      events.push({ type: 'content_block_start', index, content_block: { ...block, input: {} } });
      events.push({
        type: 'content_block_delta',
        index,
        delta: { type: 'input_json_delta', partial_json: JSON.stringify(block.input) },
      });
    }
    events.push({ type: 'content_block_stop', index });
  });
  events.push({ type: 'message_delta', delta: { stop_reason }, usage: { output_tokens: 1 } });
  events.push({ type: 'message_stop' });
  return events
    .map((e) => `event: ${(e as { type: string }).type}\ndata: ${JSON.stringify(e)}\n\n`)
    .join('');
}
