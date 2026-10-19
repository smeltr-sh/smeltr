// An OpenID provider in the test process, as `tests/gate-tests.rs` has one: discovery, its key, an authorize
// endpoint that signs in `person` at once, and a token endpoint that holds to PKCE and the client's secret. It
// stands in for the provider a person with a role signs in through, since only a provider's groups give roles.
import crypto from 'node:crypto';
import http from 'node:http';
import type { AddressInfo } from 'node:net';

export const CLIENT_ID = 'smeltr';
export const CLIENT_SECRET = 's3cret-value';

/** Who the provider signs in, without asking, the next time it is visited. */
export type Person = { subject: string; name: string; email: string; groups: string[] };

const b64url = (b: Buffer | string) => Buffer.from(b).toString('base64url');

export class Provider {
  person: Person;
  private codes = new Map<string, { nonce: string; challenge: string }>();
  private key = crypto.generateKeyPairSync('ec', { namedCurve: 'P-256' });
  private server: http.Server;
  base = '';

  private constructor(person: Person) {
    this.person = person;
    this.server = http.createServer((req, res) => this.answer(req, res));
  }

  /** A provider on a free loopback port. */
  static async start(person: Person): Promise<Provider> {
    const p = new Provider(person);
    await new Promise<void>((r) => p.server.listen(0, '127.0.0.1', r));
    p.base = `http://127.0.0.1:${(p.server.address() as AddressInfo).port}`;
    return p;
  }

  async stop() {
    this.server.closeAllConnections();
    await new Promise((r) => this.server.close(r));
  }

  private json(res: http.ServerResponse, status: number, body: unknown) {
    res.writeHead(status, { 'content-type': 'application/json' });
    res.end(JSON.stringify(body));
  }

  private async answer(req: http.IncomingMessage, res: http.ServerResponse) {
    const url = new URL(req.url ?? '/', this.base);
    if (url.pathname === '/.well-known/openid-configuration')
      return this.json(res, 200, {
        issuer: this.base,
        authorization_endpoint: `${this.base}/authorize`,
        token_endpoint: `${this.base}/token`,
        jwks_uri: `${this.base}/jwks`,
        response_types_supported: ['code'],
        code_challenge_methods_supported: ['S256'],
        id_token_signing_alg_values_supported: ['ES256'],
      });
    if (url.pathname === '/jwks') {
      const { x, y } = this.key.publicKey.export({ format: 'jwk' });
      return this.json(res, 200, {
        keys: [{ kty: 'EC', crv: 'P-256', kid: 'k1', use: 'sig', alg: 'ES256', x, y }],
      });
    }
    if (url.pathname === '/authorize') {
      const q = url.searchParams;
      const code = `code-${this.codes.size}-${crypto.randomUUID()}`;
      this.codes.set(code, {
        nonce: q.get('nonce') ?? '',
        challenge: q.get('code_challenge') ?? '',
      });
      const back = new URL(q.get('redirect_uri')!);
      back.searchParams.set('code', code);
      back.searchParams.set('state', q.get('state') ?? '');
      res.writeHead(302, { location: back.toString() });
      return res.end();
    }
    if (url.pathname === '/token' && req.method === 'POST') {
      let body = '';
      for await (const chunk of req) body += chunk;
      const form = new URLSearchParams(body);
      const basic = (req.headers.authorization ?? '').replace(/^Basic /, '');
      const proved =
        form.get('client_secret') === CLIENT_SECRET ||
        Buffer.from(basic, 'base64').toString() === `${CLIENT_ID}:${CLIENT_SECRET}`;
      const refused = (why: string) =>
        this.json(res, 400, { error: 'invalid_grant', error_description: why });
      if (!proved) return refused('the client did not prove itself');
      const issued = this.codes.get(form.get('code') ?? '');
      if (!issued) return refused('unknown code');
      this.codes.delete(form.get('code')!);
      const digest = crypto
        .createHash('sha256')
        .update(form.get('code_verifier') ?? '')
        .digest();
      if (b64url(digest) !== issued.challenge) return refused('PKCE');
      return this.json(res, 200, {
        access_token: 'a',
        token_type: 'Bearer',
        id_token: this.idToken(issued.nonce),
      });
    }
    res.writeHead(404);
    res.end();
  }

  private idToken(nonce: string): string {
    const now = Math.floor(Date.now() / 1000);
    const header = b64url(JSON.stringify({ alg: 'ES256', kid: 'k1', typ: 'JWT' }));
    const claims = b64url(
      JSON.stringify({
        iss: this.base,
        sub: this.person.subject,
        aud: CLIENT_ID,
        iat: now,
        exp: now + 300,
        name: this.person.name,
        email: this.person.email,
        email_verified: true,
        groups: this.person.groups,
        nonce,
      }),
    );
    const signed = `${header}.${claims}`;
    const signature = crypto.sign('sha256', Buffer.from(signed), {
      key: this.key.privateKey,
      dsaEncoding: 'ieee-p1363',
    });
    return `${signed}.${b64url(signature)}`;
  }
}
