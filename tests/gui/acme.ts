// A certificate authority over ACME in the test process, as `oidc.ts` holds an OpenID provider: a directory, an
// account, an order for the leader's names, each name proved by connecting back to the leader over TLS-ALPN-01 or
// HTTP-01, and a certificate signed under the authority's own root. The root is minted at start, and again on
// `rotateRoot`, which is what a renewal that moves the fleet to a new root is ordered against.
//
// Node builds no certificate of its own, so the few DER structures a root and a leaf need are written here over
// its crypto: an ECDSA P-256 key, a SHA-256 signature, and the base64url the protocol speaks.
import crypto from 'node:crypto';
import http from 'node:http';
import https from 'node:https';
import type { AddressInfo } from 'node:net';
import tls from 'node:tls';

const b64url = (b: Buffer | string) => Buffer.from(b).toString('base64url');
const sha256 = (b: Buffer | string) => crypto.createHash('sha256').update(b).digest();

// --- DER ---------------------------------------------------------------------------------------------------------

function der(tag: number, body: Buffer): Buffer {
  const n = body.length;
  const length =
    n < 0x80
      ? Buffer.from([n])
      : n < 0x100
        ? Buffer.from([0x81, n])
        : Buffer.from([0x82, n >> 8, n & 0xff]);
  return Buffer.concat([Buffer.from([tag]), length, body]);
}
const seq = (...parts: Buffer[]) => der(0x30, Buffer.concat(parts));
const set = (...parts: Buffer[]) => der(0x31, Buffer.concat(parts));
const int = (n: number) => der(0x02, Buffer.from([n]));
const octets = (b: Buffer) => der(0x04, b);
const bool = (v: boolean) => der(0x01, Buffer.from([v ? 0xff : 0]));
const utf8 = (s: string) => der(0x0c, Buffer.from(s, 'utf8'));
const explicit = (n: number, b: Buffer) => der(0xa0 | n, b);

function oid(dotted: string): Buffer {
  const [a, b, ...rest] = dotted.split('.').map(Number);
  const bytes = [40 * a + b];
  for (const part of rest) {
    const chunk: number[] = [part & 0x7f];
    for (let v = part >> 7; v > 0; v >>= 7) chunk.unshift((v & 0x7f) | 0x80);
    bytes.push(...chunk);
  }
  return der(0x06, Buffer.from(bytes));
}

const ECDSA_SHA256 = seq(oid('1.2.840.10045.4.3.2'));
const COMMON_NAME = oid('2.5.4.3');
const BASIC_CONSTRAINTS = oid('2.5.29.19');
const SUBJECT_ALT_NAME = oid('2.5.29.17');
const EXTENDED_KEY_USAGE = oid('2.5.29.37');
/** The Extended Key Usage purposes a leaf can name. */
const PURPOSES = { server: oid('1.3.6.1.5.5.7.3.1'), client: oid('1.3.6.1.5.5.7.3.2') };

function name(cn: string): Buffer {
  return seq(set(seq(COMMON_NAME, utf8(cn))));
}

function utcTime(d: Date): Buffer {
  const s = d.toISOString().replace(/[-:T]/g, '').slice(2, 14) + 'Z';
  return der(0x17, Buffer.from(s, 'ascii'));
}

/** The four octets of `name` when it is an IPv4 address. */
function ipv4(name: string): number[] | null {
  const ip = name.match(/^(\d+)\.(\d+)\.(\d+)\.(\d+)$/);
  return ip ? ip.slice(1).map(Number) : null;
}

/** The server name a validator asks for `name` by: an address in its reverse-DNS form (RFC 8738). */
function askedAs(name: string): string {
  const ip = ipv4(name);
  return ip ? `${ip.reverse().join('.')}.in-addr.arpa` : name;
}

/** A general name: an IP address as its bytes, anything else as a DNS name. */
function altName(n: string): Buffer {
  const ip = ipv4(n);
  return ip ? der(0x87, Buffer.from(ip)) : der(0x82, Buffer.from(n, 'ascii'));
}

/** An identifier as the protocol names it. */
function identifier(n: string) {
  return { type: ipv4(n) ? 'ip' : 'dns', value: n };
}

/** A fresh id for an account, an order or an authorization. */
const id = (): string => b64url(crypto.randomBytes(6));

function extension(kind: Buffer, critical: boolean, value: Buffer): Buffer {
  return seq(kind, ...(critical ? [bool(true)] : []), octets(value));
}

type Key = crypto.KeyObject;

/** A certificate for `subjectKey`, signed by `issuerKey` as `issuer`: a root when `names` is empty, else a leaf
 * carrying them, its Extended Key Usage naming `purposes` where any are given. */
function certificate(
  cn: string,
  subjectKey: Key,
  issuer: string,
  issuerKey: Key,
  names: string[],
  purposes: (keyof typeof PURPOSES)[] = [],
): Buffer {
  const now = Date.now();
  const extensions = [
    names.length === 0
      ? extension(BASIC_CONSTRAINTS, true, seq(bool(true)))
      : extension(SUBJECT_ALT_NAME, false, seq(...names.map(altName))),
    ...(purposes.length > 0
      ? [extension(EXTENDED_KEY_USAGE, false, seq(...purposes.map((p) => PURPOSES[p])))]
      : []),
  ];
  const tbs = seq(
    explicit(0, int(2)),
    // A serial that is positive and minimally encoded: its first byte is neither zero nor high.
    der(0x02, crypto.randomBytes(8).map((b, i) => (i === 0 ? b & 0x7f || 1 : b)) as Buffer),
    ECDSA_SHA256,
    name(issuer),
    // A root outlives what it signs: ten years to a leaf's ninety days.
    seq(
      utcTime(new Date(now - 60_000)),
      utcTime(new Date(now + (names.length === 0 ? 3650 : 90) * 24 * 60 * 60 * 1000)),
    ),
    name(cn),
    (subjectKey.type === 'public' ? subjectKey : crypto.createPublicKey(subjectKey)).export({
      type: 'spki',
      format: 'der',
    }) as Buffer,
    explicit(3, seq(...extensions)),
  );
  const signature = crypto.sign('sha256', tbs, { key: issuerKey, dsaEncoding: 'der' });
  return seq(tbs, ECDSA_SHA256, der(0x03, Buffer.concat([Buffer.from([0]), signature])));
}

function pem(derBytes: Buffer, label = 'CERTIFICATE'): string {
  const lines = derBytes.toString('base64').match(/.{1,64}/g) ?? [];
  return `-----BEGIN ${label}-----\n${lines.join('\n')}\n-----END ${label}-----\n`;
}

/** The public key a certification request asks a certificate for: the third element of its info. */
function requestedKey(csr: Buffer): Key {
  let at = 0;
  const read = (): { tag: number; body: Buffer; whole: Buffer } => {
    const start = at;
    const tag = csr[at++];
    let len = csr[at++];
    if (len & 0x80) {
      const n = len & 0x7f;
      len = 0;
      for (let i = 0; i < n; i++) len = (len << 8) | csr[at++];
    }
    const body = csr.subarray(at, at + len);
    at += len;
    return { tag, body, whole: csr.subarray(start, at) };
  };
  read(); // CertificationRequest
  at = 0;
  const outer = read();
  csr = outer.body;
  at = 0;
  const info = read();
  csr = info.body;
  at = 0;
  read(); // version
  read(); // subject
  const spki = read().whole;
  return crypto.createPublicKey({ key: spki, format: 'der', type: 'spki' });
}

/** A leaf for `names` whose Extended Key Usage names `purpose` alone, under a root of its own: the certificate,
 * its private key and that root, PEM. The leader issues its own for servers and clients both. */
export function issuedFor(names: string[], purpose: keyof typeof PURPOSES) {
  const root = crypto.generateKeyPairSync('ec', { namedCurve: 'P-256' }).privateKey;
  const { privateKey, publicKey } = crypto.generateKeyPairSync('ec', { namedCurve: 'P-256' });
  return {
    certificate: pem(certificate(names[0], publicKey, 'Stand-in Root', root, names, [purpose])),
    privateKey: privateKey.export({ type: 'pkcs8', format: 'pem' }) as string,
    authority: pem(certificate('Stand-in Root', root, 'Stand-in Root', root, [])),
  };
}

// --- The authority -----------------------------------------------------------------------------------------------

type Order = {
  id: string;
  names: string[];
  status: 'pending' | 'ready' | 'valid';
  /** Each authorization by its id: the name, its challenges' tokens, and whether one was proved. */
  authorizations: { id: string; name: string; token: string; status: 'pending' | 'valid' }[];
  certificate: string | null;
};

type Account = { id: string; thumbprint: string };

/** Where the authority reaches the leader it validates: its TLS port, and the port it answers HTTP-01 on. */
export type Reaches = { tlsPort: number; httpPort: number };

export class Authority {
  /** The directory URL the leader is pointed at. */
  directory = '';
  /** Every root minted so far, oldest first, as one PEM: what the leader's authority setting is given. */
  roots: string[] = [];
  /** How many certificates were issued. */
  issued = 0;
  private rootKey!: Key;
  private rootName = '';
  private server: https.Server;
  private base = '';
  private accounts = new Map<string, Account>();
  private orders = new Map<string, Order>();
  private reaches: Reaches;

  private constructor(reaches: Reaches) {
    this.reaches = reaches;
    this.mint();
    this.server = https.createServer(this.serving(), (req, res) => {
      this.answer(req, res).catch((e) => this.problem(res, 500, 'serverInternal', String(e)));
    });
  }

  /** An authority on a free loopback port, serving under its own root. */
  static async start(reaches: Reaches): Promise<Authority> {
    const a = new Authority(reaches);
    await new Promise<void>((r) => a.server.listen(0, '127.0.0.1', r));
    a.base = `https://127.0.0.1:${(a.server.address() as AddressInfo).port}`;
    a.directory = `${a.base}/dir`;
    return a;
  }

  async stop() {
    this.server.closeAllConnections();
    await new Promise((r) => this.server.close(r));
  }

  /** A new root. Everything issued from now on, and the directory itself, is under it. */
  rotateRoot() {
    this.mint();
    this.server.setSecureContext(this.serving());
  }

  private mint() {
    this.rootKey = crypto.generateKeyPairSync('ec', { namedCurve: 'P-256' }).privateKey;
    this.rootName = `Stand-in Root ${this.roots.length + 1}`;
    this.roots.push(pem(certificate(this.rootName, this.rootKey, this.rootName, this.rootKey, [])));
  }

  /** The directory's own certificate, for 127.0.0.1 under the current root. */
  private serving(): tls.SecureContextOptions {
    const { privateKey, publicKey } = crypto.generateKeyPairSync('ec', { namedCurve: 'P-256' });
    return {
      key: privateKey.export({ type: 'pkcs8', format: 'pem' }) as string,
      cert: pem(certificate('127.0.0.1', publicKey, this.rootName, this.rootKey, ['127.0.0.1'])),
    };
  }

  private nonce(res: http.ServerResponse) {
    res.setHeader('replay-nonce', b64url(crypto.randomBytes(16)));
  }

  /** Every answer carries a fresh nonce. */
  private send(res: http.ServerResponse, status: number, type: string, body: string, headers = {}) {
    this.nonce(res);
    res.writeHead(status, { 'content-type': type, ...headers });
    res.end(body);
  }

  private json(res: http.ServerResponse, status: number, body: unknown, headers = {}) {
    this.send(res, status, 'application/json', JSON.stringify(body), headers);
  }

  private problem(res: http.ServerResponse, status: number, type: string, detail: string) {
    const body = { type: `urn:ietf:params:acme:error:${type}`, detail };
    this.send(res, status, 'application/problem+json', JSON.stringify(body));
  }

  /** The JWS every request but the directory's carries: who signed it, and what it says. */
  private async jws(
    req: http.IncomingMessage,
  ): Promise<{ account: Account | null; jwk: object | null; payload: any }> {
    const chunks: Buffer[] = [];
    for await (const c of req) chunks.push(c as Buffer);
    const { protected: header, payload } = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    const head = JSON.parse(Buffer.from(header, 'base64url').toString('utf8'));
    const body =
      payload === '' ? null : JSON.parse(Buffer.from(payload, 'base64url').toString('utf8'));
    const account = head.kid
      ? (this.accounts.get(String(head.kid).split('/').pop()!) ?? null)
      : null;
    return { account, jwk: head.jwk ?? null, payload: body };
  }

  private thumbprint(jwk: { crv: string; kty: string; x: string; y: string }): string {
    return b64url(sha256(JSON.stringify({ crv: jwk.crv, kty: jwk.kty, x: jwk.x, y: jwk.y })));
  }

  private orderJson(o: Order) {
    return {
      status: o.status,
      identifiers: o.names.map(identifier),
      authorizations: o.authorizations.map((a) => `${this.base}/authz/${a.id}`),
      finalize: `${this.base}/finalize/${o.id}`,
      ...(o.certificate ? { certificate: `${this.base}/cert/${o.id}` } : {}),
    };
  }

  private async answer(req: http.IncomingMessage, res: http.ServerResponse) {
    const url = new URL(req.url ?? '/', this.base);
    const [, kind, at] = url.pathname.split('/');
    if (kind === 'dir')
      return this.json(res, 200, {
        newNonce: `${this.base}/new-nonce`,
        newAccount: `${this.base}/new-account`,
        newOrder: `${this.base}/new-order`,
        meta: { termsOfService: `${this.base}/terms`, externalAccountRequired: false },
      });
    if (kind === 'new-nonce') {
      this.nonce(res);
      res.writeHead(req.method === 'HEAD' ? 200 : 204);
      return res.end();
    }
    const { account, jwk, payload } = await this.jws(req);
    if (kind === 'new-account') {
      if (!jwk) return this.problem(res, 400, 'malformed', 'no key');
      const made = { id: id(), thumbprint: this.thumbprint(jwk as any) };
      this.accounts.set(made.id, made);
      return this.json(
        res,
        201,
        { status: 'valid', contact: payload?.contact ?? [] },
        {
          location: `${this.base}/acct/${made.id}`,
        },
      );
    }
    if (!account) return this.problem(res, 401, 'accountDoesNotExist', 'no such account');
    if (kind === 'new-order') {
      const names = (payload.identifiers as { value: string }[]).map((i) => i.value.toLowerCase());
      const order: Order = {
        id: id(),
        names,
        status: 'pending',
        authorizations: names.map((n) => ({
          id: id(),
          name: n,
          token: b64url(crypto.randomBytes(16)),
          status: 'pending',
        })),
        certificate: null,
      };
      this.orders.set(order.id, order);
      return this.json(res, 201, this.orderJson(order), {
        location: `${this.base}/order/${order.id}`,
      });
    }
    if (kind === 'order') {
      const order = this.orders.get(at);
      if (!order) return this.problem(res, 404, 'malformed', 'no such order');
      return this.json(res, 200, this.orderJson(order));
    }
    if (kind === 'authz' || kind === 'chall') {
      const [challengeType, authzId] = kind === 'chall' ? at.split('.') : ['', at];
      const order = [...this.orders.values()].find((o) =>
        o.authorizations.some((a) => a.id === authzId),
      );
      const authz = order?.authorizations.find((a) => a.id === authzId);
      if (!order || !authz) return this.problem(res, 404, 'malformed', 'no such authorization');
      if (kind === 'chall') {
        const keyAuthorization = `${authz.token}.${account.thumbprint}`;
        const held =
          challengeType === 'tls-alpn-01'
            ? await this.provedOverTls(authz.name, keyAuthorization)
            : await this.provedOverHttp(authz.name, authz.token, keyAuthorization);
        if (held !== null) return this.problem(res, 400, 'unauthorized', `${authz.name}: ${held}`);
        authz.status = 'valid';
        if (order.authorizations.every((a) => a.status === 'valid')) order.status = 'ready';
      }
      const challenge = (type: string) => ({
        type,
        url: `${this.base}/chall/${type}.${authz.id}`,
        token: authz.token,
        status: authz.status,
      });
      if (kind === 'chall') return this.json(res, 200, challenge(challengeType));
      return this.json(res, 200, {
        status: authz.status,
        identifier: identifier(authz.name),
        challenges: [challenge('tls-alpn-01'), challenge('http-01')],
      });
    }
    if (kind === 'finalize') {
      const order = this.orders.get(at);
      if (!order) return this.problem(res, 404, 'malformed', 'no such order');
      if (order.status !== 'ready')
        return this.problem(res, 403, 'orderNotReady', 'the names are not all proved');
      const key = requestedKey(Buffer.from(payload.csr, 'base64url'));
      order.certificate = pem(
        certificate(order.names[0], key, this.rootName, this.rootKey, order.names),
      );
      order.status = 'valid';
      this.issued += 1;
      return this.json(res, 200, this.orderJson(order));
    }
    if (kind === 'cert') {
      const order = this.orders.get(at);
      if (!order?.certificate) return this.problem(res, 404, 'malformed', 'nothing issued');
      return this.send(res, 200, 'application/pem-certificate-chain', order.certificate);
    }
    this.problem(res, 404, 'malformed', `nothing at ${url.pathname}`);
  }

  /** Connects to the leader as a validator does, asking for `acme-tls/1` under `name`, and checks the proof it
   * is shown carries the key authorization's digest. The reason it does not, or `null`. */
  private provedOverTls(name: string, keyAuthorization: string): Promise<string | null> {
    return new Promise((resolve) => {
      const socket = tls.connect({
        host: '127.0.0.1',
        port: this.reaches.tlsPort,
        servername: askedAs(name),
        ALPNProtocols: ['acme-tls/1'],
        rejectUnauthorized: false,
      });
      socket.once('secureConnect', () => {
        const shown = socket.getPeerCertificate();
        const digest = sha256(keyAuthorization);
        const proved =
          socket.alpnProtocol === 'acme-tls/1' && shown.raw.includes(digest)
            ? null
            : `no proof for ${name} over acme-tls/1`;
        socket.destroy();
        resolve(proved);
      });
      socket.once('error', (e) => resolve(`could not connect: ${e.message}`));
    });
  }

  /** Fetches the token from the leader as a validator does, and checks what comes back. */
  private provedOverHttp(
    name: string,
    token: string,
    keyAuthorization: string,
  ): Promise<string | null> {
    return new Promise((resolve) => {
      const req = http.get(
        {
          host: '127.0.0.1',
          port: this.reaches.httpPort,
          path: `/.well-known/acme-challenge/${token}`,
          headers: { host: name },
        },
        (res) => {
          const chunks: Buffer[] = [];
          res.on('data', (c) => chunks.push(c));
          res.on('end', () => {
            const body = Buffer.concat(chunks).toString('utf8');
            resolve(
              res.statusCode === 200 && body === keyAuthorization
                ? null
                : `the token was answered with ${res.statusCode}`,
            );
          });
        },
      );
      req.once('error', (e) => resolve(`could not connect: ${e.message}`));
    });
  }
}
