// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { flushSync, mount, unmount } from 'svelte';
import type { Question } from './Confirm.svelte';

// The page asks through the app's one dialog, which App mounts; here each test answers it.
const ask = vi.hoisted(() => vi.fn<(q: Question) => Promise<boolean | null>>());
vi.mock('./Confirm.svelte', () => ({ ask }));

import CertificatesSettings from './CertificatesSettings.svelte';

let page: ReturnType<typeof mount> | undefined;
let target: HTMLDivElement;
let posts: string[];
let deletes: string[];
let puts: Record<string, unknown>[];

const YEAR = 365 * 24 * 60 * 60 * 1000;

function described(subject: string) {
  return {
    subject,
    fingerprint: 'ab'.repeat(32),
    names: ['leader.example'],
    notBeforeMillis: Date.now(),
    notAfterMillis: Date.now() + YEAR,
  };
}

const CERTIFICATES = {
  servingTls: true,
  authority: described('smeltr authority'),
  served: described('leader.example'),
  supplied: false,
  source: 'own',
  entry: null,
  names: ['localhost', '127.0.0.1', 'leader.example'],
  workerName: null,
  restartRequired: false,
  unserved: null,
  rotation: {
    incoming: described('smeltr authority 2'),
    startedMillis: Date.now(),
    deadlineMillis: Date.now() + YEAR,
    latestDeadlineMillis: Date.now() + 2 * YEAR,
    installs: 'nextKey',
    source: 'own',
    notFollowing: [],
    workers: [],
  },
};

/** Nothing ordered yet. */
const ACME = {
  enabled: false,
  directory: '',
  contact: '',
  names: [],
  challenge: 'tls-alpn-01',
  authority: '',
  eabKeyId: '',
  eabHmacSet: false,
  dnsProvider: 'cloudflare',
  cloudflareTokenSet: false,
  httpPort: 80,
  notAfterMillis: null,
  renewAfterMillis: null,
  lastAttemptMillis: null,
  lastError: null,
  orderWithinSecs: { 'tls-alpn-01': 120, 'http-01': 120, 'dns-01': 900 },
};

/** The certificate list the leader's chooser offers. */
const LISTED = [
  {
    name: 'corporate',
    description: '',
    versions: [{ version: 1, setAt: 0, usable: true }],
    chain: [],
    served: false,
    forServers: true,
    forClients: true,
  },
];

function json(body: unknown) {
  return new Response(JSON.stringify(body), { headers: { 'content-type': 'application/json' } });
}

async function show(
  certificates: object = CERTIFICATES,
  ready = 'Cut over now',
  acme: object = ACME,
) {
  vi.stubGlobal(
    'fetch',
    vi.fn(async (url: string, init?: RequestInit) => {
      if (init?.method === 'POST') posts.push(url);
      if (init?.method === 'DELETE') deletes.push(url);
      if (init?.method === 'PUT') puts.push(JSON.parse(String(init.body)));
      if (url.startsWith('/api/settings/tls/acme')) return json(acme);
      if (url.startsWith('/api/settings/tls')) return json(certificates);
      if (url.startsWith('/api/certificates'))
        return json({ ok: true, errors: [], certificates: LISTED });
      throw new Error(`unexpected ${url}`);
    }),
  );
  page = mount(CertificatesSettings, { target });
  await vi.waitFor(() => expect(button(ready)).toBeDefined());
}

/** The chooser's option, by its label. */
function option(label: string): HTMLButtonElement | undefined {
  return [...target.querySelectorAll('[role="group"] button')].find((b) =>
    b.textContent?.startsWith(label),
  ) as HTMLButtonElement | undefined;
}

function button(text: string): HTMLButtonElement | undefined {
  return [...target.querySelectorAll('button')].find((b) => b.textContent?.trim() === text);
}

/** Lets a click's awaited question settle, answered or not. */
async function settle() {
  await new Promise((r) => setTimeout(r, 0));
  flushSync();
}

beforeEach(() => {
  posts = [];
  deletes = [];
  puts = [];
  ask.mockReset();
  target = document.createElement('div');
  document.body.append(target);
});

afterEach(() => {
  if (page) unmount(page);
  page = undefined;
  target.remove();
  vi.unstubAllGlobals();
});

describe('the certificate actions', () => {
  it('cuts over only once it is confirmed', async () => {
    await show();
    ask.mockResolvedValueOnce(false);
    button('Cut over now')!.click();
    await settle();
    expect(ask).toHaveBeenCalledTimes(1);
    expect(ask.mock.calls[0][0].body).toContain('can no longer connect');
    expect(posts).toEqual([]);

    ask.mockResolvedValueOnce(true);
    button('Cut over now')!.click();
    await vi.waitFor(() => expect(posts).toEqual(['/api/settings/tls/rotation/cutover']));
  });
});

describe('the source', () => {
  const UPLOADED = {
    ...CERTIFICATES,
    supplied: true,
    source: 'upload',
    entry: 'theirs',
    rotation: null,
  };

  it('opens on the source in force and marks it', async () => {
    await show(UPLOADED, 'Serve this certificate');
    expect(option('Uploaded')!.getAttribute('aria-pressed')).toBe('true');
    expect(option('Uploaded')!.getAttribute('aria-current')).toBe('true');
    expect(option('Leader issued')!.getAttribute('aria-current')).toBeNull();
  });

  it('changes nothing until the chosen source is acted on', async () => {
    await show(UPLOADED, 'Serve this certificate');
    option('Leader issued')!.click();
    flushSync();
    expect(button('Switch to Leader issued')).toBeDefined();
    expect(button('Serve this certificate')).toBeUndefined();
    expect(puts).toEqual([]);

    button('Switch to Leader issued')!.click();
    await vi.waitFor(() => expect(puts).toEqual([{ certificate: 'leader' }]));
  });

  it('offers rotation only under Leader issued', async () => {
    await show({ ...CERTIFICATES, rotation: null }, 'Rotate authority');
    expect(target.textContent).toContain('Rotate authority');
    option('Uploaded')!.click();
    flushSync();
    expect(target.textContent).not.toContain('Rotate authority');
  });

  it('shows a switch in progress whichever source is looked at, and keeps only the upload form', async () => {
    const switching = {
      ...CERTIFICATES,
      rotation: {
        ...CERTIFICATES.rotation,
        incoming: described('their authority'),
        installs: 'supplied',
        source: 'upload',
      },
    };
    await show(switching, 'Abandon switch');
    expect(target.textContent).toContain('Switch to Uploaded in progress');
    expect(button('Serve this certificate')).toBeUndefined();
    expect(option('Leader issued')!.getAttribute('aria-current')).toBe('true');
    option('Uploaded')!.click();
    flushSync();
    expect(target.textContent).toContain('Switch to Uploaded in progress');
    // A certificate under the root in force is a renewal, served at once whatever is open.
    expect(button('Serve this certificate')).toBeDefined();
  });

  it('lists each worker a switch waits for, and whether it holds the incoming authority', async () => {
    const switching = {
      ...CERTIFICATES,
      supplied: true,
      source: 'upload',
      entry: 'theirs',
      rotation: {
        ...CERTIFICATES.rotation,
        installs: 'supplied',
        source: 'own',
        workers: [
          { id: 'w-1', holdsIncoming: true },
          { id: 'w-2', holdsIncoming: false },
        ],
      },
    };
    await show(switching, 'Abandon switch');
    expect(target.textContent).toContain('1 of 2 hold it');
    const rows = [...target.querySelectorAll('.workers li')].map((li) => li.textContent);
    expect(rows[0]).toContain('w-1');
    expect(rows[0]).toContain('holds it');
    expect(rows[1]).toContain('w-2');
    expect(rows[1]).toContain('waiting');
  });

  it('names the workers a switch does not wait for', async () => {
    const switching = {
      ...CERTIFICATES,
      supplied: true,
      source: 'upload',
      entry: 'theirs',
      rotation: {
        ...CERTIFICATES.rotation,
        installs: 'supplied',
        source: 'own',
        notFollowing: ['ops-1', 'ops-2'],
      },
    };
    await show(switching, 'Abandon switch');
    expect(target.textContent).toContain('Switch to Leader issued in progress');
    expect(target.textContent).toContain('Not waited for: ops-1, ops-2');
  });

  it('says that an upload under another root is published before it is served', async () => {
    let answered: object = { ...CERTIFICATES, rotation: null };
    vi.stubGlobal(
      'fetch',
      vi.fn(async (url: string, init?: RequestInit) => {
        if (init?.method === 'PUT') {
          puts.push(JSON.parse(String(init.body)));
          answered = {
            ...CERTIFICATES,
            rotation: { ...CERTIFICATES.rotation, installs: 'supplied', source: 'upload' },
          };
        }
        if (url.startsWith('/api/settings/tls/acme')) return json(ACME);
        if (url.startsWith('/api/settings/tls')) return json(answered);
        if (url.startsWith('/api/certificates'))
          return json({ ok: true, errors: [], certificates: LISTED });
        throw new Error(`unexpected ${url}`);
      }),
    );
    page = mount(CertificatesSettings, { target });
    await vi.waitFor(() => expect(button('Rotate authority')).toBeDefined());
    option('Uploaded')!.click();
    flushSync();
    const chooser = target.querySelector('select')!;
    await vi.waitFor(() => expect(chooser.options).toHaveLength(2));
    chooser.value = 'corporate';
    chooser.dispatchEvent(new Event('change', { bubbles: true }));
    flushSync();
    button('Serve this certificate')!.click();
    await vi.waitFor(() =>
      expect(target.textContent).toContain(
        'the incoming authority is being published to the fleet',
      ),
    );
    expect(target.textContent).toContain('Switch to Uploaded in progress');
    expect(puts).toEqual([{ certificate: 'corporate' }]);
  });

  it('keeps a rotation already open in reach while an uploaded certificate is in force', async () => {
    await show(
      { ...CERTIFICATES, supplied: true, source: 'upload', entry: 'theirs' },
      'Serve this certificate',
    );
    option('Leader issued')!.click();
    flushSync();
    expect(button('Abandon rotation')).toBeDefined();
    expect(target.textContent).not.toContain('Rotate authority');
  });
});

describe('uploading during a switch', () => {
  it('says the cutover replaces what is uploaded', async () => {
    const switching = {
      ...CERTIFICATES,
      rotation: { ...CERTIFICATES.rotation, installs: 'supplied', source: 'acme' },
    };
    await show(switching, 'Cut over now');
    option('Uploaded')!.click();
    flushSync();
    expect(target.textContent).toContain(
      'A switch to ACME is open. When it cuts over, it replaces this certificate.',
    );
  });
});

describe('a certificate a start could not serve', () => {
  it('says why, and that the leader serves its own', async () => {
    const why =
      'This leader is serving a "Leader issued" certificate instead of "theirs", which cannot be served because the certificate has already expired. Choose a certificate under Source.';
    await show(
      {
        ...CERTIFICATES,
        supplied: false,
        source: 'upload',
        entry: 'theirs',
        unserved: why,
        rotation: null,
      },
      'Serve this certificate',
    );
    expect(target.textContent).toContain(why);
    expect(target.textContent).not.toContain('Restart the leader');

    option('Leader issued')!.click();
    flushSync();
    expect(button('Rotate authority')).toBeUndefined();
    button('Switch to Leader issued')!.click();
    await vi.waitFor(() => expect(puts).toEqual([{ certificate: 'leader' }]));
  });
});

describe('ACME', () => {
  const ORDERED = {
    ...ACME,
    enabled: true,
    directory: 'https://acme-v02.api.letsencrypt.org/directory',
    contact: 'ops@example.com',
    names: ['leader.example'],
    notAfterMillis: Date.now() + YEAR,
    renewAfterMillis: Date.now() + YEAR / 2,
  };
  const IN_FORCE = {
    ...CERTIFICATES,
    supplied: true,
    source: 'acme',
    entry: 'acme',
    rotation: null,
  };

  it('orders only once the terms are agreed to, and fills the names from the certificate', async () => {
    await show({ ...CERTIFICATES, rotation: null }, 'Rotate authority');
    option('ACME')!.click();
    flushSync();
    const get = button('Get certificate')!;
    expect(get.disabled).toBe(true);
    expect(target.querySelector('button[aria-label="remove leader.example"]')).not.toBeNull();
    target.querySelector<HTMLInputElement>('input[type="checkbox"]')!.click();
    flushSync();
    expect(get.disabled).toBe(false);
    expect(target.textContent).toContain('Port 443 on leader.example must reach this leader');
  });

  it('shows the renewal, Save and Renew now with a certificate in force', async () => {
    await show(IN_FORCE, 'Save', ORDERED);
    expect(option('ACME')!.getAttribute('aria-current')).toBe('true');
    expect(target.textContent).toContain('renews');
    expect(button('Renew now')).toBeDefined();
    expect(button('Get certificate')).toBeUndefined();
  });

  it('saves the same names and authority without ordering', async () => {
    await show(IN_FORCE, 'Save', ORDERED);
    button('Save')!.click();
    await vi.waitFor(() => expect(target.textContent).toContain('settings saved'));
    expect(posts).toEqual([]);
  });

  it('orders at once when Save carries other names', async () => {
    await show(IN_FORCE, 'Save', { ...ORDERED, names: ['leader.example', 'old.example'] });
    target.querySelector<HTMLButtonElement>('button[aria-label="remove old.example"]')!.click();
    flushSync();
    button('Save')!.click();
    await vi.waitFor(() => expect(posts).toEqual(['/api/settings/tls/acme/order']));
  });

  it('says an order is running until the leader answers, in place of the last failure', async () => {
    await show(IN_FORCE, 'Save', {
      ...ORDERED,
      challenge: 'dns-01',
      lastAttemptMillis: Date.now(),
      lastError: 'no answer on port 443',
    });
    expect(target.textContent).toContain('no answer on port 443');
    const answered = vi.mocked(fetch).getMockImplementation()!;
    let answer!: () => void;
    vi.mocked(fetch).mockImplementation(async (url, init) => {
      if (!String(url).endsWith('/order')) return answered(url as string, init);
      posts.push(String(url));
      await new Promise<void>((r) => (answer = r));
      return json(IN_FORCE);
    });
    button('Renew now')!.click();
    await vi.waitFor(() => expect(button('Renewing…')?.disabled).toBe(true));
    expect(target.textContent).toContain('This can take up to 15 minutes.');
    expect(target.textContent).not.toContain('no answer on port 443');
    answer();
    await vi.waitFor(() => expect(button('Renew now')?.disabled).toBe(false));
    expect(target.textContent).not.toContain('This can take up to');
  });

  it('says what the last order failed with', async () => {
    await show(IN_FORCE, 'Save', {
      ...ORDERED,
      lastAttemptMillis: Date.now(),
      lastError: 'no answer on port 443',
    });
    expect(target.textContent).toContain('no answer on port 443');
  });

  it('shows a renewal under a new root as the fleet being moved to it', async () => {
    const moving = {
      ...IN_FORCE,
      rotation: { ...CERTIFICATES.rotation, installs: 'supplied', source: 'acme' },
    };
    await show(moving, 'Cut over now', ORDERED);
    expect(target.textContent).toContain('Moving workers to a new root');
    expect(button('Renew now')).toBeDefined();
  });

  it('asks DNS-01 for its provider and token, and for no port', async () => {
    await show(IN_FORCE, 'Save', { ...ORDERED, challenge: 'dns-01' });
    expect(target.textContent).toContain('DNS provider');
    expect(target.querySelector('[aria-label="API token"]')).not.toBeNull();
    expect(target.textContent).not.toContain('must reach this leader');
  });

  it('sends a typed token, and keeps a stored one when none is typed', async () => {
    await show(IN_FORCE, 'Save', { ...ORDERED, challenge: 'dns-01', cloudflareTokenSet: true });
    expect(target.textContent).toContain('Stored');
    button('Save')!.click();
    await vi.waitFor(() => expect(puts).toHaveLength(1));
    expect(puts[0]).toMatchObject({
      challenge: 'dns-01',
      dnsProvider: 'cloudflare',
      cloudflareToken: null,
    });
    await vi.waitFor(() => expect(target.textContent).toContain('settings saved'));

    target.querySelector<HTMLButtonElement>('button[aria-label="replace the API token"]')!.click();
    flushSync();
    const input = target.querySelector<HTMLInputElement>('input[aria-label="API token"]')!;
    input.value = 'cf-token';
    input.dispatchEvent(new Event('input', { bubbles: true }));
    flushSync();
    button('Save')!.click();
    await vi.waitFor(() => expect(puts).toHaveLength(2));
    expect(puts[1]).toMatchObject({ cloudflareToken: 'cf-token' });
  });
});
