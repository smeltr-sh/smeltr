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
  restartRequired: false,
  rotation: {
    incoming: described('smeltr authority 2'),
    startedMillis: Date.now(),
    deadlineMillis: Date.now() + YEAR,
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
  httpPort: 80,
  issued: null,
  notAfterMillis: null,
  renewAfterMillis: null,
  lastAttemptMillis: null,
  lastError: null,
};

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
      if (url.startsWith('/api/settings/certificates')) return json(certificates);
      if (url.startsWith('/api/settings/acme')) return json(acme);
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
    await vi.waitFor(() => expect(posts).toEqual(['/api/settings/certificates/rotation/cutover']));
  });

  it('issues a new certificate only once it is confirmed', async () => {
    await show();
    ask.mockResolvedValueOnce(false);
    button('Issue a new one')!.click();
    await settle();
    expect(ask).toHaveBeenCalledTimes(1);
    expect(posts).toEqual([]);

    ask.mockResolvedValueOnce(true);
    button('Issue a new one')!.click();
    await vi.waitFor(() => expect(posts).toEqual(['/api/settings/certificates/reissue']));
  });
});

describe('the source', () => {
  const UPLOADED = { ...CERTIFICATES, supplied: true, source: 'upload', rotation: null };

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
    expect(deletes).toEqual([]);

    button('Switch to Leader issued')!.click();
    await vi.waitFor(() => expect(deletes).toEqual(['/api/settings/certificates/supplied']));
  });

  it('offers rotation only under Leader issued', async () => {
    await show({ ...CERTIFICATES, rotation: null }, 'Issue a new one');
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
    expect(button('Issue a new one')).toBeUndefined();
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
      rotation: {
        ...CERTIFICATES.rotation,
        installs: 'own',
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
      rotation: { ...CERTIFICATES.rotation, installs: 'own', notFollowing: ['ops-1', 'ops-2'] },
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
          answered = {
            ...CERTIFICATES,
            rotation: { ...CERTIFICATES.rotation, installs: 'supplied', source: 'upload' },
          };
        }
        if (url.startsWith('/api/settings/certificates')) return json(answered);
        if (url.startsWith('/api/settings/acme')) return json(ACME);
        throw new Error(`unexpected ${url}`);
      }),
    );
    page = mount(CertificatesSettings, { target });
    await vi.waitFor(() => expect(button('Issue a new one')).toBeDefined());
    option('Uploaded')!.click();
    flushSync();
    for (const area of target.querySelectorAll('textarea')) {
      area.value = 'pem';
      area.dispatchEvent(new Event('input', { bubbles: true }));
    }
    flushSync();
    button('Serve this certificate')!.click();
    await vi.waitFor(() =>
      expect(target.textContent).toContain(
        'the incoming authority is being published to the fleet',
      ),
    );
    expect(target.textContent).toContain('Switch to Uploaded in progress');
  });

  it('keeps a rotation already open in reach while an uploaded certificate is in force', async () => {
    await show({ ...CERTIFICATES, supplied: true, source: 'upload' }, 'Serve this certificate');
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

describe('ACME', () => {
  const ORDERED = {
    ...ACME,
    enabled: true,
    directory: 'https://acme-v02.api.letsencrypt.org/directory',
    contact: 'ops@example.com',
    names: ['leader.example'],
    issued: 'ab'.repeat(32),
    notAfterMillis: Date.now() + YEAR,
    renewAfterMillis: Date.now() + YEAR / 2,
  };
  const IN_FORCE = { ...CERTIFICATES, supplied: true, source: 'acme', rotation: null };

  it('orders only once the terms are agreed to, and fills the names from the certificate', async () => {
    await show({ ...CERTIFICATES, rotation: null }, 'Issue a new one');
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
    await vi.waitFor(() => expect(posts).toEqual(['/api/settings/acme/order']));
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
});
