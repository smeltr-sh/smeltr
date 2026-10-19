<script lang="ts">
  import { highlight } from '#lib/highlight/highlight.ts';
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import ProductScreenshot from '#lib/components/ProductScreenshot.svelte';
  import KindSettings from '#lib/components/KindSettings.svelte';
  import FieldsTable from '#lib/components/FieldsTable.svelte';
  import { SOURCES, kind } from '#lib/connector-reference.ts';
  import examples from '#lib/connector-examples.json';
  const sections = [
    { id: 'example', title: 'Example' },
    { id: 'sign-in', title: 'How it signs in' },
    { id: 'service-app', title: 'Set up a service app' },
    { id: 'api-token', title: 'Set up an API token' },
    { id: 'settings', title: 'Settings' },
    { id: 'fields-it-gives', title: 'Fields it gives' },
    { id: 'start-and-stop', title: 'Where it starts' },
    { id: 'how-it-works', title: 'How it works' },
    { id: 'verify', title: 'Verify' },
    { id: 'troubleshooting', title: 'Troubleshooting' },
    { id: 'see-also', title: 'See also' },
  ];
  const json = (value: unknown) => JSON.stringify(value, null, 2);
</script>

<GuideLayout
  title="Okta System Log source"
  description="Read an Okta org's System Log, signed in as a service app or with an API token: how to set up each, every setting, and the errors Okta gives."
  {sections}
>
  <h1>Okta System Log</h1>
  <p class="lede">Reads an Okta org's System Log, every event once, in the order Okta stored it.</p>
  <p>
    The source polls Okta's
    <a href="https://developer.okta.com/docs/reference/api/system-log/">System Log API</a>.
  </p>

  <h2 id="example">Example</h2>
  <p>
    Reads the System Log of <code>acme.okta.com</code>. The source signs in as the service app
    <code>0oa1b2c3d4e5f6g7h8i9</code> and reads its private key from the worker-host variable
    <code>OKTA_PRIVATE_KEY</code>.
  </p>
  <pre><code>{@html highlight(json(examples.oktaLogsSource))}</code></pre>
  <ProductScreenshot
    source="okta-logs-source-form"
    height={2100}
    crop={{ x: 608, y: 551, width: 1664, height: 999 }}
    alt="The Okta System Log form on the Sources page: id okta-audit, org https://acme.okta.com, sign in as a service app, client id 0oa1b2c3d4e5f6g7h8i9."
    caption="The same source in the Sources page's add dialog, before you pick its private key."
  />
  <p>The same source, signing in with an API token instead:</p>
  <pre><code>{@html highlight(json(examples.oktaTokenSource))}</code></pre>

  <h2 id="sign-in">How it signs in</h2>
  <p>
    The source signs in to Okta as a service app or with an API token. For a service app, set
    <code>clientId</code> and <code>privateKey</code>; for an API token, set <code>token</code>.
  </p>
  <table>
    <thead>
      <tr><th></th><th>Service app</th><th>API token</th></tr>
    </thead>
    <tbody>
      <tr>
        <th scope="row">Settings</th>
        <td
          ><code>clientId</code>, <code>privateKey</code>, and <code>keyId</code> when the app has several
          active keys</td
        >
        <td><code>token</code></td>
      </tr>
      <tr>
        <th scope="row">Acts as</th>
        <td>The app, with only the scope and role you grant it</td>
        <td>The admin who created the token, with all of that admin's rights</td>
      </tr>
    </tbody>
  </table>
  <p>
    Use a service app. Recommended because it keeps working after the person who set it up leaves.
    Use an API token for quick tests.
  </p>

  <h2 id="service-app">Set up a service app</h2>
  <p>
    Create an API Services app as Okta's
    <a href="https://developer.okta.com/docs/guides/implement-oauth-for-okta-serviceapp/main/"
      >service app guide</a
    > describes, with:
  </p>
  <ol>
    <li>Public key / private key client authentication, with DPoP on or off.</li>
    <li>The <code>okta.logs.read</code> scope.</li>
    <li>The Read-only Administrator role.</li>
  </ol>
  <p>
    Copy its client ID into <code>clientId</code>, put its active private key in a
    <a href="/docs/secrets">credential</a> named in <code>privateKey</code>, and set
    <code>keyId</code> when the app has more than one active key.
  </p>
  <p>
    The source takes an RSA key in PKCS#8 PEM, which begins
    <code>-----BEGIN PRIVATE KEY-----</code>. If your key begins
    <code>-----BEGIN RSA PRIVATE KEY-----</code>, convert it first:
  </p>
  <pre><code>openssl pkcs8 -topk8 -nocrypt -in key.pem -out key-pkcs8.pem</code></pre>

  <h2 id="api-token">Set up an API token</h2>
  <p>
    Create an API token as Okta's
    <a href="https://developer.okta.com/docs/guides/create-an-api-token/main/">API token guide</a>
    describes, signed in as an admin who can read the System Log, such as a Read-only Administrator. Put
    it in a <a href="/docs/secrets">credential</a> and name it in <code>token</code>.
  </p>

  <h2 id="settings">Settings</h2>
  <p>
    Set <code>org</code> to the address your users sign in at, such as
    <code>https://acme.okta.com</code>, or to the org's custom domain. The source also takes the
    settings
    <a href="/docs/sources#settings">every source</a> takes.
  </p>
  <KindSettings group={SOURCES} type="oktaLogs" />

  <h2 id="fields-it-gives">Fields it gives</h2>
  <p>
    Each record is one event, exactly as Okta sends it. <code>__source.url</code> holds the address of
    the page the source read it from.
  </p>
  <FieldsTable names={kind(SOURCES, 'oktaLogs').fields ?? []} />

  <h2 id="start-and-stop">Where it starts</h2>
  <p>
    The first poll asks for the last seven days of events. With <code>start</code> set to
    <code>new</code>, it asks only for events from the first poll onward. After that, the source
    resumes at the last page every destination took, so a restarted worker neither skips nor repeats
    events, and changing <code>start</code> does not replay anything. The source keeps reading until you
    remove it.
  </p>

  <h2 id="how-it-works">How it works</h2>
  <p>
    One worker of the group holds the source. Each poll asks the System Log API for events in
    ascending order and follows Okta's next links until a page holds nothing new, then waits
    <code>pollIntervalSecs</code> from the poll's start. When the source reaches Okta's rate limit, it
    waits until the reset time Okta names.
  </p>
  <p>
    As a service app, the source signs a key assertion with <code>privateKey</code> and asks the
    org's token endpoint, <code>/oauth2/v1/token</code>, for a token for the
    <code>okta.logs.read</code> scope. The source makes its own DPoP key when it starts, and proves each
    request with it when the app requires DPoP. The source renews the token shortly before it expires,
    and signs in again when Okta refuses a token it has already used.
  </p>
  <p>The source follows no redirects, because it sends its credentials with every request.</p>

  <h2 id="verify">Verify</h2>
  <p>
    Sign in to the org, then look at the destination for an event with <code>eventType</code>
    <code>user.session.start</code>. It arrives within a poll interval after Okta lists it.
  </p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <p>The worker log shows Okta's answer, followed by what to check.</p>
  <dl>
    <dt><strong>The client_assertion signature is invalid</strong></dt>
    <dd>
      The app holds no active key that matches <code>privateKey</code>. Check that the app holds the
      key's public half and that the key is <strong>Active</strong>.
    </dd>
    <dt><strong>kid is invalid</strong></dt>
    <dd>
      The app has several active keys. Set
      <code>keyId</code> to the key's <strong>KID</strong>, or deactivate the other keys.
    </dd>
    <dt><strong>Refused the credentials (403), E0000006</strong></dt>
    <dd>Check that the app, or the token's admin, has the Read-only Administrator role.</dd>
    <dt><strong>Refused the sign-in (400), invalid_scope</strong></dt>
    <dd>
      Check that the app has the <code>okta.logs.read</code> scope.
    </dd>
    <dt><strong>Refused the sign-in (302)</strong></dt>
    <dd>
      <code>org</code> names the <code>-admin</code> address. Remove <code>-admin</code> from it.
    </dd>
    <dt><strong>The private key is not PKCS#8</strong></dt>
    <dd>Convert it with <code>openssl pkcs8 -topk8 -nocrypt</code>, as shown above.</dd>
    <dt><strong>Refused the credentials (401) with an API token</strong></dt>
    <dd>Okta no longer accepts the token. Create a new token, or switch to a service app.</dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/sources">Sources</a></li>
    <li><a href="/docs/secrets">Credentials</a></li>
    <li>
      <a href="https://developer.okta.com/docs/guides/implement-oauth-for-okta-serviceapp/main/"
        >Okta: Implement OAuth for Okta with a service app</a
      >
    </li>
    <li>
      <a href="https://developer.okta.com/docs/reference/api/system-log/">Okta: System Log API</a>
    </li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
  th[scope='row'] {
    text-align: left;
    white-space: nowrap;
  }
</style>
