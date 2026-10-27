<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  const sections = [
    { id: 'model', title: 'Choose the trust model' },
    { id: 'download', title: 'Obtain and verify the public CA' },
    { id: 'list', title: 'Keep certificates in the list' },
    { id: 'connectors', title: 'Give a connector a certificate' },
    { id: 'names', title: 'Names a certificate must cover' },
    { id: 'source', title: 'Choose where the certificate comes from' },
    { id: 'acme', title: 'Order a certificate over ACME' },
    { id: 'rotation', title: 'Rotate the authority' },
  ];
</script>

<GuideLayout
  title="Certificates and TLS"
  description="Certificates and TLS for Smeltr administrators."
  {sections}
>
  <h1>Certificates and TLS</h1>
  <p>
    TLS encrypts connections and checks the server’s identity. A certificate authority (CA) signs
    server certificates. Clients must trust that authority before accepting its certificates.
  </p>
  <h2 id="model">Choose the trust model</h2>
  <div class="table-scroll">
    <table>
      <thead><tr><th></th><th>TLS at the leader</th><th>TLS at a proxy</th></tr></thead>
      <tbody>
        <tr><th>Certificate served by</th><td>Smeltr</td><td>Reverse proxy or ingress</td></tr>
        <tr
          ><th>Setting</th><td><code>leader.tls.enabled = true</code></td><td
            ><code>leader.tls.enabled = false</code>; keep upstream private</td
          ></tr
        >
        <tr
          ><th>Worker trust</th><td>Enrollment codes establish trust in the leader CA</td><td
            >System trust; configure private CA trust separately</td
          ></tr
        >
        <tr
          ><th>Browser trust</th><td
            >Verify the CA fingerprint, then install its public certificate</td
          ><td>Trust the proxy certificate; install a private CA if needed</td></tr
        >
        <tr
          ><th>Certificate changes</th><td>Admin → System → Leader certificate</td><td
            >At the proxy or ingress</td
          ></tr
        >
      </tbody>
    </table>
  </div>
  <h2 id="download">Obtain and verify the public CA</h2>
  <p>
    For direct TLS the generated CA is at <code>tls/ca.pem</code> beneath leader.storage.data_dir.
    Obtain that public file through a trusted host-management channel for initial browser setup.
    Once signed in, Admin → System → Leader certificate also provides
    <strong>Download ca.pem</strong> and the authority fingerprint. Never distribute the CA private key.
  </p>
  <pre><code
      >openssl x509 -in /var/lib/smeltr/tls/ca.pem -noout -subject -dates -fingerprint -sha256</code
    ></pre>
  <p>
    Verify the certificate names against the public URL and any worker-only DNS names. Add
    worker-only names with leader.tls.extra_san, restart for startup-setting changes, and check the
    certificate shown in the UI. Do not use a certificate warning bypass as the production setup
    procedure.
  </p>
  <h2 id="list">Keep certificates in the list</h2>
  <p>
    Open Admin → Certificates and choose <strong>+ Add certificate</strong>. Enter a Name, paste the
    full certificate chain and its private key, both PEM, and choose <strong>Add</strong>. For a
    private authority, end the chain with its root. The private key is stored encrypted and never
    shown again.
  </p>
  <p>
    To renew a certificate, choose <strong>Replace</strong> and paste the new version. If the leader serves
    it, the leader switches to the new version; there's no need to choose it again. A worker whose connector
    names it presents the new version from its next heartbeat. The certificate the leader serves cannot
    be deleted.
  </p>
  <p>
    To have the leader issue one instead, choose <strong>Issued by this leader</strong>, enter the
    DNS names and IP addresses it is for under <strong>Issued for</strong>, choose how long it lasts
    under <strong>Valid for</strong>, and choose <strong>Add</strong>. The leader signs it with its
    own authority, so a client verifies it against the leader's
    <a href="#download">ca.pem</a>. When a third of its life remains (30 days of 90, about four
    months of a year), or after you <a href="#rotation">rotate the authority</a>, the leader issues
    the next version on its own, valid for the same length of time, and workers use it from their
    next heartbeat.
  </p>
  <h2 id="connectors">Give a connector a certificate</h2>
  <p>
    A source, destination or receiver names a certificate from this list by name, under its TLS
    settings. A source or destination presents it as a client, when the far end asks for one. A
    receiver serves TLS with it, and without one takes plain traffic. Workers use the newest version
    of the certificate, and switch to a new version on their own.
  </p>
  <ul>
    <li>
      <strong>Trusted authorities</strong> are the certificates of the authorities you trust, as PEM,
      pasted into the connector. A source or destination trusts them beside the public roots, Mozilla's
      and the worker host's own. A receiver with trusted authorities admits only senders whose certificate
      chains to one of them, and trusts no public root.
    </li>
    <li>
      To accept only certificates from your trusted authorities, turn off <strong
        >trust public roots</strong
      >
      under Advanced TLS.
    </li>
    <li>
      With no TLS setting filled, a source or destination presents no certificate and trusts the
      public roots.
    </li>
  </ul>
  <p>
    Check that the name a worker connects to is on the far end's certificate. The certificate the
    leader serves to browsers and workers is chosen under <a href="#source"
      >where the certificate comes from</a
    >.
  </p>
  <h2 id="names">Names a certificate must cover</h2>
  <p>
    A certificate this leader serves must cover the name your browser reaches it by. If this leader
    runs its own worker, it must also cover the address that worker reaches it by: 127.0.0.1, unless
    <a href="/docs/reference#worker.leader.url">worker.leader.url</a> says otherwise. Public
    authorities don't issue certificates for 127.0.0.1, so to serve a public certificate, set
    worker.leader.url to a URL on one of the certificate's names, such as
    <code>https://smeltr.example.com</code>, then restart.
  </p>
  <h2 id="source">Choose where the certificate comes from</h2>
  <p>
    Admin → System → Leader certificate shows the authority and the certificate this leader serves,
    and a <strong>Source</strong> chooser with the one in force marked.
  </p>
  <div class="table-scroll">
    <table>
      <thead><tr><th>Source</th><th>Where the certificate comes from</th></tr></thead>
      <tbody>
        <tr
          ><th>Leader issued</th><td
            >The leader's own authority signs it. <strong>Issue a new one</strong> replaces it under the
            same authority.</td
          ></tr
        >
        <tr
          ><th>Uploaded</th><td
            >An administrator picks a certificate from <strong>Admin → Certificates</strong>, or
            adds one there with <strong>add a certificate</strong>, and selects
            <strong>Serve this certificate</strong>. See
            <a href="#names">Names a certificate must cover</a>.</td
          ></tr
        >
        <tr
          ><th>ACME</th><td
            >The leader orders it from a certificate authority and renews it.
            <a href="#acme">Order a certificate over ACME</a>.</td
          ></tr
        >
      </tbody>
    </table>
  </div>
  <p>A new certificate from the same authority is served at once.</p>
  <p>
    A new authority has to reach the workers first. The page shows the switch in progress while the
    leader hands the authority to them, and which workers have it. The leader serves the new
    certificate as soon as every worker has it, or at the cutover time the page shows, which you can
    change.
  </p>
  <ul>
    <li><strong>Cut over now</strong> serves it without waiting.</li>
    <li><strong>Abandon switch</strong> cancels the switch.</li>
  </ul>
  <p>
    Browsers must be told to trust a new authority separately. If the page says the socket refused
    the reload, restart the leader. Replace proxy certificates at the proxy.
  </p>
  <p>
    If the leader starts and cannot serve the certificate you chose, for example because it expired,
    it serves a "Leader issued" certificate instead. Admin → System → Leader certificate says why
    and what to do.
  </p>
  <h2 id="acme">Order a certificate over ACME</h2>
  <p>
    Let's Encrypt and other certificate authorities issue certificates automatically over ACME. The
    authority checks that each name on the certificate reaches this leader, then issues it. The
    leader renews it on its own. Each order is a new version of the <strong>acme</strong> entry in Admin
    → Certificates, which a connector can name too; its workers use each new version from their next heartbeat.
  </p>
  <h3>Before you start</h3>
  <ul>
    <li>
      Every name on the certificate points at this leader in DNS, unless you use DNS-01. A public
      authority such as Let's Encrypt won't issue for <code>localhost</code> or a private address.
    </li>
    <li>
      With the TLS-ALPN-01 challenge, the default, port 443 on each name must reach this leader.
    </li>
    <li>
      With the HTTP-01 challenge, port 80 on each name must reach this leader. The leader listens on
      leader.tls.acme_http_port only while it orders. That setting is 80 unless something forwards
      80 to another port.
    </li>
    <li>
      With the DNS-01 challenge, nothing has to reach this leader, so it works for a leader on a
      private network. The zones that hold the names must be in your Cloudflare account. DNS-01
      can't prove an IP address.
    </li>
    <li>
      If this leader runs its own worker, read
      <a href="#names">Names a certificate must cover</a> first.
    </li>
  </ul>
  <h3>Order it</h3>
  <ol>
    <li>Under Source, choose <strong>ACME</strong>.</li>
    <li>
      Pick the <strong>Directory</strong>: Let's Encrypt, or Other to type another authority's URL.
    </li>
    <li>Check <strong>Names</strong>. They start as this leader's own names, without localhost.</li>
    <li>
      Enter a <strong>Contact email</strong>. The authority sends notices about the account there.
    </li>
    <li>Agree to the terms of service.</li>
    <li>
      Select <strong>Get certificate</strong>. The first certificate from an authority is a switch,
      as described above.
    </li>
  </ol>
  <h3>Renewal</h3>
  <p>With ACME in force, the page shows when the certificate renews.</p>
  <ul>
    <li>
      The leader renews when one-third of the certificate's life remains, e.g. 30 days of a 90-day
      certificate.
    </li>
    <li>If an order fails, the page shows why, and the leader tries again every hour.</li>
    <li>
      If the secrets <a href="/docs/secrets#encryption-key">encryption key</a> is regenerated, the leader
      orders a new certificate right away, because it can no longer open the private key of the one it
      has.
    </li>
    <li>
      <strong>Save</strong> keeps your changes for the next renewal. <strong>Renew now</strong> orders
      a certificate right away.
    </li>
    <li>A renewal under a new root is a switch, like the first order.</li>
  </ul>
  <p>
    The leader keeps renewing after you choose another source, so connectors that name the
    certificate <strong>acme</strong> in Admin → Certificates stay current.
  </p>
  <h3>Advanced settings</h3>
  <p>Under <strong>Advanced</strong>:</p>
  <ul>
    <li>
      <strong>Challenge</strong> picks TLS-ALPN-01, HTTP-01 or DNS-01. For DNS-01,
      <strong>API token</strong> takes a Cloudflare token with Zone · Zone · Read and Zone · DNS · Edit
      on those zones. The leader stores it encrypted and never shows it again.
    </li>
    <li>
      <strong>Authority</strong> takes the root a private authority issues under, as PEM. It is needed
      only when the platform does not already trust that root.
    </li>
    <li>
      <strong>EAB key ID</strong> and <strong>EAB HMAC key</strong> are for an authority that requires
      an account with it. It gives you both. The leader stores the HMAC key encrypted and never shows
      it again.
    </li>
  </ul>
  <h2 id="rotation">Rotate the authority</h2>
  <ol>
    <li>
      Back up the leader state and key; inventory offline workers and workers using explicit
      worker.leader.ca files.
    </li>
    <li>
      Under Rotate authority, set <strong>Wait for the fleet</strong> and select
      <strong>Start rotation</strong>.
    </li>
    <li>
      The leader publishes the incoming authority on heartbeats while it still signs under the old
      one. Ensure workers have received it. Explicit CA-file overrides require manual maintenance;
      update browser trust separately.
    </li>
    <li>
      Only after verifying client readiness choose <strong>Cut over now</strong>, or allow the
      deadline to do so automatically. The leader retires the old authority and withdraws enrollment
      codes naming it.
    </li>
    <li>
      Verify heartbeats and browser access after cutover. Before cutover, <strong
        >Abandon rotation</strong
      > cancels the pending rotation.
    </li>
  </ol>
  <aside class="callout">
    <p>
      A worker that missed the incoming authority can lose the ability to verify the leader at
      cutover and require re-enrollment. The deadline is an automatic cutover, not merely a
      reminder. Do not start rotation without a plan for offline workers.
    </p>
  </aside>
</GuideLayout>
