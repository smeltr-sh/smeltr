<script lang="ts">
  import { latest } from '#lib/releases.ts';
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  // The command names the newest release, so it runs as shown.
  const version = latest?.version ?? '<version>';
  const sections = [
    { id: 'direct', title: 'Direct HTTPS' },
    { id: 'proxy', title: 'HTTPS reverse proxy' },
    { id: 'compose-proxy', title: 'Reverse proxy with Docker Compose' },
    { id: 'proxy-workers', title: 'Workers behind a proxy' },
  ];
</script>

<GuideLayout
  title="HTTPS"
  description="Serve a Smeltr leader over HTTPS, directly or behind a reverse proxy."
  {sections}
>
  <h1>HTTPS</h1>
  <p>
    Remote browsers and workers reach the leader over HTTPS: the leader serves it on port 443, or a
    reverse proxy in front of it does.
  </p>
  <h2 id="direct">Direct HTTPS</h2>
  <p>
    For a network deployment, configure DNS to reach this host, permit TCP 443 only from intended
    clients, and save this as <code>smeltr.toml</code>:
  </p>
  <pre><code
      >[leader]
enabled = true

[leader.http]
bind = "0.0.0.0"
public_origin = "https://smeltr.example.com"

[leader.tls]
enabled = true

[leader.storage]
data_dir = "/var/lib/smeltr"

[leader.secrets]
key = "/etc/smeltr/secrets/sealing.key"</code
    ></pre>
  <p>
    Port 443 is the default, so you need no <code>port</code> line. On Linux, grant the service
    <code>CAP_NET_BIND_SERVICE</code>
    to listen on a port below 1024 (see
    <a href="https://man7.org/linux/man-pages/man7/capabilities.7.html">capabilities</a>). Keep the
    leader under a dedicated, non-root service account. Provision these directories for the service
    account. Back up the secrets encryption key as
    <a href="/docs/secrets#encryption-key">Encryption key</a> describes. The hostname and port must
    match the URL users actually open. Trust the generated public CA through your organization's
    browser/OS trust procedure before signing in; see
    <a href="/docs/certificates">Certificates</a>. Setting public_origin does not create DNS, open a
    firewall, or configure a proxy.
  </p>
  <p>
    To run it as a service, follow <a href="/docs/install-leader/binary#service"
      >Run as a systemd service</a
    >, which grants that capability. Then confirm <code>https://smeltr.example.com</code> is reachable
    and trusted from a client.
  </p>
  <h2 id="proxy">HTTPS reverse proxy</h2>
  <p>
    When a proxy on the same host terminates HTTPS for a host-installed binary, set <code
      >leader.http.bind = "127.0.0.1"</code
    >, <code>leader.tls.enabled = false</code>, and set
    <code>leader.http.public_origin = "https://smeltr.example.com"</code>. Set
    <code>leader.http.port = 4000</code> (or another unprivileged port) so the process needs no
    extra capability, and configure the proxy separately to forward that origin to
    <code>http://127.0.0.1:4000</code>. Serve a certificate clients trust. Keep the upstream
    listener private and preserve the browser Origin header.
  </p>
  <p>
    For a container, the Smeltr process normally listens on <code>0.0.0.0</code> inside its network
    namespace. Restrict the published host port instead, for example with
    <code>-p 127.0.0.1:4000:4000</code>, and point the same-host proxy at that address. In
    Kubernetes, expose Smeltr with a ClusterIP Service and keep the workload private behind the
    ingress.
  </p>
  <h2 id="compose-proxy">Reverse proxy with Docker Compose</h2>
  <p>
    A <a href="/docs/install-leader/container#compose">Compose install</a> takes a proxy as a second
    service in the same project. Put your certificate and its key in a <code>certs</code> folder
    beside
    <code>compose.yaml</code>, and save this as <code>Caddyfile</code>:
  </p>
  <pre><code
      >smeltr.example.com &#123;
  tls /certs/smeltr.example.com.crt /certs/smeltr.example.com.key
  reverse_proxy smeltr:8443
&#125;</code
    ></pre>
  <p>
    Save this as <code>compose.override.yaml</code>, which Compose reads with
    <code>compose.yaml</code>. It adds the proxy on port 443, turns the leader's own TLS off, and
    stops publishing the leader's port, so other machines reach the leader only through the proxy.
    <code>!reset</code> needs Docker Compose 2.24 or later.
  </p>
  <pre><code
      >services:
  smeltr:
    ports: !reset []
    environment:
      SMELTR_LEADER_TLS: "0"
  proxy:
    image: caddy:2
    restart: unless-stopped
    ports:
      - "443:443"
    volumes:
      - ./Caddyfile:/etc/caddy/Caddyfile:ro
      - ./certs:/certs:ro</code
    ></pre>
  <p>
    Keep <code>SMELTR_PUBLIC_ORIGIN</code> in <code>.env</code> at the proxy's address, then apply it:
  </p>
  <pre><code>docker compose up -d</code></pre>
  <p>
    The worker inside the leader's container reaches the leader directly and keeps running. Other
    workers now meet the proxy's certificate; see <a href="#proxy-workers">Workers behind a proxy</a
    >.
  </p>

  <h2 id="proxy-workers">Workers behind a proxy</h2>
  <p>
    Set workers to use the proxy’s externally reachable HTTPS URL. In this setup, enrollment codes
    tell workers to verify the proxy certificate using the operating system’s trusted certificates.
    A worker trusts Mozilla’s roots and its host’s own, so a certificate from a public authority
    needs nothing more.
  </p>
  <p>
    For a certificate from your organization's authority, give the worker that authority's
    certificate in <code>SMELTR_LEADER_CA</code>. It replaces the authority the enrollment code
    pinned, and the worker verifies only the leader with it. Remove the worker's container and run
    it again with the same volume, which keeps its identity:
  </p>
  <pre><code
      >docker rm -f smeltr-worker
docker run -d --name smeltr-worker --restart unless-stopped \
  --read-only --cap-drop ALL --security-opt no-new-privileges \
  --env-file worker.env \
  -v smeltr-worker:/var/lib/smeltr \
  -v ./example-corp-ca.pem:/etc/smeltr/example-corp-ca.pem:ro \
  -e SMELTR_LEADER_CA=/etc/smeltr/example-corp-ca.pem \
  smeltr:{version}</code
    ></pre>
  <p>
    A worker that does not trust the proxy's certificate goes silent in Status, and its log says the
    leader presented a certificate this worker does not trust. Test both browser login and worker
    enrollment before opening general access.
  </p>
</GuideLayout>
