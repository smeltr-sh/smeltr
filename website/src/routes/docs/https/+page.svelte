<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  const sections = [
    { id: 'direct', title: 'Direct HTTPS' },
    { id: 'proxy', title: 'HTTPS reverse proxy' },
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
  <p>
    Set workers to use the proxy’s externally reachable HTTPS URL. In this setup, enrollment codes
    tell workers to verify the proxy certificate using the operating system’s trusted certificates.
    Install your private CA there if needed. Test both browser login and worker enrollment before
    opening general access.
  </p>
</GuideLayout>
