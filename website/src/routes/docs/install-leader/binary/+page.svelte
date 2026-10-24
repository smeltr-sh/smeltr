<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  const sections = [
    { id: 'verify', title: 'Verify the executable' },
    { id: 'install', title: 'Install the binary' },
    { id: 'first-start', title: 'Standalone: first local start' },
    { id: 'service', title: 'Run as a systemd service' },
  ];
</script>

<GuideLayout
  title="Install the binary"
  description="Install a Smeltr leader from the supplied executable."
  {sections}
>
  <h1>Install the binary</h1>
  <p>
    <a href="/get#download">Download</a> the Linux archive or the Windows zip.
  </p>
  <h2 id="verify">Verify the executable</h2>
  <p>
    Compare the archive's SHA-256 checksum with the one shown beside it on the release page, and
    stop if it differs:
  </p>
  <pre><code>sha256sum smeltr-linux.tar.gz</code></pre>
  <p>
    On Windows use <code>Get-FileHash -Algorithm SHA256 smeltr-windows.zip</code>. The archive holds
    the executable, the cryptography library it loads, the license and the third-party notices.
  </p>

  <h2 id="install">Install the binary</h2>
  <p>
    The following commands target Linux with <code>useradd</code>. Create a dedicated service
    account once. If it already exists, use it. Keep the supplied executable name, version and
    checksum with your installation records.
  </p>
  <pre><code
      >sudo useradd --system --user-group --home-dir /var/lib/smeltr --no-create-home --shell /usr/sbin/nologin smeltr
sudo install -d -o root -g root -m 0755 /opt/smeltr
tar -xzf smeltr-linux.tar.gz
sudo install -o root -g root -m 0755 smeltr /opt/smeltr/smeltr
sudo install -d -o root -g smeltr -m 0750 /etc/smeltr
sudo install -d -o smeltr -g smeltr -m 0700 /var/lib/smeltr /etc/smeltr/secrets</code
    ></pre>
  <p>
    Install Git using your operating system's package manager and confirm <code>git --version</code>
    succeeds. The leader uses <code>git http-backend</code> to serve configuration; workers have a built-in
    Git client. The executable includes the browser UI.
  </p>
  <p>
    The <strong>secrets encryption key</strong> encrypts secrets the leader stores. On a fresh
    installation, leave <code>/etc/smeltr/secrets/sealing.key</code> absent so Smeltr creates it.
    Back it up and keep the matching copies for recovery;
    <a href="/docs/secrets#encryption-key">Encryption key</a>
    says where to keep that backup.
  </p>

  <h2 id="first-start">Standalone: first local start</h2>
  <p>
    Save this complete configuration as <code>smeltr.toml</code> in your current directory. It runs the
    leader and one worker as one service, with the browser interface restricted to this machine.
  </p>
  <pre><code
      >[leader]
enabled = true

[leader.http]
bind = "127.0.0.1"
port = 4000
public_origin = "http://127.0.0.1:4000"

[leader.tls]
enabled = false

[leader.storage]
data_dir = "/var/lib/smeltr"

[leader.secrets]
key = "/etc/smeltr/secrets/sealing.key"

[worker]
enabled = true
group = "default"</code
    ></pre>
  <p>
    The worker enrolls automatically and uses the local leader. Leave its enrollment code and leader
    URL unset. It stores its identity, credential and cached configuration in <code
      >/var/lib/smeltr/worker</code
    >. This HTTP setup is for local evaluation; use <a href="/docs/https">HTTPS</a> for network access.
  </p>
  <p>Install the configuration, then start Smeltr in the foreground as its service account:</p>
  <pre><code
      >sudo install -o root -g smeltr -m 0640 ./smeltr.toml /etc/smeltr/smeltr.toml
sudo -u smeltr env SMELTR_CONFIG_FILE=/etc/smeltr/smeltr.toml /opt/smeltr/smeltr</code
    ></pre>
  <p>
    Keep this process running while you <a href="/docs/first-login">sign in</a>. Stopping it stops
    both the leader and worker. The absolute configuration and storage paths make startup
    independent of your working directory.
  </p>

  <h2 id="service">Run as a systemd service</h2>
  <p>
    Stop any foreground evaluation process before starting the service. Install the configuration it
    runs: the local one above, or a network one from <a href="/docs/https">HTTPS</a>.
  </p>
  <pre><code>sudo install -o root -g smeltr -m 0640 ./smeltr.toml /etc/smeltr/smeltr.toml</code
    ></pre>
  <p>
    The HTTPS configuration runs the leader only. To keep standalone mode, add the <code
      >[worker]</code
    >
    settings from the local example before installing it. Save this unit as
    <code>/etc/systemd/system/smeltr-leader.service</code>:
  </p>
  <pre><code
      >[Unit]
Description=Smeltr leader
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=smeltr
Group=smeltr
StateDirectory=smeltr
StateDirectoryMode=0700
WorkingDirectory=/var/lib/smeltr
Environment=SMELTR_CONFIG_FILE=/etc/smeltr/smeltr.toml
ExecStart=/opt/smeltr/smeltr
AmbientCapabilities=CAP_NET_BIND_SERVICE
CapabilityBoundingSet=CAP_NET_BIND_SERVICE
NoNewPrivileges=true
UMask=0077
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target</code
    ></pre>
  <p>
    The service can bind port 443 while running as <code>smeltr</code>.
  </p>
  <pre><code
      >sudo systemctl daemon-reload
sudo systemctl enable --now smeltr-leader
sudo journalctl -u smeltr-leader -b</code
    ></pre>
  <p>
    On first start, read the one-time administrator credentials in the service journal and complete
    the <a href="/docs/first-login">first login</a>. Restrict access to that journal as you would
    other credential-bearing console logs.
  </p>
  <p>
    For another service manager, set the executable, absolute configuration path, working directory,
    service account and restart policy explicitly. Preserve the data, key and configuration across
    replacing the executable. Do not start a second leader against the same data.
  </p>
</GuideLayout>
