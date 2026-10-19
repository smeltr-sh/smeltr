<script lang="ts">
  import { image, source } from '#lib/release.ts';
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  const sections = [
    { id: 'pull', title: 'Pull and verify the image' },
    { id: 'archive', title: 'Load an image archive' },
    { id: 'run', title: 'Run the container' },
  ];
</script>

<GuideLayout
  title="Run the container image"
  description={`Run a Smeltr leader from the container image published at ${image}.`}
  {sections}
>
  <h1>Run the container image</h1>
  <p>
    Smeltr publishes its image at <code>{image}</code>, tagged by release. For a cluster, use
    <a href="/docs/install-leader/kubernetes">Kubernetes</a>.
  </p>

  <h2 id="pull">Pull and verify the image</h2>
  <p>
    The release workflow signs each image and attaches its package list (a CycloneDX SBOM). A tag
    can move to a newer image, so pull it, then check and run that exact digest:
  </p>
  <pre><code
      >docker pull {image}:&lt;version&gt;
image=$(docker image inspect --format '&#123;&#123;index .RepoDigests 0&#125;&#125;' {image}:&lt;version&gt;)</code
    ></pre>
  <p>Both checks fail unless that workflow, run for a release tag, made the image:</p>
  <pre><code
      >cosign verify "$image" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp '^{source}/\.github/workflows/release\.yml@refs/tags/v'
cosign verify-attestation --type cyclonedx "$image" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp '^{source}/\.github/workflows/release\.yml@refs/tags/v'</code
    ></pre>
  <p>Name the verified digest the way the commands below expect:</p>
  <pre><code>docker tag "$image" smeltr:&lt;version&gt;</code></pre>

  <h3 id="latest">Why not <code>latest</code></h3>
  <p>
    A tag names the image it was last pushed to. <code>latest</code>, <code>1</code> and
    <code>1.2</code> move to each newer release they cover, which makes them a way to find the newest
    release. A container keeps the image it was created from, but a new one, such as a worker added on
    another host or a new Kubernetes pod, may pull whatever the tag names at that moment. After a release
    those run the new build while the rest stay on the old one.
  </p>
  <p>
    A worker takes deployments only from a leader of its own major release: 1.2 and 1.3 work
    together, 2.0 does not. A worker on an older release of that major refuses a configuration that
    uses something newer, and keeps running what it has. A leader that starts on a newer build
    upgrades its data without the backup an <a href="/docs/upgrade">upgrade</a> begins with. Name the
    image by its digest, and change the digest only as an upgrade.
  </p>

  <h2 id="archive">Load an image archive</h2>
  <p>
    A host that cannot reach ghcr.io loads the image from the archive in the release's extras.
    Compare the extras zip's SHA-256 checksum with the one shown beside it on the release page, and
    stop if it differs. Then unzip the image archive, load it and check its name, tag and runtime
    user:
  </p>
  <pre><code
      >sha256sum smeltr-extras.zip
unzip smeltr-extras.zip smeltr-image.tar.gz
docker load --input smeltr-image.tar.gz
docker image inspect smeltr:&lt;version&gt; --format '&#123;&#123;.Id&#125;&#125; user=&#123;&#123;.Config.User&#125;&#125;'</code
    ></pre>

  <h2 id="run">Run the container</h2>
  <p>The image runs as UID/GID 1000. Prepare persistent host directories before starting it:</p>
  <pre><code
      >sudo install -d -o root -g 1000 -m 0750 /srv/smeltr
sudo install -d -o 1000 -g 1000 -m 0700 /srv/smeltr/leader-data /srv/smeltr/leader-secrets</code
    ></pre>
  <p>
    Save this leader-only configuration as <code>container-smeltr.toml</code> in your current directory.
    HTTP listens on port 4000 inside the container; the run command below restricts the published port
    to the host.
  </p>
  <pre><code
      >[leader]
enabled = true

[leader.http]
bind = "0.0.0.0"
port = 4000
public_origin = "http://127.0.0.1:4000"

[leader.tls]
enabled = false

[leader.storage]
data_dir = "/var/lib/smeltr"

[leader.secrets]
key = "/run/secrets/smeltr/sealing.key"</code
    ></pre>
  <p>
    For a standalone container, append <code>[worker]</code> with <code>enabled = true</code> and
    <code>group = "default"</code>. Its worker state stays within the leader-data mount. Leave the
    sealing key file absent on a fresh installation.
  </p>
  <pre><code
      >sudo install -o root -g 1000 -m 0640 ./container-smeltr.toml /srv/smeltr/smeltr.toml
docker run --name smeltr-leader \
  --restart unless-stopped \
  --read-only --cap-drop ALL --security-opt no-new-privileges \
  -p 127.0.0.1:4000:4000 \
  -e SMELTR_CONFIG_FILE=/etc/smeltr/smeltr.toml \
  --mount type=bind,src=/srv/smeltr/smeltr.toml,dst=/etc/smeltr/smeltr.toml,readonly \
  --mount type=bind,src=/srv/smeltr/leader-data,dst=/var/lib/smeltr \
  --mount type=bind,src=/srv/smeltr/leader-secrets,dst=/run/secrets/smeltr \
  smeltr:&lt;version&gt;</code
    ></pre>
  <p>
    Read the first-login credentials from the container output, then follow <a
      href="/docs/first-login">First login</a
    >. The bind mounts preserve state when you replace the container. The local
    <code>smeltr:&lt;version&gt;</code> tag above names the verified image; use the verified digest
    when deploying on other hosts. Smeltr writes only to those mounts, so the run command makes the
    root filesystem read-only, drops every Linux capability, and lets no process gain privileges.
    Use the
    <a href="/docs/https#proxy">HTTPS proxy setup</a> before allowing remote browsers or workers to connect.
    A remote worker cannot reach this loopback-only example.
  </p>
</GuideLayout>
