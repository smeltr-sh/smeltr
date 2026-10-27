<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  const sections = [
    { id: 'files', title: 'What the extras hold' },
    { id: 'reports', title: 'Read the reports' },
  ];
</script>

<GuideLayout
  title="Release extras"
  description="What each file in a Smeltr release's extras zip is, and how to use it."
  {sections}
>
  <h1>Release extras</h1>
  <p>
    Each release carries <code>smeltr-extras.zip</code> beside its downloads. It holds the container image
    as a file, and the software bill of materials (SBOM) and scan reports for that image. Before you unzip
    it, compare its SHA-256 checksum with the one shown beside it on the release page, and stop if it
    differs.
  </p>

  <h2 id="files">What the extras hold</h2>
  <div class="table-scroll">
    <table>
      <thead><tr><th>File</th><th>What it is</th></tr></thead>
      <tbody>
        <tr
          ><th><code>smeltr-image.tar.gz</code></th><td
            >The container image, for a host that cannot reach the registry. See
            <a href="/docs/install-leader/container#archive">Load an image archive</a>.</td
          ></tr
        >
        <tr
          ><th><code>smeltr-sbom.cdx.json</code></th><td
            >The image's SBOM, in
            <a href="https://cyclonedx.org/">CycloneDX</a> JSON: its operating system packages and
            the libraries built into Smeltr. The published image carries the same SBOM, signed; see
            <a href="/docs/install-leader/container#pull">Pull and verify the image</a>.</td
          ></tr
        >
        <tr
          ><th><code>smeltr-cves.txt</code></th><td
            >Known vulnerabilities in those packages and libraries, found by
            <a href="https://trivy.dev/">Trivy</a> when the release was built.</td
          ></tr
        >
        <tr
          ><th><code>smeltr-stig.html</code></th><td
            >The image checked against the DISA
            <a href="https://public.cyber.mil/stigs/">Security Technical Implementation Guide</a>
            (STIG) for Red Hat Enterprise Linux 9, by
            <a href="https://www.open-scap.org/">OpenSCAP</a>.</td
          ></tr
        >
      </tbody>
    </table>
  </div>

  <h2 id="reports">Read the reports</h2>
  <p>
    The CVE and STIG reports describe the image on the day the release was built. A vulnerability
    found since then is not in them: scan the image again to see it.
  </p>
  <p>
    Some STIG rules govern the boot and the console, which a container does not have, so the STIG
    report shows those rules as failed whatever the image holds.
  </p>
</GuideLayout>
