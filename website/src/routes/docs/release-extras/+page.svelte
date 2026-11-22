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
    for each architecture as a file, and the software bill of materials (SBOM) and scan reports for each
    image. Before you unzip it, compare its SHA-256 checksum with the one shown beside it on the release
    page, and stop if it differs.
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
            >The image checked by <a href="https://www.open-scap.org/">OpenSCAP</a> against DISA's
            General Purpose Operating System
            <a href="https://public.cyber.mil/stigs/">Security Requirements Guide</a> (SRG), the
            baseline that operating system STIGs are built from, with
            <a href="https://github.com/chainguard-dev/stigs">Chainguard's profile</a> for Wolfi images.</td
          ></tr
        >
      </tbody>
    </table>
  </div>
  <p>
    Each file has an Arm64 version, with <code>-arm64</code> added to the name before the extension:
    <code>smeltr-image-arm64.tar.gz</code>, <code>smeltr-sbom-arm64.cdx.json</code>,
    <code>smeltr-stig-arm64.html</code> and <code>smeltr-cves-arm64.txt</code>.
  </p>

  <h2 id="reports">Read the reports</h2>
  <p>
    The CVE and STIG reports describe the image on the day the release was built. A vulnerability
    found since then is not in them: scan the image again to see it.
  </p>
  <p>
    The STIG report, an OpenSCAP scan against the SRG, marks rules a container cannot meet, such as
    those for the boot and the console, as not applicable. Two kinds of rule fail, and you can check
    each yourself:
  </p>
  <ul>
    <li>
      Rules that ask for FIPS-validated cryptography look for it only in OpenSSL's FIPS provider.
      The image has no OpenSSL: all of Smeltr's cryptography goes through AWS-LC's FIPS module,
      built into Smeltr. The SBOM lists it as <code>aws-lc-fips-sys</code>. In NIST's
      <a href="https://csrc.nist.gov/projects/cryptographic-module-validation-program"
        >Cryptographic Module Validation Program</a
      >, look up the validation of the version the SBOM lists. The SBOM also lists
      <code>aws-lc-sys</code>, which is built because <code>rustls</code> asks for it, and is not used.
    </li>
    <li>
      The rule on trust anchors checks that <code>SSL_CERT_FILE</code> names the image's CA bundle.
      The image sets it, but the scan reads the image's files, not the environment the image gives a
      container. Run <code>docker image inspect</code> on the image to see it set.
    </li>
  </ul>
</GuideLayout>
