<script lang="ts">
  import { highlight } from '#lib/highlight/highlight.ts';
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import KindSettings from '#lib/components/KindSettings.svelte';
  import FieldsTable from '#lib/components/FieldsTable.svelte';
  import { DESTINATIONS, kind } from '#lib/connector-reference.ts';
  import examples from '#lib/connector-examples.json';
  const sections = [
    { id: 'example', title: 'Example' },
    { id: 'before-you-start', title: 'Before you start' },
    { id: 'settings', title: 'Settings' },
    { id: 'fields-it-reads', title: 'Fields it reads' },
    { id: 'how-it-works', title: 'How it works' },
    { id: 'verify', title: 'Verify' },
    { id: 'troubleshooting', title: 'Troubleshooting' },
    { id: 'see-also', title: 'See also' },
  ];
  const json = (value: unknown) => JSON.stringify(value, null, 2);
</script>

<GuideLayout
  title="SFTP destination"
  description="Write batches as files in a directory on an SFTP server: an example, every setting, the fields it reads, and what to have ready."
  {sections}
>
  <h1>SFTP</h1>
  <p class="lede">
    Writes batches of records as files in a directory on an SFTP server. A reader of the directory
    never sees part of a file.
  </p>

  <h2 id="example">Example</h2>
  <p>
    Signs in as <code>smeltr</code> with the private key in the worker-host variable
    <code>SFTP_PRIVATE_KEY</code>, and connects only to the server holding the pinned host key.
  </p>
  <pre><code>{@html highlight(json(examples.sftpDestination))}</code></pre>

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>
      Create the directory on the server. It must exist, as a bucket must; the destination makes the
      folders a prefix names under it as needed.
    </li>
    <li>
      Have an account on the server that may create files and folders in the directory and rename
      files there.
    </li>
    <li>
      Get the server's host key: a line such as <code>ssh-ed25519 AAAA…</code>. Copy it from the
      server's <code>/etc/ssh/ssh_host_ed25519_key.pub</code>, or run
      <code>ssh-keyscan -t ed25519 sftp.example.com</code> and drop the host name the line starts
      with. <code>ssh-keyscan</code> trusts the network it asks over, so check its answer against the
      server's file.
    </li>
    <li>
      Put the account's password, or its private key, in a <a href="/docs/secrets">credential</a>.
      The private key is an unencrypted RSA, ed25519 or ECDSA key in OpenSSH's format.
    </li>
  </ul>

  <h2 id="settings">Settings</h2>
  <p>
    It also takes the settings <a href="/docs/destinations#settings">every destination</a> takes.
  </p>
  <KindSettings group={DESTINATIONS} type="sftp" />

  <h2 id="fields-it-reads">Fields it reads</h2>
  <FieldsTable names={kind(DESTINATIONS, 'sftp').reads ?? []} />

  <h2 id="how-it-works">How it works</h2>
  <p>
    The destination keeps one file open per prefix, and for Parquet, Avro and CSV per record type of
    the routes feeding it, across batches, at
    <code>&lt;directory&gt;/&lt;prefix&gt;&lt;random id&gt;.&lt;extension&gt;</code>. It finishes
    the file when it reaches <code>maxObjectBytes</code> on disk, when <code>fileIntervalSecs</code>
    passes, when <code>bufferBytes</code> fills on the worker, or when a 65th file opens and this one
    was written least recently, and acknowledges a batch once every file holding its records has landed.
    It makes the folders the prefix names first when they are not there.
  </p>
  <p>
    It writes a file under a hidden temporary name in the same folder, a dot, its own name and a
    tag, ending in <code>.tmp</code>, such as
    <code>.3f9c…e1.ndjson.gz.4182-7-123456789.tmp</code>. Once it has written every byte, it renames
    the file to its own name, so a reader sees the whole file or none of it. An
    <a href="/docs/sources/sftp">SFTP source</a> skips these temporary files.
  </p>
  <p>
    Two files never share a name, so records written again after a failure make new files. On a
    failure the destination deletes the files it already finished, best effort; one it cannot delete
    holds records that land twice. When the rename fails, the destination removes the temporary
    file; a connection lost while writing can leave one behind, which is safe to delete.
  </p>
  <p>
    One SSH connection carries the destination's writes. It connects only to a server that shows the
    pinned host key, and asks the server for that key's algorithm alone. The destination closes a
    connection that stops answering, and the next write opens a new one.
  </p>

  <h2 id="verify">Verify</h2>
  <p>
    Send a record with a unique marker through the source, then list the directory under the prefix
    for a new file and look for the marker in it.
  </p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>the server's host key is not the one configured</strong></dt>
    <dd>
      The server showed another key than <code>hostKey</code>. Check that <code>host</code> names
      the right server and that <code>hostKey</code> is its current key.
    </dd>
    <dt><strong>the server refused the password given for user …</strong></dt>
    <dd>
      With a private key, the text names it in place of the password. Check <code>username</code>,
      and that the credential holds that user's password, or a private key whose public half the
      server accepts for that user.
    </dd>
    <dt><strong>no connection within 30s</strong></dt>
    <dd>
      Connecting and signing in did not finish. Check that the worker host can reach
      <code>host</code> on <code>port</code>.
    </dd>
    <dt><strong>writing … no answer within …</strong></dt>
    <dd>
      The server stopped answering while the worker made a folder, wrote the named file or renamed
      it. With <code>stallAfterSecs</code> at 30 or less, an unreachable host shows this too, before the
      30s connection limit runs out.
    </dd>
    <dt><strong>Writing fails with a file not found</strong></dt>
    <dd>Create <code>directory</code> on the server.</dd>
    <dt><strong>Writing fails with permission denied</strong></dt>
    <dd>
      Check that the user may create files and folders in <code>directory</code> and rename files there.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/destinations">Destinations</a></li>
    <li><a href="/docs/sources/sftp">SFTP source</a></li>
    <li><a href="/docs/destinations/s3">Amazon S3 destination</a></li>
    <li><a href="/docs/secrets">Secrets & credentials</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
