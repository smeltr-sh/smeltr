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
    { id: 'before-you-start', title: 'Before you start' },
    { id: 'settings', title: 'Settings' },
    { id: 'fields-it-gives', title: 'Fields it gives' },
    { id: 'how-it-works', title: 'How it works' },
    { id: 'verify', title: 'Verify' },
    { id: 'troubleshooting', title: 'Troubleshooting' },
    { id: 'see-also', title: 'See also' },
  ];
  const json = (value: unknown) => JSON.stringify(value, null, 2);
</script>

<GuideLayout
  title="SFTP source"
  description="Read the files in a directory on an SFTP server: an example, every setting, the fields it gives, and what to have ready."
  {sections}
>
  <h1>SFTP</h1>
  <p class="lede">
    Reads each file under a prefix of one directory on an SFTP server, its subdirectories included,
    and splits it into records.
  </p>
  <p>
    The source finds files by listing the directory every poll interval. It reads a file once two
    listings in a row find it unchanged, since an upload to it may still be under way. Only one
    worker of each group reads this source at a time.
  </p>

  <h2 id="example">Example</h2>
  <p>
    Reads each file under <code>/srv/export/incoming</code> one line per record, signing in with a
    private key, and renames each under <code>done/</code> once Smeltr delivers every record.
  </p>
  <pre><code>{@html highlight(json(examples.sftpSource))}</code></pre>
  <ProductScreenshot
    source="sftp-source-form"
    height={3000}
    crop={{ x: 608, y: 458, width: 1664, height: 2084 }}
    alt="The SFTP form on the Sources page: id partner-drop, host sftp.example.com, username smeltr, a private key, the host key, directory /srv/export, prefix incoming/, after commit move to done/."
    caption="The same source in the Sources page's add dialog, before you pick its private key."
  />

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>
      Have a user on the server that can list and read the directory and its subdirectories. With
      <code>afterCommit</code> <code>delete</code> or <code>move</code>, the user also needs write
      access to the directory, to remove or rename files there.
    </li>
    <li>
      Put the user's password, or its private key, in a <a href="/docs/secrets">credential</a>. The
      private key is an unencrypted RSA, ed25519 or ECDSA key in OpenSSH's format.
    </li>
    <li>
      Have the server's public host key, as one line as OpenSSH writes it:
      <code>ssh-ed25519 AAAA…</code>. It is in the server's
      <code>/etc/ssh/ssh_host_ed25519_key.pub</code>. <code>ssh-keyscan</code> also prints it:
      <pre><code>ssh-keyscan -t ed25519 sftp.example.com</code></pre>
      Drop the host name its line starts with, and check the key against the server's file, since {' '}<code
        >ssh-keyscan</code
      > trusts the network it asks over.
    </li>
  </ul>

  <h2 id="settings">Settings</h2>
  <p>It also takes the settings <a href="/docs/sources#settings">every source</a> takes.</p>
  <KindSettings group={SOURCES} type="sftp" />

  <h2 id="fields-it-gives">Fields it gives</h2>
  <p>
    <code>__source.bucket</code> holds the directory, and <code>__source.key</code> the file's path under
    it.
  </p>
  <FieldsTable names={kind(SOURCES, 'sftp').fields ?? []} />

  <h2 id="how-it-works">How it works</h2>
  <p>
    Every <code>pollIntervalSecs</code> the source lists <code>directory</code> and each
    subdirectory that may hold a path starting with <code>prefix</code>. It reads regular files
    only: it skips a link. A file's version is its size and modification time. The source reads a
    file it finds new or changed once the next listing finds it the same, and reads it again whole
    if it changes after that. The source does not see a rewrite to the same size within the same
    second as a change.
  </p>
  <p>
    The source decompresses a gzip, zstd, framed snappy or framed lz4 file first and reads any other
    as it is.
  </p>
  <p>
    The source skips the files an <a href="/docs/destinations/sftp">SFTP destination</a> writes under
    a hidden temporary name before renaming them into place, so it reads a file such a destination writes
    only once it is whole.
  </p>
  <h3>One worker reads it</h3>
  <p>
    Every worker of the group runs the source, and the leader <strong>holds</strong> it for one of them,
    which reads it. The leader gives the hold to another worker after three heartbeat intervals without
    hearing from the holder, the holder's interval or the leader's, whichever is longer, and that worker
    resumes from the last progress the leader kept. That worker delivers again what the previous holder
    delivered after that progress, so a takeover can cost duplicates, never lost records.
  </p>
  <h3>After commit</h3>
  <p>
    Once Smeltr delivers every record of a file, the source does what <code>afterCommit</code> says:
  </p>
  <ul>
    <li>
      <code>keep</code> leaves the file and records its path and version in a
      <strong>ledger</strong>, which the worker reports to the leader. The next worker to read the
      source reads only what the ledger does not hold. Every poll lists everything under the prefix.
    </li>
    <li><code>delete</code> removes the file.</li>
    <li>
      <code>move</code> renames the file on the server, from under <code>prefix</code> to under
      <code>movePrefix</code>, creating the directories it needs. The rename replaces a file already
      at the new path.
    </li>
  </ul>
  <p>
    With <code>delete</code> or <code>move</code>, the directory is its own queue and the source
    keeps no ledger. Either one first checks that the file still holds the version the source read;
    the source leaves a file changed since in place and reads it again.
  </p>
  <p>
    A file handed out five times and never delivered counts as unreadable, and is not read again
    until it changes. With <code>keep</code> it goes into the ledger as read; with
    <code>delete</code>
    or
    <code>move</code> it stays where it is. With <code>delete</code> or <code>move</code> only the holder
    remembers it, so a restart or a new holder hands it out again.
  </p>
  <h3>Start</h3>
  <p>
    <code>start</code> <code>everything</code> reads every file the first listing finds.
    <code>new</code> records what the first listing finds as read, and reads only what arrives or
    changes after it. It needs <code>afterCommit</code> <code>keep</code>.
  </p>
  <p>
    The source's progress belongs to its name and to its user, host, port, directory and prefix.
    Renamed, or pointed anywhere else, it starts afresh.
  </p>
  <p>
    Once you remove the source and deploy, the leader forgets its progress, so a rollback that
    brings the source back starts it afresh.
  </p>
  <h3>The host key</h3>
  <p>
    The source connects only to the server holding the key in <code>hostKey</code>, and asks the
    server for a key of that kind only, so a server holding keys of several kinds shows the one
    pinned. The source refuses a server offering any other key before it signs in.
  </p>

  <h2 id="verify">Verify</h2>
  <p>
    Upload a file with a line you can recognize to the directory, under the prefix, then find that
    line at the destination once the next two listings have passed. With <code>delete</code> the
    file then leaves the directory; with <code>move</code> it is under <code>movePrefix</code>.
  </p>
  <p>
    On the Sources page, the <strong>worker</strong> column names the worker holding the source, and
    with <code>keep</code> how many files its ledger holds.
  </p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>the server's host key is not the one configured</strong></dt>
    <dd>
      <code>hostKey</code> is not the key this server offers. Copy it again from the server's
      <code>/etc/ssh/ssh_host_&lt;kind&gt;_key.pub</code>, of the kind you pin. A server whose key
      was replaced needs the new one.
    </dd>
    <dt><strong>the server refused the password given for user …</strong></dt>
    <dd>
      The server does not accept that password, or, for a private key, that key, for
      <code>username</code>. Check the credential's value, and for a key, that the user's
      <code>authorized_keys</code> on the server holds its public half.
    </dd>
    <dt><strong>no connection within 30s</strong></dt>
    <dd>
      Connecting and signing in did not finish. Check that the worker host can reach <code
        >host</code
      >
      on <code>port</code>, and that the port is the server's SSH port.
    </dd>
    <dt><strong>Nothing arrives</strong></dt>
    <dd>
      The source reads a file only once two listings in a row find it the same, so allow two poll
      intervals. Check that the file's path under <code>directory</code> starts with
      <code>prefix</code>, and the worker log for the reason a listing failed. With
      <code>keep</code>, a subdirectory under the prefix that the user may not read fails the whole
      listing; with <code>delete</code> or
      <code>move</code> the source skips it with a warning.
    </dd>
    <dt><strong>The worker column is empty</strong></dt>
    <dd>
      No worker of the group holds the source: check that a worker of the group is running and
      reaches the leader.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/sources">Sources</a></li>
    <li><a href="/docs/sources/s3-list">Amazon S3</a></li>
    <li><a href="/docs/sources/gcs-list">Google Cloud Storage</a></li>
    <li><a href="/docs/sources/azure-blob-list">Azure Blob Storage</a></li>
    <li><a href="/docs/destinations/sftp">SFTP destination</a></li>
    <li><a href="/docs/secrets">Secrets & credentials</a></li>
    <li><a href="/docs/delivery#recovery">Recover and replay</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
