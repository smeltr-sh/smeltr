<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import examples from '#lib/connector-examples.json';
  const sections = [
    { id: 'secrets', title: 'Create and rotate a secret' },
    { id: 'encryption-key', title: 'Encryption key' },
    { id: 'identities', title: 'Connection identities' },
    { id: 'kafka', title: 'Kafka security' },
  ];
</script>

<GuideLayout
  title="Secrets & credentials"
  description="Store connector credentials and rotate secrets across Smeltr deployments."
  {sections}
>
  <h1>Secrets &amp; credentials</h1>
  <p>
    Use a secret to give a connector access to a data system. Choose whether its value lives on the
    leader or on the worker host, then reference it in your configuration.
  </p>
  <h2 id="secrets">Create and rotate a secret</h2>
  <ol>
    <li>
      Open Admin → Secrets and choose <strong>+ Add secret</strong>. Enter Name and Description.
      Only an administrator creates or changes a secret; an editor picks a stored secret by name in
      a source or destination form.
    </li>
    <li>
      Choose <strong>Value lives on</strong>: <strong>The leader</strong> stores an encrypted value.
      <strong>The worker host</strong> reads the value from an environment variable on each worker. You
      cannot change where the value lives after creating the secret.
    </li>
    <li>
      For a value stored on the leader, supply Value. Then select <strong>Add</strong>. You cannot
      read a value back afterward.
    </li>
    <li>
      Reference the secret in the relevant connector configuration and review/deploy the branch. On
      Status, check for missing-credential warnings.
    </li>
    <li>
      To rotate a leader-stored secret, select <strong>New value</strong>, enter it and choose
      <strong>Store</strong>. Review the current branch and any reference/version changes, deploy,
      and confirm the Running column on Admin → Secrets. Stored does not mean deployed.
    </li>
  </ol>
  <p>
    <strong>Check</strong> tests a candidate value without revealing the stored value. Keep old external
    credentials valid until deployments and rollback requirements no longer need them. Keep secrets that
    deployed configurations still use. Change a secret through New value; replacing the secrets encryption
    key would stop the leader from decrypting existing values.
  </p>
  <p>
    A stored value belongs to no branch. The leader keeps the newest values of each secret, as many
    as <code>leader.secrets.versions</code> allows, whichever branch you were working on. Deleting a branch
    does not remove a value stored while working on it: an older value it pushed out is gone, and a rollback
    to a deployment that used that value fails, unless a running deployment still uses it.
  </p>
  <h2 id="encryption-key">Encryption key</h2>
  <p>
    Admin → Secrets → <strong>Encryption key</strong> shows the key the leader encrypts stored secrets
    with, and the file it reads it from. The leader encrypts each value with AES-256-GCM. It also encrypts
    the Git remote token, the single sign-on client secret and the AI agents API key. Keep a backup of
    the file: without it, a restored leader cannot open the stored secrets. Kept apart from the leader
    data, a stolen data backup reveals no secret.
  </p>
  <p>
    If the file is missing, or is not the one that encrypted the stored secrets, the leader cannot
    open them. Issues shows the secrets encryption key as failed, and no secret can be stored or
    changed. Restore the original file and restart the leader. If it is lost for good, choose
    <strong>Regenerate key</strong>: Issues then lists each secret and setting to enter again, and
    each clears once you store a new value for it.
  </p>
  <h2 id="identities">Connection identities</h2>
  <p>
    A connection identity contains the certificates a worker trusts when connecting to a data system
    and, when needed, the client certificate it uses to identify itself. Create it under Build →
    Resources → Connection identities, then select it in the source or destination's <strong
      >identity</strong
    > field.
  </p>
  <ul>
    <li>
      For a private certificate authority (CA), supply its public certificates in PEM format (text
      files containing certificate blocks). A source or destination trusts them in addition to the
      public roots: Mozilla's and the worker host's own.
    </li>
    <li>
      To trust only your own authorities, turn off <strong>trust public roots</strong> under Advanced.
      A receiver always admits by its identity's authorities alone.
    </li>
    <li>
      For client authentication, supply the client certificate chain and a secret reference for its
      private key. Provide the certificate and key together.
    </li>
    <li>
      An identity can contain trust authorities, a client certificate, or both. With no identity
      selected, the connector trusts the public roots and presents no client certificate.
    </li>
  </ul>
  <p>
    Review and deploy the identity with the connector that uses it. Check certificate names and
    secret availability from each worker. You configure leader/browser certificates separately in <a
      href="/docs/certificates">Certificates &amp; TLS</a
    >.
  </p>

  <h2 id="kafka">Kafka security</h2>
  <p>
    The same security controls apply to <a href="/docs/sources/kafka">Kafka sources</a> and
    <a href="/docs/destinations/kafka">Kafka destinations</a>.
  </p>
  <ol>
    <li>
      Select the broker's security protocol. <code>SSL</code> uses TLS; <code>SASL_SSL</code> adds
      SASL authentication over TLS. <code>PLAINTEXT</code> is the default when you set no protocol.
    </li>
    <li>
      For SASL, select the mechanism your brokers use. PLAIN and SCRAM need a username and password.
      OAUTHBEARER uses the OIDC token endpoint, client id, client secret, and optional scope.
    </li>
    <li>
      Select named secrets for password or client-secret fields. Make worker-host credentials
      available in every worker process that uses the connection.
    </li>
    <li>
      Select a connection identity for private trust authorities or a client certificate. An
      identity requires a TLS protocol, and you must not duplicate its TLS settings in the extra
      properties.
    </li>
  </ol>
  <p>
    Kerberos (GSSAPI) requires a build that supports it. Configure the service name and principal; a
    Linux keytab path must exist on each worker host. A Windows worker uses its own ticket cache.
  </p>
  <p>
    For example, you can add these fields to a Kafka source or destination. Set the worker-host
    environment variable <code>KAFKA_PASSWORD</code> to the password and replace the username. The broker
    must support SCRAM-SHA-512 over TLS:
  </p>
  <pre><code>{JSON.stringify(examples.kafkaSecurity, null, 2)}</code></pre>
  <p>
    Strings in <code>credentials</code> name environment variables; they are not literal passwords.
    Select a leader-stored secret in the form to use a versioned secret reference instead. Keep
    ordinary settings in <code>properties</code>.
  </p>
</GuideLayout>
