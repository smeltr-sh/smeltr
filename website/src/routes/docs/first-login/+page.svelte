<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  const sections = [{ id: 'credentials', title: 'Sign in with the printed credentials' }];
</script>

<GuideLayout
  title="First login"
  description="Sign in to a new Smeltr leader for the first time."
  {sections}
>
  <h1>First login</h1>
  <p>
    A new leader creates one administrator on first start. Follow these steps once, after
    <a href="/docs/install-leader/binary">installing the binary</a>,
    <a href="/docs/install-leader/container">running the container</a> or
    <a href="/docs/install-leader/kubernetes">deploying on Kubernetes</a>.
  </p>
  <h2 id="credentials">Sign in with the printed credentials</h2>
  <ol>
    <li>
      Read the startup console of the leader you just started. A fresh account store prints a
      username and one-time password to the process console (stderr), not the fleet log viewer. Look
      for output in this form; the values below are placeholders, not credentials you can use:
      <pre><code
          >smeltr: this leader had no accounts, so one was created.
           username: admin
  one-time password: &lt;generated one-time password&gt;
It is shown once and is not recoverable. Signing in with it requires choosing a new one.</code
        ></pre>
    </li>
    <li>
      Open the configured public origin URL in a browser: <code>http://127.0.0.1:4000</code> for the local
      examples.
    </li>
    <li>
      Sign in with the printed credentials and choose your own password when prompted. Protect any
      service-console logs containing the temporary password.
    </li>
    <li>
      Open Admin → Accounts and create named administrators as described in <a href="/docs/accounts"
        >Accounts and access</a
      >.
    </li>
    <li>
      For standalone mode, open Status and wait for the automatically enrolled worker in
      <code>default</code> to appear Online. Check that its running commit matches
      <code>main</code>. For a leader-only installation,
      <a href="/docs/enroll-worker/host">enroll a separate worker</a> first.
    </li>
  </ol>
</GuideLayout>
