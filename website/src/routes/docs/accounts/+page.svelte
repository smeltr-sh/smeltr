<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  const sections = [
    { id: 'accounts', title: 'Add an account' },
    { id: 'keys', title: 'Call the API with a key' },
    { id: 'manage', title: 'Find, change, disable or sign out' },
    { id: 'sessions', title: 'Change session lifetimes' },
    { id: 'recovery', title: 'Recover access when nobody can sign in' },
    { id: 'oidc', title: 'Sign in through single sign-on' },
    { id: 'roles', title: 'Give people access with roles' },
  ];
</script>

<GuideLayout
  title="Accounts and access"
  description="Accounts and access for Smeltr administrators."
  {sections}
>
  <h1>Accounts and access</h1>
  <h2 id="accounts">Add an account</h2>
  <p>
    Every account signs in one way: with a <strong>Password</strong>, through <strong>SSO</strong>,
    or with a <strong>Key</strong>. However it signs in, it can do only what it holds. You add
    password and key accounts here; an SSO account is created the first time its person signs in
    through the provider.
  </p>
  <ol>
    <li>Open Admin → Accounts and select <strong>+ Add account</strong>.</li>
    <li>Under Signs in with, choose <strong>Password</strong> or <strong>Key</strong>.</li>
    <li>Enter a Display name, and for a password account a Username.</li>
    <li>
      Turn on <strong>Administrator</strong> for an account that holds everything, or tick the
      <a href="#roles">roles</a> it holds. An account with neither can sign in but sees nothing.
    </li>
    <li>Choose <strong>Add</strong>.</li>
    <li>
      For a password account, securely hand over the one-time password before dismissing the panel.
      The recipient must choose their own password at first sign-in. If it is lost, reset it; you
      cannot read it back. A key account's row opens, ready for its first <a href="#keys">key</a>.
    </li>
  </ol>
  <p>
    Smeltr keeps at least one enabled password account that is an administrator, as the way in when
    the provider is down, so it refuses to disable that account or take away its Administrator.
    Display names need not be unique.
  </p>
  <h2 id="keys">Call the API with a key</h2>
  <p>
    A script calls Smeltr's API as a key account, with one of its keys. Only an administrator makes
    a key, and only a key account holds one.
  </p>
  <ol>
    <li>
      Choose <strong>Manage</strong> on the key account's row, then <strong>+ New key</strong>.
    </li>
    <li>
      Enter a Label that says where the key is used. To have it stop working on its own, enter how
      many days it lasts in Expires in (days); leave that empty for a key that never expires.
    </li>
    <li>
      Choose <strong>Create key</strong>, and copy the key before dismissing the panel: it is shown
      only once. The script sends it as <code>Authorization: Bearer &lt;key&gt;</code>.
    </li>
  </ol>
  <p>
    The Keys table shows when each key was created, when it expires and when it was last used. An
    expired key is refused. <strong>Revoke</strong> stops a key from its next request. A key account holds
    any number of keys, so make the new one before you revoke the old.
  </p>
  <h2 id="manage">Find, change, disable or sign out</h2>
  <p>
    Search Accounts by display name, username or the provider's login, or choose
    <strong>Password</strong>, <strong>SSO</strong> or <strong>Key</strong> to list only those
    accounts. Then choose <strong>Manage</strong> on the account's row. It offers what that kind of account
    has.
  </p>
  <ul>
    <li><strong>Rename:</strong> change the display name only. Every account.</li>
    <li>
      <strong>Change access:</strong> turn Administrator on or off, or change the roles it holds. A password
      or key account; an SSO account holds the roles of its provider groups.
    </li>
    <li>
      <strong>Reset password:</strong> sign out existing sessions and issue a temporary password. A password
      account.
    </li>
    <li>
      <strong>Revoke sessions:</strong> sign out without changing the password. This does not block future
      access. A password or SSO account.
    </li>
    <li>
      <strong>Disable account:</strong> sign out, revoke every key, and block access; keep the account’s
      name on its past changes. Every account.
    </li>
    <li>
      <strong>Enable account:</strong> allow new sign-ins without restoring old sessions or keys.
    </li>
  </ul>
  <p>
    Smeltr asks you to confirm before it resets a password or disables an account. Your own row has
    no Reset password: a password account uses Change password in the account menu at the top right,
    which shows your name. A single sign-on account changes its password at the provider.
  </p>
  <h2 id="sessions">Change session lifetimes</h2>
  <ol>
    <li>Open Admin → System → Sessions.</li>
    <li>
      Set Idle timeout and Maximum session age, observing the units displayed alongside each field.
    </li>
    <li>Select <strong>Save sessions</strong>.</li>
  </ol>
  <p>
    By default, a session ends after 30 minutes without activity or 12 hours after sign-in. You can
    set both limits from two minutes to 30 days. Idle timeout cannot exceed Maximum session age.
    Changes apply on each session's next request; shortening a limit can sign people out
    immediately. A leader restart clears all in-memory sessions, not account records.
  </p>
  <h2 id="recovery">Recover access when nobody can sign in</h2>
  <p>
    If you've lost your admin password and nobody else can sign in, recover access on the leader
    host.
  </p>
  <ol>
    <li>Stop the leader and verify there is no other instance using its data directory.</li>
    <li>
      Start the same binary, configuration and data under the same account, once, with the following
      environment setting.
    </li>
  </ol>
  <pre><code
      >SMELTR_RECOVER_ADMIN=1 SMELTR_CONFIG_FILE=/absolute/path/to/smeltr.toml /absolute/path/to/smeltr</code
    ></pre>
  <p>
    Read the username and temporary password from the leader’s process console or service log, then
    sign in and change the password. Remove SMELTR_RECOVER_ADMIN from the service environment before
    the next restart; leaving it enabled resets the account again every time. This recovery
    procedure requires access to the leader host.
  </p>
  <h2 id="oidc">Sign in through single sign-on</h2>
  <p>
    People can sign in through your organization's identity provider over <a
      href="https://openid.net/specs/openid-connect-core-1_0.html">OpenID Connect</a
    > (OIDC). Password accounts keep working whatever is set here, and Smeltr keeps at least one that
    is an administrator, as a way in when the provider is down.
  </p>
  <ol>
    <li>
      Open Admin → Accounts → Single sign-on and copy the <strong>Redirect URI</strong>. It is the
      leader's own address followed by <code>/api/auth/oidc/callback</code>.
    </li>
    <li>
      At your provider, register a web application (a confidential client) with that redirect URI.
      Have the provider name each person's groups in a <code>groups</code> claim of the ID token.
    </li>
    <li>
      Back in Smeltr, turn on <strong>Enabled</strong> and enter the provider's
      <strong>Issuer URL</strong>, the <strong>Client ID</strong> and the
      <strong>Client secret</strong>. If your provider names groups in another claim, set it under
      Advanced.
    </li>
    <li>Select <strong>Save</strong>.</li>
  </ol>
  <p>
    The issuer must be an <code>https://</code> URL, or an <code>http://</code> URL to
    <code>localhost</code> or a loopback address, for a local test provider. Saving reads the provider's
    discovery document and keys, so a wrong issuer is refused when you save. The client and the groups
    claim are only tried at the first sign-in, so sign in once to check them. The client secret is stored
    encrypted and never shown again; changing the issuer or client ID forgets it, so enter the new client's
    secret with them.
  </p>
  <p>
    The sign-in screen then offers <strong>Sign in with SSO</strong>, with
    <strong>Sign in with username</strong> beside it for password accounts. The first time somebody
    signs in through the provider, Smeltr creates their account. Accounts lists them with SSO as
    their sign-in, and the email or username the provider gives them under their name. To sign in, a
    person must belong to at least one group that has a role. <strong>Refused sign-ins</strong> on the
    Roles tab lists everyone turned away, with the groups the provider sent.
  </p>
  <p>
    The provider owns an SSO account's password, so it has no Reset password. To stop somebody
    signing in, remove them from every group that has a role, or disable their account in Smeltr.
  </p>
  <p>
    Account, role and session actions take effect immediately; they are not pipeline changes waiting
    for branch deployment.
  </p>
  <h2 id="roles">Give people access with roles</h2>
  <p>
    An account can do only what its roles give it, or everything as an administrator. A role gives
    one level in each area: the shared configuration or a worker group. An SSO account holds the
    role named for each of its groups in the provider's groups claim. A password or key account
    holds the roles you give it under Change access.
  </p>
  <ul>
    <li><strong>viewer</strong> reads the area's configuration.</li>
    <li>
      <strong>editor</strong> also saves to it on a branch, and captures, previews and reads its samples.
    </li>
    <li><strong>deployer</strong> also approves and deploys changes to it.</li>
  </ul>
  <p>
    Anybody holding an area also reads the shared configuration, which every group draws on. The Map
    and Status pages show only the groups a person holds. A change is shown only to someone who can
    view every area it touches; changing it takes an editor, and deploying it a deployer, in each of
    them. A change to shared counts as shared alone. Rolling back from History, or selecting
    <strong>Release</strong>, moves every group, so it takes a deployer in shared and in every
    group. Group lists name only the groups a person holds. A role marked
    <strong>Administrator</strong>
    gives everything, Admin included.
  </p>
  <ol>
    <li>Open Admin → Accounts → Roles and select <strong>+ Add role</strong>.</li>
    <li>
      Enter the group exactly as the provider names it. For a role you give only to password or key
      accounts, any name will do.
    </li>
    <li>Choose a level for each area, or tick Administrator, and select <strong>Save</strong>.</li>
  </ol>
  <p>
    A change to a role applies on each holder's next request. A change to somebody's groups at the
    provider applies within a few minutes, while they stay signed in, when the provider issues
    refresh tokens and its refreshes carry an ID token; otherwise at their next sign-in. A refresh
    the provider refuses, or one that leaves them in no group with a role, signs them out. To shut
    somebody out at once, disable their account in Accounts, or remove them from those groups at the
    provider and then revoke their sessions. Accounts lists each account's roles beside its name.
  </p>
</GuideLayout>
