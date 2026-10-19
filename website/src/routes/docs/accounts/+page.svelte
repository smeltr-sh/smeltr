<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  const sections = [
    { id: 'accounts', title: 'Create a local account' },
    { id: 'manage', title: 'Reset, disable or sign out' },
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
  <h2 id="accounts">Create a local account</h2>
  <p>
    A local account signs in with a username and password and is always an administrator. Give
    everybody else access through single sign-on and <a href="#roles">roles</a>.
  </p>
  <ol>
    <li>Open Admin → Accounts → Users and select <strong>+ Add user</strong>.</li>
    <li>Enter Display name and Username, then choose <strong>Add</strong>.</li>
    <li>
      Securely hand over the one-time password before dismissing the panel. The recipient must
      choose their own password at first sign-in. If it is lost, reset it; you cannot read it back.
    </li>
  </ol>
  <h2 id="manage">Reset, disable or sign out</h2>
  <p>
    Choose <strong>Manage</strong> on the person's row.
  </p>
  <ul>
    <li>
      <strong>Reset password:</strong> sign out existing sessions and issue a temporary password.
    </li>
    <li>
      <strong>Disable account:</strong> sign out and block access; keep the account’s name on its past
      changes.
    </li>
    <li><strong>Enable account:</strong> allow new sign-ins without restoring old sessions.</li>
    <li>
      <strong>Revoke sessions:</strong> sign out without changing the password. This does not block future
      access.
    </li>
    <li><strong>Rename:</strong> change the display name only.</li>
  </ul>
  <p>
    Smeltr asks you to confirm before it resets a password or disables an account. Your own row has
    no Reset password: a local account uses Change password in the account menu at the top right,
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
    > (OIDC). Local accounts keep working whatever is set here, so keep at least one local administrator
    as a way in when the provider is down.
  </p>
  <ol>
    <li>
      Open Admin → System → Single sign-on and copy the <strong>Redirect URI</strong>. It is the
      leader's own address followed by <code>/api/auth/oidc/callback</code>.
    </li>
    <li>
      At your provider, register a web application (a confidential client) with that redirect URI.
      Have the provider name each person's groups in a <code>groups</code> claim of the ID token, and
      create a group for the people who may sign in to Smeltr.
    </li>
    <li>
      Back in Smeltr, turn on <strong>Enabled</strong> and enter the provider's
      <strong>Issuer URL</strong>, the <strong>Client ID</strong> and
      <strong>Client secret</strong>, and the <strong>Required group</strong>. If your provider
      names groups in another claim, set it under Advanced.
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
    <strong>Sign in with username</strong> beside it for local accounts. The first time somebody signs
    in through the provider, Smeltr creates their account. Somebody outside the required group is refused.
    Accounts lists them with SSO as their sign-in, and the email or username the provider gives them under
    their name.
  </p>
  <p>
    The provider owns an SSO account's password, so it has no Reset password. To stop somebody
    signing in, remove them from the required group, or disable their account in Smeltr.
  </p>
  <p>
    Account, role and session actions take effect immediately; they are not pipeline changes waiting
    for branch deployment.
  </p>
  <h2 id="roles">Give people access with roles</h2>
  <p>
    An SSO account can do only what its roles give it. A role is named for one group in the
    provider's groups claim, and gives whoever holds that group one level in each area: the shared
    configuration or a worker group.
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
    <li>Enter the group exactly as the provider names it.</li>
    <li>Choose a level for each area, or tick Administrator, and select <strong>Save</strong>.</li>
  </ol>
  <p>
    A change to a role applies on each holder's next request. A change to somebody's groups at the
    provider applies within a few minutes, while they stay signed in, when the provider issues
    refresh tokens and its refreshes carry an ID token; otherwise at their next sign-in. A refresh
    the provider refuses, or one that drops the required group, signs them out. To shut somebody out
    at once, disable their account in Users, or remove them from the required group at the provider
    and then revoke their sessions. Users lists each person's roles beside their name.
  </p>
</GuideLayout>
