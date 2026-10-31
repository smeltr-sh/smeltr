<script lang="ts">
  import SqlSourceGuide from '#lib/components/SqlSourceGuide.svelte';
  import ProductScreenshot from '#lib/components/ProductScreenshot.svelte';
  import examples from '#lib/connector-examples.json';
  const statement = `SELECT * FROM (
${examples.mysqlSource.query}
) AS q
WHERE q.\`event_id\` > ?
ORDER BY q.\`event_id\`
LIMIT 5000`;
</script>

<SqlSourceGuide
  type="mysql"
  name="MySQL or MariaDB"
  title="MySQL and MariaDB source"
  description="Read the rows a query returns from a MySQL or MariaDB database every poll: what to set up, every setting, how it finds new rows, and the errors it reports."
  example={examples.mysqlSource}
  {statement}
  clock="NOW(6)"
>
  {#snippet lede()}
    <h1>MySQL and MariaDB</h1>
    <p class="lede">
      Runs a query you write against a MySQL or MariaDB database every poll, and reads each row of
      its result as one record.
    </p>
    <p>
      Use it for what an application keeps in its own tables, such as an audit table or a sign-in
      history. The source reads only what your query returns, and its mode decides which of those
      rows are new. One source kind serves both servers.
    </p>
  {/snippet}

  {#snippet exampleNote()}
    <p>
      Reads the table <code>events</code> in the database <code>auth</code> on
      <code>db.internal</code>, as the user <code>smeltr_reader</code>, whose password is in the
      worker-host variable <code>MYSQL_PASSWORD</code>. It begins at the largest
      <code>event_id</code>
      on its first poll, and each poll reads the rows whose <code>event_id</code> is past the last one
      sent.
    </p>
  {/snippet}

  {#snippet screenshot()}
    <ProductScreenshot
      source="mysql-source-form"
      height={2200}
      crop={{ x: 608, y: 226, width: 1664, height: 1748 }}
      alt="The MySQL / MariaDB form on the Sources page: id auth-events, host db.internal, database auth, user smeltr_reader, and the query SELECT event_id, occurred_at, user_name FROM events."
      caption="The same source in the Sources page's add dialog, before you pick its password or run its query."
    />
  {/snippet}

  {#snippet beforeYouStart()}
    <ul>
      <li>
        Create a user that can connect from the workers' addresses and read the tables the query
        reads, and nothing more. See
        <a href="https://dev.mysql.com/doc/refman/8.4/en/create-user.html">CREATE USER</a> and
        <a href="https://dev.mysql.com/doc/refman/8.4/en/grant.html">GRANT</a> for MySQL, or
        <a href="https://mariadb.com/kb/en/create-user/">CREATE USER</a> and
        <a href="https://mariadb.com/kb/en/grant/">GRANT</a> for MariaDB.
      </li>
      <li>Allow workers to reach the server's port.</li>
      <li>
        With <code>tls</code> <code>require</code>, the default, the server needs encrypted
        connections on, with a certificate that names <code>host</code>. See
        <a href="https://dev.mysql.com/doc/refman/8.4/en/using-encrypted-connections.html"
          >MySQL: Using encrypted connections</a
        >
        or
        <a href="https://mariadb.com/kb/en/secure-connections-overview/"
          >MariaDB: Secure connections overview</a
        >. For a certificate from a private authority, paste it into the source's
        <a href="/docs/certificates#connectors">authorities</a>.
      </li>
      <li>Put the password in a <a href="/docs/secrets">credential</a>.</li>
      <li>
        For Incrementing or Updated at, index the first field of <code>position</code>, so each poll
        reads only the new rows. See
        <a href="https://dev.mysql.com/doc/refman/8.4/en/create-index.html">CREATE INDEX</a>.
      </li>
    </ul>
  {/snippet}

  {#snippet types()}
    <p>
      <strong>Run</strong> suggests each field's type from its column's type. A column of a type not listed
      here, such as a geometry, is read as its text.
    </p>
    <div class="table-scroll">
      <table>
        <thead
          ><tr><th scope="col">MySQL or MariaDB type</th><th scope="col">Field type</th></tr></thead
        >
        <tbody>
          <tr><td><code>tinyint(1)</code>, <code>boolean</code></td><td><code>bool</code></td></tr>
          <tr
            ><td
              ><code>tinyint</code>, <code>smallint</code>, <code>mediumint</code>,
              <code>int</code>,
              <code>bigint</code>, <code>year</code>, <code>bit</code></td
            ><td><code>i64</code></td></tr
          >
          <tr><td><code>float</code>, <code>double</code></td><td><code>f64</code></td></tr>
          <tr
            ><td
              ><code>char</code>, <code>varchar</code>, <code>text</code>, <code>decimal</code>,
              <code>enum</code>, <code>set</code></td
            ><td><code>str</code></td></tr
          >
          <tr
            ><td><code>date</code>, <code>datetime</code>, <code>timestamp</code></td><td
              ><code>datetime</code></td
            ></tr
          >
          <tr><td><code>time</code></td><td><code>duration</code></td></tr>
          <tr><td><code>json</code> on MySQL</td><td><code>variant</code></td></tr>
          <tr
            ><td><code>binary</code>, <code>varbinary</code>, <code>blob</code></td><td
              ><code>bytes</code></td
            ></tr
          >
        </tbody>
      </table>
    </div>
    <p>
      A <code>decimal</code> is read as its exact decimal text. A zero date, such as
      <code>0000-00-00</code>, is read as null. A MariaDB <code>json</code> column reads as
      <code>str</code>. A <code>tinyint(1)</code> that holds other than 0 or 1 needs an
      <code>i64</code>
      field, not <code>bool</code>. An unsigned <code>bigint</code> or <code>bit(64)</code> past
      9223372036854775807 is read as its decimal text, so it needs a <code>str</code> field.
    </p>
  {/snippet}

  {#snippet troubleshooting()}
    <dt><strong>The MySQL password is in …, which this worker does not have</strong></dt>
    <dd>
      The credential is not on the worker that holds the source. See
      <a href="/docs/secrets">Credentials</a>.
    </dd>
    <dt><strong>Run fails with a refusal to read a table</strong></dt>
    <dd>Grant the user <code>SELECT</code> on the table.</dd>
  {/snippet}

  {#snippet seeAlso()}
    <li><a href="/docs/sources/postgres">PostgreSQL source</a></li>
  {/snippet}
</SqlSourceGuide>
