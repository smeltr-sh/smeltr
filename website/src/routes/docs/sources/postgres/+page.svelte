<script lang="ts">
  import SqlSourceGuide from '#lib/components/SqlSourceGuide.svelte';
  import ProductScreenshot from '#lib/components/ProductScreenshot.svelte';
  import examples from '#lib/connector-examples.json';
  const statement = `SELECT * FROM (
${examples.postgresSource.query}
) AS q
WHERE q."event_id" > $1
ORDER BY q."event_id"
LIMIT 5000`;
</script>

<SqlSourceGuide
  type="postgres"
  name="PostgreSQL"
  title="PostgreSQL source"
  description="Read the rows a query returns from a PostgreSQL database every poll: what to set up, every setting, how it finds new rows, and the errors it reports."
  example={examples.postgresSource}
  {statement}
  clock="now()"
>
  {#snippet lede()}
    <h1>PostgreSQL</h1>
    <p class="lede">
      Runs a query you write against a PostgreSQL database every poll, and reads each row of its
      result as one record.
    </p>
    <p>
      Use it for what an application keeps in its own tables, such as an audit table or a sign-in
      history. The source reads only what your query returns, and its mode decides which of those
      rows are new.
    </p>
  {/snippet}

  {#snippet exampleNote()}
    <p>
      Reads the table <code>auth.events</code> in the database <code>auth</code> on
      <code>db.internal</code>, as the user <code>smeltr_reader</code>, whose password is in the
      worker-host variable <code>PG_PASSWORD</code>. It begins with the rows added after its first
      poll, and each poll reads the rows whose <code>event_id</code> is past the last one sent.
    </p>
  {/snippet}

  {#snippet screenshot()}
    <ProductScreenshot
      source="postgres-source-form"
      height={2200}
      crop={{ x: 608, y: 226, width: 1664, height: 1748 }}
      alt="The PostgreSQL form on the Sources page: id auth-events, host db.internal, database auth, user smeltr_reader, and the query SELECT event_id, occurred_at, user_name FROM auth.events."
      caption="The same source in the Sources page's add dialog, before you pick its password or run its query."
    />
  {/snippet}

  {#snippet beforeYouStart()}
    <ul>
      <li>
        Create a user that can connect to the database and read the tables the query reads, and
        nothing more. See PostgreSQL's
        <a href="https://www.postgresql.org/docs/current/sql-createrole.html">CREATE ROLE</a> and
        <a href="https://www.postgresql.org/docs/current/sql-grant.html">GRANT</a>.
      </li>
      <li>
        Let the user connect from the workers' addresses in
        <a href="https://www.postgresql.org/docs/current/auth-pg-hba-conf.html">pg_hba.conf</a>, and
        allow workers to reach the server's port.
      </li>
      <li>
        With <code>tls</code> <code>require</code>, the default, the server needs
        <a href="https://www.postgresql.org/docs/current/ssl-tcp.html">SSL</a> on, with a
        certificate that names <code>host</code>. For a certificate from a private authority, paste
        it into the source's <a href="/docs/certificates#connectors">authorities</a>.
      </li>
      <li>Put the password in a <a href="/docs/secrets">credential</a>.</li>
      <li>
        For Incrementing or Updated at, index the first field of <code>position</code>, so each poll
        reads only the new rows. See
        <a href="https://www.postgresql.org/docs/current/sql-createindex.html">CREATE INDEX</a>.
      </li>
    </ul>
  {/snippet}

  {#snippet types()}
    <p>
      <strong>Run</strong> suggests each field's type from its column's type. A column of a type not listed
      here is read as its text.
    </p>
    <div class="table-scroll">
      <table>
        <thead><tr><th scope="col">PostgreSQL type</th><th scope="col">Field type</th></tr></thead>
        <tbody>
          <tr><td><code>boolean</code></td><td><code>bool</code></td></tr>
          <tr
            ><td
              ><code>smallint</code>, <code>integer</code>, <code>bigint</code>,
              <code>oid</code></td
            ><td><code>i64</code></td></tr
          >
          <tr><td><code>real</code>, <code>double precision</code></td><td><code>f64</code></td></tr
          >
          <tr
            ><td
              ><code>text</code>, <code>varchar</code>, <code>char</code>, <code>numeric</code>,
              <code>uuid</code>, <code>inet</code>, <code>cidr</code>, an enum</td
            ><td><code>str</code></td></tr
          >
          <tr
            ><td><code>timestamptz</code>, <code>timestamp</code>, <code>date</code></td><td
              ><code>datetime</code></td
            ></tr
          >
          <tr><td><code>time</code>, <code>interval</code></td><td><code>duration</code></td></tr>
          <tr><td><code>json</code>, <code>jsonb</code></td><td><code>variant</code></td></tr>
          <tr><td><code>bytea</code></td><td><code>bytes</code></td></tr>
          <tr
            ><td>An array</td><td
              ><code>array&lt;…&gt;</code> of its element's type, or <code>variant</code></td
            ></tr
          >
          <tr><td>A domain</td><td>Its base type's</td></tr>
        </tbody>
      </table>
    </div>
    <p>
      A <code>numeric</code> is read as its exact decimal text. An <code>interval</code> counts a
      month as 30 days. <strong>Run</strong> suggests an array of one dimension; declare an
      <code>array&lt;…&gt;</code> for each further one, or <code>variant</code>.
    </p>
  {/snippet}

  {#snippet troubleshooting()}
    <dt><strong>The PostgreSQL password is in …, which this worker does not have</strong></dt>
    <dd>
      The credential is not on the worker that holds the source. See
      <a href="/docs/secrets">Credentials</a>.
    </dd>
    <dt><strong>Run fails with a refusal to read a table</strong></dt>
    <dd>Grant the user <code>SELECT</code> on the table, and <code>USAGE</code> on its schema.</dd>
  {/snippet}

  {#snippet seeAlso()}
    <li><a href="/docs/sources/mysql">MySQL and MariaDB source</a></li>
    <li>
      <a href="https://www.postgresql.org/docs/current/user-manag.html"
        >PostgreSQL: Database roles</a
      >
    </li>
  {/snippet}
</SqlSourceGuide>
