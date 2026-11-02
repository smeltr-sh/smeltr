#!/bin/sh
# Run once by the postgres image on an empty data directory, after init.sql. The server signs its own certificate,
# on P-384 so it signs with SHA-384 and a channel binding that hashed with SHA-256 regardless would fail. The certificate
# stays in the data directory; a test reads it there as the owner. Plain connections are served as before.
set -e
cd "$PGDATA"
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-384 -sha384 -nodes -days 3650 -subj /CN=localhost \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" -addext "basicConstraints=critical,CA:FALSE" \
  -keyout server.key -out server.crt 2>/dev/null
chmod 600 server.key
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "ALTER SYSTEM SET ssl = on"
