#!/usr/bin/env bash
# A Kafka broker that serves TLS and demands a client certificate, on this
# machine. Everything it writes is under /tmp, so `down.sh` removes it and a
# rerun starts clean. Nothing here needs root: 9094 and 9095 are above the
# privileged range, and clear of `just live-up`'s broker on 19092 and the
# Kerberized one on 9093, so the three rigs can be up at once.
#
# On the runner rather than in containers, like the Kerberos rig: the broker
# reads its key and certificate by path, and what this writes is what the
# tests read.
#
# PEM keystores rather than JKS, so openssl is the only tool needed and no
# password protects a key that lives for one test run. Kafka has read
# `ssl.keystore.type=PEM` since 2.7.
set -euo pipefail

"$(dirname "$0")/down.sh" >/dev/null 2>&1 || true

T=/tmp/smeltr-tls
KAFKA_VERSION=${KAFKA_VERSION:-3.9.0}
KAFKA_HOME=/tmp/kafka-tls
TGZ=/tmp/kafka.tgz
TOPIC=smeltr-tls-tests

mkdir -p "$T"

# --- the authority, and the two certificates under it ----------------------
# One authority issues both halves, which is what an identity holds: the
# broker's certificate to verify, and the client's to present. A client
# under another authority is what the suite's negative case builds, in the
# test rather than here -- minting it there keeps this script to the one
# arrangement that is meant to work.
#
# openssl says what went wrong on stderr and nothing on success, so its
# output is kept and printed only when a step fails. Silencing it is how a
# certificate that was never issued becomes a broker that never starts, with
# nothing on screen between the two.
ssl() {
    local log="$T/openssl.log"
    if ! openssl "$@" >>"$log" 2>&1; then
        echo "openssl $1 failed:" >&2
        tail -20 "$log" >&2
        exit 1
    fi
}

ssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
    -subj "/CN=smeltr TLS test authority" \
    -keyout "$T/ca.key.pem" -out "$T/ca.pem"

# `ext` holds one extension per line, which is the only shape `-extfile`
# reads: two on one line are parsed as two values of the first.
issue() {
    local name=$1 common=$2 ext=$3
    ssl req -newkey rsa:2048 -nodes \
        -subj "/CN=$common" \
        -keyout "$T/$name.key.pem" -out "$T/$name.csr"
    printf '%s\n' "$ext" > "$T/$name.ext"
    ssl x509 -req -in "$T/$name.csr" -days 3650 \
        -CA "$T/ca.pem" -CAkey "$T/ca.key.pem" -CAcreateserial \
        -extfile "$T/$name.ext" \
        -out "$T/$name.crt.pem"
    rm -f "$T/$name.csr" "$T/$name.ext"
}

# The broker's certificate carries `localhost` and 127.0.0.1, because librdkafka
# verifies the name it was asked to connect to; one without them fails the
# handshake for the wrong reason. It is good for client authentication as well
# as server, because the broker is a client of itself over the inter-broker
# listener and refuses to start against a certificate it could not connect
# with.
issue broker localhost $'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth'
issue client smeltr-tls-client $'subjectAltName=DNS:smeltr\nextendedKeyUsage=clientAuth'

# Kafka's PEM keystore is the key and the certificate in one file.
cat "$T/broker.key.pem" "$T/broker.crt.pem" > "$T/broker.keystore.pem"

# --- the broker ------------------------------------------------------------
# Into a temporary name first: a transfer cut off midway must not leave a
# file the next run mistakes for a complete one. Shared with the Kerberos
# rig, which downloads the same archive to the same path.
if [ ! -f "$TGZ" ]; then
    curl -fsSL -o "$TGZ.part" \
        "https://archive.apache.org/dist/kafka/$KAFKA_VERSION/kafka_2.13-$KAFKA_VERSION.tgz"
    mv "$TGZ.part" "$TGZ"
fi
rm -rf "$KAFKA_HOME" /tmp/kafka-tls-logs
mkdir -p "$KAFKA_HOME"
tar xzf "$TGZ" -C "$KAFKA_HOME" --strip-components=1

# The controller listener stays PLAINTEXT -- this tests the client's TLS, not
# the quorum's. `ssl.client.auth=required` is the point of the rig: without
# it a client presenting nothing would still connect, and the suite could not
# tell a certificate that was sent from one that was ignored.
#
# The broker is a client of itself over the inter-broker listener, and the
# name it dials itself by is its own advertised one; `ssl.endpoint...` is left
# at its default so that name is verified there too.
cat > "$KAFKA_HOME/config/kraft-tls.properties" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9095
listeners=SSL://:9094,CONTROLLER://:9095
advertised.listeners=SSL://localhost:9094
listener.security.protocol.map=SSL:SSL,CONTROLLER:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=SSL
ssl.keystore.type=PEM
ssl.keystore.location=$T/broker.keystore.pem
ssl.truststore.type=PEM
ssl.truststore.location=$T/ca.pem
ssl.client.auth=required
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=true
log.dirs=/tmp/kafka-tls-logs
EOF

UUID=$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)
"$KAFKA_HOME/bin/kafka-storage.sh" format -t "$UUID" \
    -c "$KAFKA_HOME/config/kraft-tls.properties" >/dev/null

export KAFKA_HEAP_OPTS="-Xmx768m -Xms256m"
nohup "$KAFKA_HOME/bin/kafka-server-start.sh" \
    "$KAFKA_HOME/config/kraft-tls.properties" > /tmp/kafka-tls-broker.log 2>&1 &
# The start script execs java, so this pid is the broker's. Stopping by it
# cannot reach a broker this script did not start.
echo $! > "$T/broker.pid"

for _ in $(seq 1 90); do
    grep -q "Kafka Server started" /tmp/kafka-tls-broker.log && break
    sleep 1
done
grep -q "Kafka Server started" /tmp/kafka-tls-broker.log || {
    echo "broker did not start; last 40 lines:" >&2
    tail -40 /tmp/kafka-tls-broker.log >&2
    exit 1
}

# A fixture topic with a known partition count. A metadata request does not
# create one -- librdkafka leaves `allow.auto.create.topics` off for
# consumers -- and the source test asserts the count it gets back, so an
# auto-created single partition would not tell it apart from a real answer.
cat > "$T/client.properties" <<EOF
security.protocol=SSL
ssl.keystore.type=PEM
ssl.keystore.location=$T/client.keystore.pem
ssl.truststore.type=PEM
ssl.truststore.location=$T/ca.pem
EOF
cat "$T/client.key.pem" "$T/client.crt.pem" > "$T/client.keystore.pem"
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9094 \
    --command-config "$T/client.properties" --create --if-not-exists \
    --topic "$TOPIC" --partitions 2 --replication-factor 1 >/dev/null

# The one place these paths are decided. `just check-tls` sources this rather
# than repeating them.
cat > "$T/env" <<EOF
KAFKA_TLS_BROKERS=localhost:9094
KAFKA_TLS_TOPIC=$TOPIC
KAFKA_TLS_CLIENT_CERT=$T/client.crt.pem
KAFKA_TLS_CLIENT_KEY=$T/client.key.pem
KAFKA_TLS_CA=$T/ca.pem
EOF

echo "TLS broker up on localhost:9094. Environment: $T/env"
