#!/usr/bin/env bash
# A KDC and a broker that demands GSSAPI, on this machine. Everything it
# writes is under /tmp, so `down.sh` removes it and a rerun starts clean.
# Nothing here needs root, and nothing here touches the machine's own
# Kerberos setup: `KRB5_CONFIG` and `KRB5_KDC_PROFILE` are what every krb5
# tool reads instead of /etc, and 8088 is above the privileged range.
#
# On the runner rather than in containers: the KDC generates the keytabs,
# and both the broker and the test read them by path, which sibling
# service containers cannot share.
set -euo pipefail

# Start clean: what a failed run left behind would make `kdb5_util create`
# refuse. `down.sh` is the one list of what this script writes.
"$(dirname "$0")/down.sh" >/dev/null 2>&1 || true

REALM=SMELTR.TEST
K=/tmp/krb
KAFKA_VERSION=${KAFKA_VERSION:-3.9.0}
KAFKA_HOME=/tmp/kafka
TGZ=/tmp/kafka.tgz

# The host half of the service principal, and what the broker advertises.
# Not `localhost`: MIT krb5 replaces that with the machine's own name when it
# builds a service principal, whatever canonicalization is turned off, and
# then asks a realm derived from that name -- which no KDC here serves. So
# name the machine up front. Lowercase because krb5 lowercases the component
# it puts in the principal.
HOST=$(hostname -f | tr '[:upper:]' '[:lower:]')
case "$HOST" in
    '' | localhost | localhost.*)
        echo "hostname -f gave ${HOST:-nothing}; need a name krb5 will keep" >&2
        exit 1
        ;;
esac
getent hosts "$HOST" >/dev/null || {
    echo "$HOST does not resolve; the broker and the KDC both need it to" >&2
    exit 1
}

mkdir -p "$K"

# --- the KDC ---------------------------------------------------------------
# Both variables are exported: `kdb5_util`, `kadmin.local` and `krb5kdc`
# read them here, and the tests and the broker read `KRB5_CONFIG` from the
# env file written at the end.
export KRB5_CONFIG="$K/krb5.conf"
export KRB5_KDC_PROFILE="$K/kdc.conf"

cat > "$KRB5_CONFIG" <<EOF
[libdefaults]
    default_realm = $REALM
    dns_lookup_realm = false
    dns_lookup_kdc = false
    rdns = false
    # The service principal names $HOST exactly; a resolver that canonicalizes
    # it to something else must not change what the client asks for.
    dns_canonicalize_hostname = false
    # The KDC is on loopback with no UDP fragmentation to worry about, and
    # TCP gives a clearer failure than a silent UDP timeout.
    udp_preference_limit = 1

[realms]
    $REALM = {
        kdc = 127.0.0.1:8088
        admin_server = 127.0.0.1:8749
    }

[domain_realm]
    $HOST = $REALM
EOF

cat > "$KRB5_KDC_PROFILE" <<EOF
[kdcdefaults]
    kdc_ports = 8088
    kdc_tcp_ports = 8088

[realms]
    $REALM = {
        database_name = $K/principal
        admin_keytab = $K/kadm5.keytab
        acl_file = $K/kadm5.acl
        key_stash_file = $K/.k5.$REALM
        max_life = 10h 0m 0s
        max_renewable_life = 7d 0h 0m 0s
    }
EOF
echo '*/admin *' > "$K/kadm5.acl"

kdb5_util create -s -P masterpw -r "$REALM"

# `-randkey`: nothing needs a password, both principals authenticate with
# the keytab written below.
kadmin.local -q "addprinc -randkey kafka/$HOST@$REALM"
kadmin.local -q "addprinc -randkey smeltrclient@$REALM"
kadmin.local -q "ktadd -k $K/kafka.keytab kafka/$HOST@$REALM"
kadmin.local -q "ktadd -k $K/client.keytab smeltrclient@$REALM"

krb5kdc -P "$K/kdc.pid"

# --- the broker ------------------------------------------------------------
# Into a temporary name first: a transfer cut off midway must not leave a
# file the next run mistakes for a complete one.
if [ ! -f "$TGZ" ]; then
    curl -fsSL -o "$TGZ.part" \
        "https://archive.apache.org/dist/kafka/$KAFKA_VERSION/kafka_2.13-$KAFKA_VERSION.tgz"
    mv "$TGZ.part" "$TGZ"
fi
rm -rf "$KAFKA_HOME" /tmp/kafka-logs
mkdir -p "$KAFKA_HOME"
tar xzf "$TGZ" -C "$KAFKA_HOME" --strip-components=1

cat > "$K/kafka_server_jaas.conf" <<EOF
KafkaServer {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  storeKey=true
  keyTab="$K/kafka.keytab"
  principal="kafka/$HOST@$REALM";
};
EOF

# The controller listener stays PLAINTEXT -- this tests the client's SASL,
# not the quorum's. `sasl.mechanism.controller.protocol` is still required
# or the broker refuses to start.
cat > "$KAFKA_HOME/config/kraft-gssapi.properties" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=SASL_PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=SASL_PLAINTEXT://$HOST:9092
listener.security.protocol.map=SASL_PLAINTEXT:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=SASL_PLAINTEXT
sasl.enabled.mechanisms=GSSAPI
sasl.mechanism.inter.broker.protocol=GSSAPI
sasl.mechanism.controller.protocol=GSSAPI
sasl.kerberos.service.name=kafka
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=true
log.dirs=/tmp/kafka-logs
EOF

UUID=$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)
"$KAFKA_HOME/bin/kafka-storage.sh" format -t "$UUID" \
    -c "$KAFKA_HOME/config/kraft-gssapi.properties"

export KAFKA_OPTS="-Djava.security.auth.login.config=$K/kafka_server_jaas.conf -Djava.security.krb5.conf=$KRB5_CONFIG"
export KAFKA_HEAP_OPTS="-Xmx768m -Xms256m"
nohup "$KAFKA_HOME/bin/kafka-server-start.sh" \
    "$KAFKA_HOME/config/kraft-gssapi.properties" > /tmp/kafka-broker.log 2>&1 &
# The start script execs java, so this pid is the broker's. Stopping by it
# cannot reach a broker this script did not start.
echo $! > "$K/broker.pid"

for _ in $(seq 1 60); do
    grep -q "Kafka Server started" /tmp/kafka-broker.log && break
    sleep 1
done
grep -q "Kafka Server started" /tmp/kafka-broker.log || {
    echo "broker did not start; last 40 lines:" >&2
    tail -40 /tmp/kafka-broker.log >&2
    exit 1
}

# A fixture topic with a known partition count. A metadata request does not
# create one -- librdkafka leaves `allow.auto.create.topics` off for
# consumers -- and the source test asserts the count it gets back, so an
# auto-created single partition would not tell it apart from a real answer.
cat > "$K/client.properties" <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=kafka
EOF
cat > "$K/kafka_client_jaas.conf" <<EOF
KafkaClient {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  storeKey=true
  keyTab="$K/client.keytab"
  principal="smeltrclient@$REALM";
};
EOF
KAFKA_OPTS="-Djava.security.auth.login.config=$K/kafka_client_jaas.conf -Djava.security.krb5.conf=$KRB5_CONFIG" \
    "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server $HOST:9092 \
    --command-config "$K/client.properties" --create --if-not-exists \
    --topic smeltr-kerberos-tests --partitions 2 --replication-factor 1 >/dev/null

# The one place these paths are decided. `just check-kerberos` and the
# workflow source this rather than repeating them.
cat > "$K/env" <<EOF
KRB5_CONFIG=$KRB5_CONFIG
KAFKA_GSSAPI_BROKERS=$HOST:9092
KAFKA_GSSAPI_KEYTAB=$K/client.keytab
EOF

echo "KDC and GSSAPI broker up. Environment: $K/env"
