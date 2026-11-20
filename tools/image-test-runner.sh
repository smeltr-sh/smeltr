#!/bin/sh
# nextest's target runner for `just image-tests`: starts one test process in a container of the image under test
# ($SMELTR_TEST_IMAGE) as that image runs, with a read-only root, as user 1000 and with nothing installed. The source
# is mounted where the tests were built (/src) and where the host has it, the extracted archive where nextest put it,
# and the brokers' client files where their tests read them, all read-only. Everything but the source is under
# $SMELTR_TEST_DIR, where `just image-tests` puts it.
set -u
bin=$1
shift
env=$(mktemp)
# The tests' own settings and the brokers'. Nothing else of the host's environment reaches the image.
env | grep -E '^(CARGO|NEXTEST|RUST|SMELTR|KAFKA|KRB5)[A-Z0-9_]*=' >"$env"
docker run --rm -i --read-only --user 1000 --network host --tmpfs /tmp:size=1g \
    -v "$SMELTR_TEST_SOURCE:/src:ro" -v "$SMELTR_TEST_SOURCE:$SMELTR_TEST_SOURCE:ro" \
    -v "$SMELTR_TEST_DIR/archive/target:/src/target:ro" -v "$SMELTR_TEST_DIR/archive:$SMELTR_TEST_DIR/archive:ro" \
    -v "$SMELTR_TEST_DIR/krb:/tmp/krb:ro" -v "$SMELTR_TEST_DIR/smeltr-tls:/tmp/smeltr-tls:ro" \
    --env-file "$env" -w /src --entrypoint "$bin" "$SMELTR_TEST_IMAGE" "$@"
status=$?
rm -f "$env"
exit $status
