#!/bin/bash
# Experiment, not for merge: #911's purge-everything against a 1 s jemalloc decay while the runtime is over its
# memory size. One build serves both arms; SMELTR_EXP_DECAY in the smeltr container picks the decay arm.
# Rounds alternate the arms' order. Results go to target/exp-decay.txt.
set -u
cd "$(dirname "$0")/../.."
OUT=target/exp-decay.txt
: > $OUT
cargo test --profile dist --test connector-speed --no-run || exit 1
kills() { dmesg 2>/dev/null | grep -c 'Killed process' || echo "?"; }
for r in 1 2 3; do
  if [ $((r % 2)) = 1 ]; then arms="purge decay"; else arms="decay purge"; fi
  for arm in $arms; do
    k0=$(kills)
    if [ $arm = decay ]; then export ENGINE_SMELTR_ENV=SMELTR_EXP_DECAY=1; else unset ENGINE_SMELTR_ENV; fi
    ENGINES=smeltr ENGINE_TRANSPORTS=${TRANSPORTS:-s3-out,http,hec-in} ENGINE_PIPELINES=passthrough \
      ENGINE_CORES=${CORES:-1,2,4,8} ENGINE_ROUNDS=1 \
      cargo test --profile dist --test connector-speed -- --ignored --exact engines::speed --nocapture 2>&1 \
      | grep -E ' cores ' | sed "s/^/r$r $arm /" | tee -a $OUT
    echo "r$r $arm OOM kills: $(( $(kills) - k0 ))" | tee -a $OUT
  done
done
