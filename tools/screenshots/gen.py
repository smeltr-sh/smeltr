#!/usr/bin/env python3
"""Live traffic for the demo fleet: about 1,600 records a second into Kafka, until killed.

    python3 gen.py [scale]     # scale multiplies every rate

Each rate drifts on a slow wave of its own, so the charts move.
"""

import datetime as dt
import functools
import random
import sys
import time

from confluent_kafka import Producer

import logs

SCALE = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0

# topic, events per second, generator, wave period seconds, phase
STREAMS = [
    ("syslog.panos", 900, logs.panos_traffic, 420, 0.0),
    ("syslog.fortigate", 420, functools.partial(logs.fortigate_traffic, drift=True), 600, 1.3),
    ("web.nginx.access", 260, logs.nginx_access, 300, 2.1),
    ("syslog.auth", 18, logs.auth_log, 540, 0.7),
    ("aws.cloudtrail", 35, logs.cloudtrail, 480, 2.8),
    ("syslog.asa", 120, logs.asa_connection, 360, 1.9),
]

p = Producer({"bootstrap.servers": "localhost:19092", "linger.ms": 50, "compression.type": "lz4",
              "queue.buffering.max.messages": 500000})
r = random.Random(7)
debt = {s[0]: 0.0 for s in STREAMS}
t0 = time.time()
tick = 0.1
sent = 0
last_report = t0
while True:
    start = time.time()
    now = dt.datetime.now(dt.timezone.utc)
    t = start - t0
    for topic, rate, gen, period, phase in STREAMS:
        debt[topic] += rate * SCALE * logs.wave(t, period, 0.25, phase) * tick * r.uniform(0.85, 1.15)
        n = int(debt[topic])
        debt[topic] -= n
        for _ in range(n):
            if gen is logs.auth_log:
                # Twenty seconds of password guessing every five minutes.
                line = gen(r, now, burst=(int(t) % 300) < 20)
            elif gen is logs.nginx_access:
                # A short spell of upstream errors every ten minutes.
                line = gen(r, now, error_rate=0.25 if (int(t) % 600) < 45 else 0.01)
                # Now and then a line cut short in transit: it fails to parse and takes the DLQ path.
                if r.random() < 0.004:
                    line = line[: r.randint(20, len(line) // 2)]
            else:
                line = gen(r, now)
            while True:
                try:
                    p.produce(topic, line.encode())
                    break
                except BufferError:
                    p.poll(0.05)
            sent += 1
    p.poll(0)
    if start - last_report > 30:
        print(f"{now:%H:%M:%S} sent {sent}", flush=True)
        last_report = start
    time.sleep(max(0.0, tick - (time.time() - start)))
