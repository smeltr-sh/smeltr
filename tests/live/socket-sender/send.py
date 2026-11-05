"""The live socket receiver fuzzer's sender: it carries out a plan the test drew, as a sender on another host would,
and reports what it did, so the test can say what the receiver must have taken.

python send.py PLAN REPORT

The plan names the receiver, and what to send it:
- holders: connections opened first and held, idle or stalled partway through a TLS handshake, until all else is done;
- storm: how many connections to open and close at once;
- connections: each from an address, under TLS presenting a certificate or none, or plain to a TLS receiver, written
  in pieces with a pause after each, and ended by a hang-up, a reset, a half-close or, under TLS, a hang-up unsaid;
- datagrams, each from an address, and a flood of named datagrams sent as fast as the socket takes them.

Every connection runs on its own thread, so their writes interleave. The report says, for each connection, whether
it connected and how many of its bytes the kernel took before an error, if any.
"""

import base64
import json
import socket
import ssl
import struct
import sys
import threading
import time

TIMEOUT = 15


def family(host):
    return socket.AF_INET6 if ":" in host else socket.AF_INET


def tcp(plan, source):
    sock = socket.socket(family(plan["host"]), socket.SOCK_STREAM)
    sock.settimeout(TIMEOUT)
    sock.bind((source, 0))
    # Each piece its own segment, so the receiver more often reads a message in parts.
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.connect((plan["host"], plan["port"]))
    return sock


def context(plan, presents):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(plan["tls"]["ca"])
    if presents == "trusted":
        ctx.load_cert_chain(plan["tls"]["cert"], plan["tls"]["key"])
    elif presents == "stranger":
        ctx.load_cert_chain(plan["tls"]["stranger_cert"], plan["tls"]["stranger_key"])
    return ctx


def drain(sock):
    """Reads until the receiver hangs up, so closing leaves no unread data to reset the connection with."""
    try:
        while sock.recv(65536):
            pass
        return None
    except socket.timeout:
        return "the receiver left the connection open"
    except OSError:
        return None


def reset(sock):
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    sock.close()


def connection(plan, c, out):
    out.update(id=c["id"], connected=False, written=0, error=None, open=None)
    try:
        sock = tcp(plan, c["from"])
    except OSError as e:
        out["error"] = f"connect: {e}"
        return
    out["connected"] = True
    stream = sock
    try:
        if plan["tls"] is not None and c["tls"] != "plain":
            stream = context(plan, c["tls"]).wrap_socket(sock, server_hostname="localhost")
        for piece, pause in zip(c["pieces"], c["pauses_ms"]):
            stream.sendall(base64.b64decode(piece))
            out["written"] += len(base64.b64decode(piece))
            if pause:
                time.sleep(pause / 1000)
    except (OSError, ssl.SSLError) as e:
        out["error"] = f"write: {e}"
    end = c["end"]
    try:
        if end == "rst":
            reset(stream)
            return
        if end == "notify" and stream is not sock:
            try:
                stream = stream.unwrap()
            except (OSError, ssl.SSLError):
                pass
        # Under TLS, shutting down the SSL socket skips close_notify: a hang-up unsaid.
        stream.shutdown(socket.SHUT_WR)
        if end == "half_close":
            # Writes no more, but holds the connection open a while before reading the receiver's hang-up.
            time.sleep(1)
        out["open"] = drain(stream)
    except (OSError, ssl.SSLError):
        pass
    finally:
        try:
            stream.close()
        except OSError:
            pass


def hold(plan, h, held, out):
    out.update(kind=h["kind"], connected=False)
    try:
        sock = tcp(plan, h["from"])
    except OSError as e:
        out["error"] = f"connect: {e}"
        return
    out["connected"] = True
    if h["kind"] == "handshake":
        # A TLS record header promising a ClientHello that never comes.
        try:
            sock.sendall(b"\x16\x03\x01\x02\x00\x01")
        except OSError:
            pass
    held.append(sock)


def storm(plan, n, source):
    done = 0
    for _ in range(n):
        try:
            sock = tcp(plan, source)
            if done % 2:
                reset(sock)
            else:
                sock.close()
            done += 1
        except OSError:
            pass
    return done


def datagrams(plan):
    sent = []
    socks = {}
    for d in plan["datagrams"]:
        sock = socks.get(d["from"])
        if sock is None:
            sock = socks[d["from"]] = socket.socket(family(plan["host"]), socket.SOCK_DGRAM)
            sock.bind((d["from"], 0))
        try:
            sock.sendto(base64.b64decode(d["bytes"]), (plan["host"], plan["port"]))
            sent.append(True)
        except OSError:
            sent.append(False)
    flooded = 0
    if plan["flood"]:
        sock = socket.socket(family(plan["host"]), socket.SOCK_DGRAM)
        sock.bind((plan["flood"]["from"], 0))
        filler = b"f" * plan["flood"]["size"]
        for i in range(plan["flood"]["count"]):
            try:
                sock.sendto(b"f%d " % i + filler, (plan["host"], plan["port"]))
                flooded += 1
            except OSError:
                pass
    return sent, flooded


def main():
    with open(sys.argv[1]) as f:
        plan = json.load(f)
    report = {"holders": [], "connections": [], "storm": 0, "datagrams": [], "flood": 0}
    if plan["transport"] == "udp":
        report["datagrams"], report["flood"] = datagrams(plan)
    else:
        held = []
        for h in plan["holders"]:
            out = {}
            hold(plan, h, held, out)
            report["holders"].append(out)
        report["storm"] = storm(plan, plan["storm"], plan["storm_from"])
        threads = []
        for c in plan["connections"]:
            out = {}
            report["connections"].append(out)
            t = threading.Thread(target=connection, args=(plan, c, out))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        for sock in held:
            sock.close()
    with open(sys.argv[2], "w") as f:
        json.dump(report, f)


if __name__ == "__main__":
    main()
