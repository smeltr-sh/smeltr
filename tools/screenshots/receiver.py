#!/usr/bin/env python3
"""Where the demo's alert channels send: a webhook that answers every POST with 200, and a mail server that takes
every message, over STARTTLS with the certificate `stage.py` makes for it. What either is sent is dropped.

    python3 receiver.py <dir>      # <dir> holds cert.pem and key.pem

The demo cannot reach PagerDuty, Slack or a real mail server, and a channel whose sends fail says so on the Alerts
page the shots show.
"""

import asyncio
import os
import ssl
import sys

WEBHOOK_PORT = 9099
SMTP_PORT = 2587


async def webhook(reader, writer):
    try:
        while True:
            head = await reader.readuntil(b"\r\n\r\n")
            length = next((int(line.split(b":", 1)[1]) for line in head.split(b"\r\n")
                           if line.lower().startswith(b"content-length:")), 0)
            await reader.readexactly(length)
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            await writer.drain()
    except (asyncio.IncompleteReadError, ConnectionError):
        pass
    finally:
        writer.close()


def smtp(tls):
    async def serve(reader, writer):
        async def say(line):
            writer.write(line + b"\r\n")
            await writer.drain()

        try:
            await say(b"220 localhost demo receiver")
            while line := await reader.readline():
                verb = line[:4].upper()
                if verb == b"EHLO":
                    await say(b"250-localhost")
                    await say(b"250-AUTH PLAIN LOGIN")
                    await say(b"250 STARTTLS")
                elif verb == b"STAR":
                    await say(b"220 ready")
                    await writer.start_tls(tls)
                elif verb == b"AUTH":
                    await say(b"235 accepted")
                elif verb == b"DATA":
                    await say(b"354 go on")
                    while (await reader.readline()) not in (b".\r\n", b""):
                        pass
                    await say(b"250 taken")
                elif verb == b"QUIT":
                    await say(b"221 bye")
                    break
                else:
                    await say(b"250 ok")
        except (ConnectionError, ssl.SSLError):
            pass
        finally:
            writer.close()

    return serve


async def main(certs):
    tls = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    tls.load_cert_chain(os.path.join(certs, "cert.pem"), os.path.join(certs, "key.pem"))
    servers = [await asyncio.start_server(webhook, "127.0.0.1", WEBHOOK_PORT),
               await asyncio.start_server(smtp(tls), "127.0.0.1", SMTP_PORT)]
    await asyncio.gather(*(s.serve_forever() for s in servers))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    asyncio.run(main(sys.argv[1]))
