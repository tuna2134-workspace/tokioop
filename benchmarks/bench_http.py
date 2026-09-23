"""HTTP keep-alive throughput via aiohttp server on each loop.

Runs the identical aiohttp application on asyncio / uvloop / tokioop and
hammers it with a raw-socket keep-alive client (no pipelining).
"""

import asyncio
import os
import socket
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(__file__))
from common import LOOP_KINDS, make_loop, measure, pin_cpu, table

NREQ = 2000
CONC = 10
BODY = b'{"hello":"world"}'


def run_http(kind):
    from aiohttp import web

    async def handler(request):
        return web.Response(body=BODY, content_type="application/json")

    app = web.Application()
    app.router.add_get("/", handler)

    loop = make_loop(kind)
    runner = web.AppRunner(app)
    loop.run_until_complete(runner.setup())
    site = web.TCPSite(runner, "127.0.0.1", 0)
    loop.run_until_complete(site.start())
    port = site._server.sockets[0].getsockname()[1]

    # The serving loop must run concurrently with the clients.
    def serve():
        loop.run_forever()

    server_thread = threading.Thread(target=serve, daemon=True)
    server_thread.start()

    req = (
        b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n"
    )

    def worker(n, out):
        s = socket.create_connection(("127.0.0.1", port))
        f = s.makefile("rb")
        ok = 0
        t0 = time.perf_counter()
        for _ in range(n):
            s.sendall(req)
            # minimal response parse: status line + headers + body
            line = f.readline()
            if not line.startswith(b"HTTP/1.1 200"):
                break
            length = 0
            while True:
                h = f.readline()
                if h in (b"\r\n", b"\n", b""):
                    break
                if h.lower().startswith(b"content-length:"):
                    length = int(h.split(b":", 1)[1].strip())
            f.read(length)
            ok += 1
        out.append((ok, time.perf_counter() - t0))
        s.close()

    per = NREQ // CONC
    t0 = time.perf_counter()
    results = []
    threads = [threading.Thread(target=worker, args=(per, results)) for _ in range(CONC)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    wall = time.perf_counter() - t0
    total_ok = sum(ok for ok, _ in results)

    loop.call_soon_threadsafe(loop.stop)
    server_thread.join(timeout=30)
    loop.run_until_complete(runner.cleanup())
    loop.close()
    assert total_ok == NREQ, (kind, total_ok)
    return wall


def main():
    pin_cpu()
    rows = []
    for kind in LOOP_KINDS:
        s = measure(lambda kind=kind: run_http(kind), warmup=1, iters=3)
        rows.append((kind, f"{NREQ / s['median']:,.0f} req/s", f"{s['median']:.3f}s"))
    table(f"HTTP keep-alive via aiohttp: {NREQ} reqs x {CONC} conns", rows,
          headers=("loop", "throughput", "median total"))


if __name__ == "__main__":
    main()
