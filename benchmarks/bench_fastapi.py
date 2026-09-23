"""FastAPI + uvicorn on each loop (same app, loop_factory swap only)."""

import json
import os
import socket
import sys
import threading
import time
import urllib.request

sys.path.insert(0, os.path.dirname(__file__))
from common import LOOP_KINDS, compare, make_loop, pin_cpu, table

NREQ = 1500
CONC = 6
PORT = 8477


def make_app():
    from fastapi import FastAPI

    app = FastAPI()

    @app.get("/")
    async def root():
        return {"hello": "world"}

    return app


def run_fastapi(kind):
    import uvicorn

    app = make_app()
    config = uvicorn.Config(app, host="127.0.0.1", port=PORT, loop="asyncio",
                            log_level="error")
    config.loop_factory = lambda: make_loop(kind)
    server = uvicorn.Server(config=config)
    th = threading.Thread(target=server.run, daemon=True)
    th.start()
    for _ in range(200):
        if server.started:
            break
        time.sleep(0.05)
    assert server.started, f"{kind}: uvicorn did not start"

    req = b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n"

    def worker(n, out):
        s = socket.create_connection(("127.0.0.1", PORT))
        f = s.makefile("rb")
        ok = 0
        t0 = time.perf_counter()
        for _ in range(n):
            s.sendall(req)
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
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wall = time.perf_counter() - t0
    total_ok = sum(ok for ok, _ in results)
    server.should_exit = True
    th.join(timeout=30)
    time.sleep(0.5)  # let the port free up for the next loop
    assert total_ok == NREQ, (kind, total_ok)
    return wall


def main():
    pin_cpu()
    stats = compare(lambda kind: (lambda: run_fastapi(kind)), iters=3)
    rows = []
    for kind in LOOP_KINDS:
        s = stats[kind]
        rows.append((kind, f"{NREQ / s['median']:,.0f} req/s",
                     f"median {s['median']:.3f}s", f"min {s['min']:.3f}s"))
    table(f"FastAPI+uvicorn: {NREQ} reqs x {CONC} conns (interleaved)",
          rows, headers=("loop", "throughput", "median", "min"))


if __name__ == "__main__":
    main()
