"""TCP echo throughput + latency across message sizes and connections."""

import asyncio
import os
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
from common import LOOP_KINDS, compare, make_loop, table

SIZES = (64, 256, 1024, 4096, 16384)
CONNS = (1, 10, 50)
MSGS_PER_CONN = 200


def run_echo(kind, size, nconns, nmsgs):
    """Returns (seconds, p50_ms, p99_ms). Server+clients on one loop."""
    loop = make_loop(kind)
    payload = b"x" * size
    lat = []

    async def main():
        async def handle(reader, writer):
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                writer.write(data)
                await writer.drain()
            writer.close()

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]

        async def client():
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            for _ in range(nmsgs):
                t0 = time.perf_counter()
                writer.write(payload)
                await writer.drain()
                await reader.readexactly(size)
                lat.append((time.perf_counter() - t0) * 1000)
            writer.close()

        # swap in our loop for the duration
        await asyncio.gather(*[client() for _ in range(nconns)])
        server.close()
        await server.wait_closed()

    async def runner():
        # run main() with `loop` as the running loop
        return await main()

    t0 = time.perf_counter()
    task = loop.create_task(main())
    loop.run_until_complete(task)
    total = time.perf_counter() - t0
    loop.close()
    lat.sort()
    p50 = lat[len(lat) // 2]
    p99 = lat[min(len(lat) - 1, int(len(lat) * 0.99))]
    return total, p50, p99


def main():
    for size in SIZES:
        for nconns in CONNS:
            total_msgs = nconns * MSGS_PER_CONN

            # compare() interleaves loop kinds; rebuild per-kind runners
            def factory(kind, size=size, nconns=nconns):
                def run():
                    total, p50, p99 = run_echo(kind, size, nconns, MSGS_PER_CONN)
                    run.lat = (p50, p99)
                    return total

                return run

            stats = compare(factory, iters=5)
            rows = []
            for kind in LOOP_KINDS:
                s = stats[kind]
                rps = total_msgs / s["median"]
                rows.append((kind, f"{rps:,.0f} msg/s", f"{s['median']:.3f}s",
                             f"min {s['min']:.3f}s"))
            table(f"TCP echo size={size}B conns={nconns} msgs={total_msgs} (interleaved)",
                  rows, headers=("loop", "throughput", "median", "min"))


if __name__ == "__main__":
    main()
