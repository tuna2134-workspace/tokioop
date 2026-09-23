"""UDP ping-pong packets/sec."""

import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
from common import LOOP_KINDS, compare, make_loop, pin_cpu, table

N = 20_000
SIZE = 64
BATCH = 200  # keep loopback UDP buffers from overflowing (kernel drops bursts)


def run_udp(kind):
    loop = make_loop(kind)
    payload = b"y" * SIZE

    async def main():
        loop = asyncio.get_running_loop()
        inbox = asyncio.Queue()

        class ServerProto(asyncio.DatagramProtocol):
            def datagram_received(self, data, addr):
                inbox.put_nowait((data, addr))

        class ClientProto(asyncio.DatagramProtocol):
            def __init__(self):
                self.q = asyncio.Queue()

            def datagram_received(self, data, addr):
                self.q.put_nowait(data)

        t_server, _ = await loop.create_datagram_endpoint(
            ServerProto, local_addr=("127.0.0.1", 0)
        )
        sport = t_server.get_extra_info("socket").getsockname()[1]
        t_client, cproto = await loop.create_datagram_endpoint(
            ClientProto, remote_addr=("127.0.0.1", sport)
        )
        # paced: send a batch, drain it, repeat (bursts overflow loopback
        # UDP buffers and the kernel drops them -- identically on all loops)
        t0 = time.perf_counter()
        sent = 0
        while sent < N:
            n = min(BATCH, N - sent)
            for _ in range(n):
                t_client.sendto(payload)
            for _ in range(n):
                data = await inbox.get()
                assert data[0] == payload
            sent += n
        total = time.perf_counter() - t0
        t_client.close()
        t_server.close()
        await asyncio.sleep(0.05)
        return total

    t0 = time.perf_counter()
    task = loop.create_task(main())
    total_inner = loop.run_until_complete(task)
    wall = time.perf_counter() - t0
    loop.close()
    return total_inner


def make_run(kind):
    def run():
        return run_udp(kind)

    return run


def main():
    pin_cpu()
    stats = compare(make_run)
    rows = []
    for kind in LOOP_KINDS:
        s = stats[kind]
        rows.append(
            (kind, f"{N / s['median']:,.0f} pkt/s",
             f"median {s['median']:.3f}s", f"min {s['min']:.3f}s")
        )
    table(f"UDP send {N} x {SIZE}B datagrams (paced, interleaved)",
          rows, headers=("loop", "throughput", "median", "min"))


if __name__ == "__main__":
    main()
