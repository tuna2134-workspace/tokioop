"""Callback scheduling: bulk call_soon + chained call_soon latency."""

import asyncio
import sys
import time

sys.path.insert(0, __import__("os").path.dirname(__file__))
from common import LOOP_KINDS, env_info, make_loop, measure, pin_cpu, table

N_BULK = 200_000
N_CHAIN = 100_000


def bench_bulk(kind):
    def run():
        loop = make_loop(kind)

        def noop():
            pass

        t0 = time.perf_counter()
        for _ in range(N_BULK):
            loop.call_soon(noop)
        # drain everything, then stop
        loop.call_soon(loop.stop)
        loop.run_forever()
        # one more pass in case of stragglers (paranoia; should be empty)
        t1 = time.perf_counter()
        loop.close()
        return t1 - t0

    return run


def bench_chain(kind):
    def run():
        loop = make_loop(kind)
        remaining = [N_CHAIN]

        def cb():
            remaining[0] -= 1
            if remaining[0] > 0:
                loop.call_soon(cb)
            else:
                loop.stop()

        t0 = time.perf_counter()
        loop.call_soon(cb)
        loop.run_forever()
        t1 = time.perf_counter()
        loop.close()
        assert remaining[0] == 0
        return t1 - t0

    return run


def main():
    pin_cpu()
    print("env:", env_info())
    rows = []
    for kind in LOOP_KINDS:
        s = measure(bench_bulk(kind))
        rows.append(
            (kind, f"{s['median'] * 1e9 / N_BULK:.1f} ns/cb",
             f"{N_BULK / s['median'] / 1e6:.2f} Mcb/s", f"{s['median']:.3f}s")
        )
    table(f"call_soon bulk: {N_BULK} callbacks scheduled + drained", rows,
          headers=("loop", "ns/callback", "throughput", "median total"))

    rows = []
    for kind in LOOP_KINDS:
        s = measure(bench_chain(kind))
        rows.append(
            (kind, f"{s['median'] * 1e9 / N_CHAIN:.1f} ns/step",
             f"{N_CHAIN / s['median'] / 1e6:.2f} Msteps/s", f"{s['median']:.3f}s")
        )
    table(f"call_soon chain: {N_CHAIN} sequential schedule+dispatch steps", rows,
          headers=("loop", "ns/step", "throughput", "median total"))


if __name__ == "__main__":
    main()
