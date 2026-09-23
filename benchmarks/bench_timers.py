"""Timer scheduling: insertion, expiry, cancellation."""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
from common import LOOP_KINDS, make_loop, measure, pin_cpu, table

N = 50_000


def bench_expire(kind):
    def run():
        loop = make_loop(kind)
        fired = [0]

        def cb():
            fired[0] += 1
            if fired[0] == N:
                loop.stop()

        t0 = time.perf_counter()
        for _ in range(N):
            loop.call_later(0.001, cb)
        loop.run_forever()
        t1 = time.perf_counter()
        assert fired[0] == N, (kind, fired[0])
        loop.close()
        return t1 - t0

    return run


def main():
    pin_cpu()
    rows = []
    for kind in LOOP_KINDS:
        s = measure(bench_expire(kind), warmup=1, iters=3)
        rows.append(
            (kind, f"{s['median'] * 1e9 / N:.1f} ns/timer",
             f"{N / s['median'] / 1e3:.1f} ktimers/s", f"{s['median']:.3f}s")
        )
    table(f"timers: schedule {N} x call_later + expiry", rows,
          headers=("loop", "ns/timer", "throughput", "median total"))

    rows = []
    for kind in LOOP_KINDS:
        # insertion only
        def run(kind=kind):
            loop = make_loop(kind)
            t0 = time.perf_counter()
            for _ in range(N):
                loop.call_later(10.0, lambda: None)
            t1 = time.perf_counter()
            loop.close()
            return t1 - t0

        s = measure(run)
        rows.append((kind, f"{s['median'] * 1e9 / N:.1f} ns/insert"))
    table(f"timers: {N} x call_later insertion (no expiry)", rows,
          headers=("loop", "ns/insert"))

    rows = []
    for kind in LOOP_KINDS:
        def run(kind=kind):
            loop = make_loop(kind)
            handles = [loop.call_later(10.0, lambda: None) for _ in range(N)]
            t0 = time.perf_counter()
            for h in handles:
                h.cancel()
            t1 = time.perf_counter()
            loop.close()
            return t1 - t0

        s = measure(run)
        rows.append((kind, f"{s['median'] * 1e9 / N:.1f} ns/cancel"))
    table(f"timers: {N} x cancel", rows, headers=("loop", "ns/cancel"))


if __name__ == "__main__":
    main()
