"""Shared benchmark harness: warmup, iterations, median/p95, env report."""

import asyncio
import os
import platform
import statistics
import sys
import time

LOOP_KINDS = ("asyncio", "uvloop", "tokioop")


def make_loop(kind):
    if kind == "asyncio":
        return asyncio.new_event_loop()
    if kind == "uvloop":
        import uvloop

        return uvloop.new_event_loop()
    if kind == "tokioop":
        import tokioop

        return tokioop.new_event_loop()
    raise ValueError(kind)


def env_info():
    info = {
        "python": platform.python_version(),
        "os": f"{platform.system()} {platform.release()}",
        "machine": platform.machine(),
        "cpu": platform.processor() or "?",
    }
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("model name"):
                    info["cpu"] = line.split(":", 1)[1].strip()
                    break
    except OSError:
        pass
    try:
        import uvloop

        info["uvloop"] = uvloop.__version__
    except ImportError:
        info["uvloop"] = "n/a"
    try:
        import tokioop

        info["tokioop"] = tokioop.__version__
    except ImportError:
        info["tokioop"] = "n/a"
    return info


def measure(fn, warmup=1, iters=5):
    """Run fn() (returns seconds) with warmup; return dict of stats."""
    for _ in range(warmup):
        fn()
    samples = [fn() for _ in range(iters)]
    samples.sort()
    return {
        "samples": samples,
        "median": statistics.median(samples),
        "min": samples[0],
        "max": samples[-1],
        "p95": samples[min(len(samples) - 1, int(len(samples) * 0.95))],
    }


def compare(make_run, kinds=LOOP_KINDS, warmup=1, iters=7):
    """Interleaved sampling across loops (rotated order) to average out
    host noise on shared machines. make_run(kind) -> zero-arg run fn.
    Returns {kind: stats}.
    """
    pin_cpu()
    runs = {k: make_run(k) for k in kinds}
    for k in kinds:
        for _ in range(warmup):
            runs[k]()
    samples = {k: [] for k in kinds}
    order = list(kinds)
    for i in range(iters):
        # rotate starting loop each round
        round_order = order[i % len(order):] + order[: i % len(order)]
        for k in round_order:
            samples[k].append(runs[k]())
    out = {}
    for k in kinds:
        s = sorted(samples[k])
        out[k] = {
            "samples": s,
            "median": statistics.median(s),
            "min": s[0],
            "max": s[-1],
        }
    return out


def pin_cpu():
    """Pin to CPU 0.. if taskset is available (reduces noise)."""
    try:
        if hasattr(os, "sched_setaffinity"):
            os.sched_setaffinity(0, {0, 1})
    except OSError:
        pass


def table(title, rows, headers=("loop", "median", "min", "max", "unit")):
    print(f"\n### {title}")
    widths = [max(len(str(r[i])) for r in rows + [headers]) for i in range(len(headers))]
    fmt = "  ".join("{:<%d}" % w for w in widths)
    print(fmt.format(*headers))
    print(fmt.format(*["-" * w for w in widths]))
    for r in rows:
        print(fmt.format(*[str(x) for x in r]))
    sys.stdout.flush()
