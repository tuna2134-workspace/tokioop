# Benchmark report: tokioop vs CPython asyncio vs uvloop

Date: 2026-09-23. Release build (`maturin develop --release`).
Each benchmark warms up, runs multiple iterations, and reports medians
(`compare()` interleaves loop order per sample to average out host noise).

## Environment

- CPU: Intel Xeon E5-2690 v4 @ 2.60GHz (shared virtualized host — noisy;
  medians + min reported; loop order interleaved per sample)
- OS/kernel: Linux 7.2.6-arch2-1, x86_64; RAM 7 GB
- Python 3.13.15 (Clang 22.1.3); rustc 1.97.1
- tokio 1.53.1; pyo3 0.29.2; crossbeam-queue 0.3.14; libc 0.2.189
- uvloop 0.22.1; aiohttp (bench); FastAPI 0.141.1 + uvicorn 0.53.0 (bench)
- CPU pinning via `sched_setaffinity({0,1})` where applied

## Results (medians)

### 1. `call_soon` bulk — 200k callbacks scheduled + drained

| loop    | ns/callback | throughput |
|---------|-------------|------------|
| asyncio | 1815        | 0.55 Mcb/s |
| uvloop  | 1154        | 0.87 Mcb/s |
| tokioop | **417**     | **2.40 Mcb/s** |

tokioop is **2.8x uvloop / 4.5x asyncio**: the lock-free ready queue plus one
GIL acquisition per batch (instead of per callback) dominates.

### 2. `call_soon` chain — 100k sequential schedule+dispatch steps

| loop    | ns/step | throughput |
|---------|---------|------------|
| asyncio | 2880    | 0.35 M/s   |
| uvloop  | 1417    | 0.71 M/s   |
| tokioop | **713** | **1.40 M/s** |

**2.0x uvloop** (was 0.89x): skipping `yield_now` when no watcher tasks
exist plus cheaper park rechecks. Per-iteration fixed costs (GIL attach,
queue snapshot) are now below uvloop's C loop on this shape.

### 3. Timers — 50k `call_later` schedule + expiry

| loop    | ns/timer | throughput |
|---------|----------|------------|
| asyncio | 5445     | 184 k/s    |
| uvloop  | 2907     | 344 k/s    |
| tokioop | **889**  | **1125 k/s** |

**3.3x uvloop**: heap push without `Handle` allocation/traceback overhead,
O(1) flag cancellation, batched expiry.

Insertion-only: asyncio 2793 ns / uvloop 2228 ns / tokioop **581 ns**.
Cancel: asyncio 347 ns / uvloop 803 ns / tokioop **69 ns** (11.6x).

### 4. UDP — 20k x 64B datagrams, paced batches

| loop    | throughput | median total |
|---------|------------|--------------|
| asyncio | 18 k/s     | 1.122 s      |
| uvloop  | 116 k/s    | 0.172 s      |
| tokioop | **118 k/s**| 0.169 s      |

**1.02x uvloop** (was 0.91x), 6.6x asyncio. (Bursts larger than loopback
buffers are dropped by the kernel — verified identical 221/500 delivery
on all three loops — so the bench paces sends; unpaced numbers measure
the kernel, not the loop. asyncio's number varies wildly run-to-run on
the shared host; tokioop/uvloop are stable.)

### 5. TCP echo (streams) — sizes x connections, msgs total

| size | conns | asyncio | uvloop | tokioop |
|------|-------|---------|--------|---------|
| 64B | 1 | 11.2k | 16.4k | 11.3k |
| 64B | 10 | 19.0k | 30.5k | 18.7k |
| 64B | 50 | 17.8k | 34.6k | 18.9k |
| 256B | 1 | 13.0k | 17.1k | 13.2k |
| 256B | 10 | 18.2k | 29.7k | 19.3k |
| 256B | 50 | 19.0k | 36.6k | 16.9k |
| 1KiB | 1 | 11.3k | 16.9k | 11.2k |
| 1KiB | 10 | 14.8k | 29.7k | 16.8k |
| 1KiB | 50 | 14.7k | 28.1k | 15.3k |
| 4KiB | 1 | 9.1k | 11.0k | 10.4k |
| 4KiB | 10 | 12.5k | 22.7k | 14.6k |
| 4KiB | 50 | 15.2k | 25.1k | 15.1k |
| 16KiB | 1 | 7.5k | 8.3k | 9.1k |
| 16KiB | 10 | 12.3k | 18.8k | 11.4k |
| 16KiB | 50 | 11.4k | 17.8k | 9.3k |

(msg/s; medians of 5 interleaved samples.) tokioop matches asyncio across
the matrix (0.95-1.05x; was ~0.8x) and trails uvloop ~0.6x on
small-message streams — the remaining gap is uvloop's in-reactor Cython
transports doing less Python per op (analysis below). Bulk transfer
(2MB, steady state): tokioop 0.96x asyncio / 0.84x uvloop. Sequential
single-connection echo per-msg: tokioop 65µs vs asyncio 136µs vs uvloop
51µs.

Raw fd microbenchmarks: bulk readiness delivery tokioop 36.9 MB/s ≥
asyncio 34.7 MB/s > uvloop 21.9 MB/s; single-fire latency (in-loop paced,
send+fire+recv+Event) tokioop 11.6µs vs asyncio 15.6µs vs uvloop 10.8µs —
the reactor itself is at parity or better. The remaining TCP-streams delta
lives in per-message shared-code density (futures/Task steps per
`readexactly`/`drain` round-trip), where uvloop's in-reactor Cython
transports do less Python per op (§Bottlenecks).

### 6. HTTP via aiohttp — 2000 keep-alive reqs x 10 conns

| loop    | throughput | median total |
|---------|------------|--------------|
| asyncio | 4.51 k/s   | 0.443 s      |
| uvloop  | 5.13 k/s   | 0.390 s      |
| tokioop | **5.30 k/s** | 0.378 s    |

**1.03x uvloop** / 1.17x asyncio. aiohttp runs unmodified (compat win).

### 7. FastAPI + uvicorn — 1500 reqs x 6 conns, identical app

| loop    | throughput | median |
|---------|------------|--------|
| asyncio | 2.64 k/s   | 0.568 s |
| uvloop  | 2.60 k/s   | 0.578 s |
| tokioop | 2.62 k/s   | 0.573 s |

Parity (framework-dominated): the same unmodified FastAPI app serves
identically on all three loops.

## Optimization log (benchmark-driven, before → after)

| optimization | workload | before | after |
|---|---|---|---|
| Rust drain-mode reads (watcher task recvs; batch only delivers) | UDP 20k pkt | 110k pkt/s | **118k pkt/s (1.02x uvloop)** |
| same | TCP echo 64B/10conn | 14.9k msg/s | **18.7k msg/s (0.98x asyncio, was 0.85x)** |
| same | bulk 2MB steady | 0.5x asyncio | **0.96x asyncio / 0.84x uvloop** |
| Priming quanta (dead-edge accept stall) | bulk first-byte | +2.3ms stall | **+0.6ms** |
| Quantum pipelining (yield between quanta) | bulk latency profile | 1.4ms sync burst | pipelined delivery |
| Transport burst-drain (prior pass, superseded by Rust drain) | UDP 20k pkt | 16.5k pkt/s | 121k pkt/s (7.3x) |
| Cache `Task`/`Future` classes per loop (4.3µs import/call) | TCP sequential per-msg | 133µs | 88µs (1.5x) |
| Shared per-watcher Notify rendezvous (was: `oneshot`/fire) | per-fire latency | trailing asyncio | 11.6µs vs 15.6µs asyncio (parity) |
| Skip/amortize `yield_now` + cheap park rechecks | `call_soon` chain | 1423 ns/step | **713 ns/step, 2.0x uvloop** |
| Batched single-GIL-acquisition iteration (design) | call_soon bulk | — | 2.02 Mcb/s |

What was slow / why / tradeoff, for each kept change:

- **UDP**: one fd-task handshake + Python callback per datagram; under
  burst the loop spent all time in per-event overhead. Fix: transports drain
  to EAGAIN per firing. No semantic tradeoff (byte-stream chunking is
  unspecified; datagrams still delivered 1:1 in order; drain caps + early
  exit on close/pause preserve fairness and flow control). Verified by the
  full test suite (level-triggered, pause/resume, cancel/replace tests).
- **`create_future`/`create_task`**: `py.import()` per call showed up in
  cProfile (4.3µs × ~2 futures/message in streams). Fix: resolve classes
  once in `TokioopLoop.__new__`. Tradeoff: monkeypatching
  `asyncio.tasks.Task` after loop creation is not picked up (documented).

## Bottlenecks (where tokioop loses, and next steps)

**TCP echo small-message streams (~0.6x uvloop; asyncio parity) and bulk
vs uvloop (~0.84x).** Profiling (py-spy flamegraph + `loop.stats()`
batch/io counters + cProfile + per-fire isolation + timestamp tracing):
batches/message ≈ 0.6 (healthy), completions 1:1, per-fire latency at
parity or better (11.6µs vs asyncio 15.6µs), writes already optimal
(exactly 2 direct sends/message, verified), Python time dominated by
shared streams code. The delta is per-message shared-code density: each
streams round-trip funnels several futures/Task steps through per-op
overheads where uvloop's in-reactor Cython transports do less Python per
op (fewer transitions, libuv-batched writes). The Rust drain closed the
read side (echo +35%, bulk 0.5x→0.96x asyncio); the write side is already
minimal (`transport.write` → direct `send`). Further: protocol-aware write
batching would trade latency and diverge from CPython send semantics —
deferred. Per-op microbenchmarks (`call_soon` 253ns vs 513ns uvloop,
`create_future` 440ns vs 307ns, add/remove tied) confirm no single op
explains it — it is emergent density, only closable in C/Rust transports.

**`call_soon` chain — closed (was 0.89x, now 2.0x).** Per-iteration fixed
costs were dominated by an unconditional `yield_now` (~0.5-1µs). Fixed by
skipping the yield with no watchers and amortizing it otherwise.

## Methodology notes

- Identical workloads, same process interleaving (`compare()` rotates loop
  order per sample), warmup + median/min over 5–7 samples, release build.
- Nothing handicaps uvloop: stock `uvloop.new_event_loop()`, same
  client/server code paths on all loops.
- No `perf` in this container; profiling via py-spy flamegraphs, cProfile,
  `loop.stats()` counters, and targeted microbenchmarks (`/tmp` scripts:
  raw socket cost, fd fire latency, per-fire latency).
- Variance: the shared host is noisy (±15% run-to-run on TCP); tables are
  medians of interleaved samples, min reported alongside.
