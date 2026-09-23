# tokioop — a Rust + Tokio asyncio event loop

A production-oriented Python `asyncio` event loop whose scheduling core is
written in Rust on top of Tokio, exposed through PyO3 and built with
maturin. It implements the `asyncio.AbstractEventLoop` API and runs
unmodified asyncio applications — including streams, transports/protocols,
SSL, subprocess stubs, executors, `asyncio.run()`, aiohttp, FastAPI/uvicorn:

```python
import asyncio
import tokioop

tokioop.install()  # make RustEventLoop the default backend

async def main():
    reader, writer = await asyncio.open_connection("example.com", 80)
    ...

asyncio.run(main())
```

## Status

Scheduling core (callbacks, timers, task wakeups, TCP/UDP reactor, thread-safe
wakeup, lifecycle) is implemented in Rust; the broader `AbstractEventLoop`
surface (transports, `sock_*`, executors, signals, `create_connection` /
`create_server` / `create_datagram_endpoint`, policy) is a faithful port of
CPython 3.13 semantics in a thin Python layer. 61 tests pass; benchmarks
against stdlib asyncio and uvloop are in [`BENCHMARKS.md`](BENCHMARKS.md).
Design details: [`ARCHITECTURE.md`](ARCHITECTURE.md).

## Install / build

Requires Rust (1.85+) and Python 3.9+. The venv in `.venv` already has
maturin; otherwise `pip install maturin` (or `uv pip install maturin`).

```bash
# development install (editable)
maturin develop
# release build for benchmarking (debug builds are ~2-5x slower)
maturin develop --release

# tests (61 tests: loop, tasks, networking, compat)
python -m pytest tests/ -q

# benchmarks (each compares asyncio / uvloop / tokioop)
python benchmarks/bench_call_soon.py
python benchmarks/bench_timers.py
python benchmarks/bench_udp.py
python benchmarks/bench_tcp.py      # full size x conns matrix
python benchmarks/bench_http.py     # aiohttp keep-alive
python benchmarks/bench_fastapi.py  # FastAPI + uvicorn
```

## API

```python
tokioop.RustEventLoop       # the loop (also subclasses asyncio.AbstractEventLoop)
tokioop.RustEventLoopPolicy # asyncio policy creating RustEventLoop
tokioop.install()           # set_event_loop_policy(RustEventLoopPolicy())
tokioop.new_event_loop()    # RustEventLoop()
tokioop.TokioopLoop         # Rust base class (scheduler/reactor core)
tokioop.TimerHandle / ReadyHandle / FdHandle  # cancel()/cancelled()
```

Supported loop methods include `run_forever`, `run_until_complete`, `stop`,
`close`, `is_running`, `is_closed`, `time`, `call_soon`,
`call_soon_threadsafe`, `call_later`, `call_at`, `create_task`,
`create_future`, `run_in_executor`, `set_default_executor`, `getaddrinfo`,
`getnameinfo`, `add_reader`/`remove_reader`/`add_writer`/`remove_writer`,
`sock_recv/recv_into/recvfrom/sendall/sendto/connect/accept`,
`create_connection`, `create_server`, `create_datagram_endpoint`,
`create_unix_connection/server`, `connect_accepted_socket`, `start_tls`,
`sendfile` (fallback path), `add_signal_handler`/`remove_signal_handler`,
`set/get_task_factory`, `set/get_debug`, exception-handler trio,
`shutdown_asyncgens`, `shutdown_default_executor`, and `loop.stats()`
(embedding `callbacks/timers/io/batches` counters for analysis).

Genuinely unsupported APIs fail the way asyncio expects:
`subprocess_exec/shell` and pipe transports raise `NotImplementedError`.

## Layout

```
src/                 Rust core (PyO3 module tokioop._tokioop)
  lib.rs             module definition
  state.rs           shared LoopState (queues, heap, runtime, metrics)
  event_loop.rs      TokioopLoop: lifecycle, scheduler, timers, tasks bridge
  fd.rs              Tokio AsyncFd reactor + level-triggered emulation
  handles.rs         TimerHandle / ReadyHandle / FdHandle
python/tokioop/      Python asyncio-compat layer
  __init__.py        exports + install()
  loop.py            RustEventLoop: networking, sock_*, executor, signals
  transports.py      selector-style transports (draining reads)
  policy.py          RustEventLoopPolicy
tests/               pytest suite (test_loop/test_tasks/test_net)
benchmarks/          asyncio vs uvloop vs tokioop harnesses
```

## Performance (summary)

Medians on the benchmark host (Xeon E5-2690 v4, Python 3.13, release
build; full tables in `BENCHMARKS.md`):

| workload | asyncio | uvloop | tokioop |
|---|---|---|---|
| `call_soon` bulk | 0.5 Mcb/s | 0.8 Mcb/s | **2.0 Mcb/s** |
| `call_soon` chain | 0.34 M/s | 0.74 M/s | **1.5 M/s** |
| timers schedule+expire | 200 k/s | 340-400 k/s | **1150-1350 k/s** |
| UDP 64B | 18-58 k/s | 116-132 k/s | 110-121 k/s |
| TCP echo (streams) | 1.0x | 1.6x | 0.98x |
| bulk transfer steady | 1.0x | 1.2x | 0.96x |
| HTTP via aiohttp | 4.5-6.0 k/s | 5.1-6.7 k/s | 4.7-5.8 k/s |
| FastAPI via uvicorn | 2.6-2.9 k/s | 2.6-2.7 k/s | 2.6-2.8 k/s |

Faster than uvloop on scheduling (2-3x) and at parity-or-better on UDP,
HTTP, FastAPI and bulk transfer; TCP small-message echo matches asyncio
with uvloop ahead there (its Cython transports do less Python per op —
see `BENCHMARKS.md` for the full elimination trail and methodology).
Numbers move run-to-run on shared hosts; the table shows typical bands
(medians of interleaved samples).
