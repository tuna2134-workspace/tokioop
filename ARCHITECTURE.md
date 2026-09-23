# Architecture

```
                 ┌─────────────────────────┐
                 │  Python asyncio apps    │
                 │  AbstractEventLoop API  │
                 └────────────┬────────────┘
                              │ PyO3 (boundary)
                 ┌────────────▼────────────┐
                 │   RustEventLoop (Py)    │  python/tokioop/loop.py
                 │   transports (Py)       │  python/tokioop/transports.py
                 └────────────┬────────────┘
                              │
                 ┌────────────▼────────────┐
                 │      TokioopLoop        │  src/event_loop.rs
                 │  Ready Queue (lock-free)│  SegQueue, no mutex on call_soon
                 │  Timer heap             │  BinaryHeap (when, seq), O(1) cancel
                 │  Task wakeups           │  real asyncio.Task, Rust routing
                 │  Batched GIL execution  │  1 acquisition per iteration
                 │  Exception routing      │  call_exception_handler parity
                 └────────────┬────────────┘
                              │
                 ┌────────────▼────────────┐
                 │  fd reactor (per-fd     │  src/fd.rs
                 │  Tokio tasks)           │
                 └────────────┬────────────┘
                              │
                 ┌────────────▼────────────┐
                 │  Tokio current_thread   │  1 runtime per loop, reused
                 │  runtime (reactor +     │  for the loop's lifetime
                 │  timers + park)         │
                 └─────────────────────────┘
```

Python compatibility lives at the boundary; the hot path lives in Rust.

## Event-loop iteration (`run_main`, `src/event_loop.rs`)

1. **Batch** (GIL acquired once): run a snapshot of the ready queue
   (CPython `ntodo` semantics — work scheduled mid-batch runs next
   iteration), then expired timers in deadline order, then I/O completions.
   Cancelled entries are skipped via O(1) flags, never removed eagerly.
2. Check `stop` / `close`.
3. If work remains (`ready`/`io` non-empty via cheap `is_empty`, or a timer
   already due — clock read deferred until the heap is known non-empty),
   re-drain **without parking**. This mirrors CPython checking `_ready` at
   the top of every `_run_once` and is what makes plain `call_soon` chains
   work with no wakeup.
4. Fairness yield: `yield_now` hands the runtime to woken watcher tasks,
   but only when watchers exist (`n_watchers`) and amortized every 16th
   spin — a yield costs ~0.5-1µs and parks already drive the runtime, so
   yielding every spin is pure overhead on callback-heavy bursts.
   (Measured 2.2x on the chained-callback bench.)
5. Otherwise park: `Notify::notified()` with no timers, or
   `tokio::time::timeout(deadline, notified())`. `Notify` stores a permit,
   so wakeups landing between the drain and the park are never lost.
   The GIL is released for the entire park.

`run_forever` blocks inside the loop's dedicated Tokio `current_thread`
runtime (`block_on`), so watcher tasks, timer parking and callback batches
share one thread. Current-thread (not multi-thread) is deliberate: asyncio
workloads are single-threaded, and it avoids cross-thread scheduler hops
on the hot path. The runtime is created once per loop, never per operation.

## Callback scheduler

`call_soon` = one lock-free `SegQueue` push (plus storing the caller's
already-allocated args tuple and an `Arc<AtomicBool>` cancel flag).
`call_soon_threadsafe` adds a `Notify::notify_one` (permit-storing, cheap
when the loop is awake). No mutex, no GIL beyond argument handling, no
per-callback Python object beyond the returned handle (same as CPython).

## Timers

`BinaryHeap<TimerEntry>` ordered by `(deadline, seq)` — deadline ordering
with FIFO for ties, matching asyncio. `loop.time()` =
`monotonic(t0) + Instant::elapsed`, so the clock matches asyncio semantics.
Cancellation flips the flag; expiry skips flagged entries (no heap removal).

## Python Task integration (kept in Python, routed in Rust)

`create_task` builds a real `asyncio.Task(coro, loop=self)` (C `_asyncio`
Task works too — it only needs the loop's `call_soon`/`call_later`/etc.).
Rust never reimplements coroutine semantics; it only makes wakeups cheap:
fast ready queue, batched execution, Tokio-driven I/O notifications that
land as ordinary `call_soon`-equivalent completions. `Task`/`Future` classes
are resolved once at loop creation (hot paths like `readexactly` allocate
futures constantly; a module import per call cost ~4µs).

## fd reactor (`src/fd.rs`, Linux-first)

Platform layout: `src/fd.rs` holds the shared watcher bookkeeping plus a
`#[cfg(unix)]` / `#[cfg(not(unix))]` dispatch to `fd_unix.rs` (Tokio
`AsyncFd` reactor) or `fd_stub.rs` (Windows: scheduling/timers/tasks work;
fd I/O entry points raise `NotImplementedError`, asyncio's own failure
mode for unsupported transports; an IOCP-backed reactor is future work).
macOS/BSD ride the unix reactor with two adaptations: `O_NONBLOCK` on the
owned dup at registration (no `MSG_DONTWAIT` there) and size-based IPv6
byte reads (no `in6_addr` union field names). Cross-checked with
`cargo check` / `cross check` for Windows (x86_64/aarch64 MSVC), macOS
(aarch64), 32-bit + aarch64 Linux, and musl; see `Cross.toml`.

Each watched direction gets: a `dup`'d fd owned by Rust (stable lifetime,
race-free probing), an `AsyncFd` on the loop runtime, one Tokio task, and
one shared rendezvous `Notify`. Tokio readiness is edge-triggered; asyncio
is level-triggered. Two read paths bridge them:

**Callback mode** (`add_reader`/`add_writer`, used by `sock_*` helpers and
user code): the task emulates level triggering with a single `poll(2)`
probe after every callback (still ready → re-fire, else clear + park).
One priming poll per registration covers re-registration with pending I/O.
The Python callback runs in the batched GIL section; a shared per-watcher
`Notify` rendezvous (zero per-fire allocation) releases the task.

**Drain mode** (`_add_tcp_reader`/`_add_udp_reader`, used by transports):
the watcher task performs socket reads itself (`recv`/`recvfrom` with
`MSG_DONTWAIT`, never touching socket flags) and pushes one completion per
datagram/chunk; the batch only hands payloads to the transport. No
rendezvous, no `poll(2)`, no Python `recv` — reads run until `EAGAIN`
(which doubles as the level-triggered probe), in small quanta (UDP: 16
datagrams; TCP: 4 chunks/256KB) with a yield between quanta whenever more
work may follow, so batches deliver incrementally (pipelined latency,
fairness under flood). Parking is stall-free by construction (flag cleared
only after observed drain); a priming quantum covers data that predates
registration (accept-time bulk would otherwise stall ~2ms on a dead edge).
TCP EOF (`recv` → 0) and socket errors become completions; the task yields
once on EOF so the batch processes the transport's synchronous removal.

Pause semantics are preserved without loss: drain-mode pause is flag-only
(the parked task fires on new arrivals and payloads route to a transport
replay buffer of re-delivery closures); `resume_reading` flushes replay
synchronously before new reads, preserving order across data/error/EOF.
`BufferedProtocol` falls back to callback mode (zero-copy `get_buffer`
cannot work with pre-read data); mode switches in `set_protocol`
re-register the watcher.

- `AsyncFdReadyGuard` is `#[must_use]` and **drop does not clear** (verified
  against the Tokio 1.53 source); every path that observes drain clears
  explicitly. A post-guard edge can cause at most one bounded spurious
  wakeup (callback observes `EAGAIN`), never a spin and never a stall.
  On a single-threaded runtime the flag cannot change across sync
  poll→clear windows (no await), so clears always match observations.

Replacing a watcher cancels the previous handle (CPython parity);
`remove_*` detaches + aborts the task; queued completions for dead slots
are skipped. `close()` aborts all watchers and drains queues.

## Transports (`python/tokioop/transports.py`)

Ported from CPython 3.13 selector transports (same flow control, EOF,
buffering, `sendmsg` paths); deliberate, semantics-preserving divergences:

- drain-mode reads (default): socket I/O happens in the Rust watcher task
  (§fd reactor); the transport only handles delivery (`_drain_stream` /
  `_drain_datagram`), pause replay, errors, EOF and teardown.
- pause is flag-only in drain mode with a replay buffer (no data loss,
  order preserved); callback mode keeps classic remove-on-pause.
- `__repr__` polling state comes from `loop._is_polling`, not a selector.

SSL reuses stdlib `sslproto.SSLProtocol`; `Server` is stdlib
`base_events.Server` driven by our `_start_serving`/`_stop_serving`.

## GIL strategy

Held for exactly one batch per iteration (ready + timers + I/O), released
for parking, Tokio polling, and all Rust-only work. Per-callback
acquire/release is avoided; cross-thread scheduling uses the Notify
permit (no syscall-equivalent cost when awake).

## Shutdown / safety

- `close()` (not while running): abort watchers, clear queues, non-blocking
  default-executor shutdown. Repeated create/run/close cycles tested.
- The Tokio runtime is owned via `RuntimeHolder`, whose `Drop` shunts to a
  helper thread when dropped inside any async context (a loop freed by
  `gc.collect()` inside a running loop would otherwise panic — Tokio
  forbids dropping a runtime from async context).
- `unsafe` is limited to: `libc::dup` + `OwnedFd::from_raw_fd` (fresh fd,
  checked), `libc::poll`/`recv`/`recvfrom` on an owned dup with
  `MSG_DONTWAIT` (never blocks; buffers are valid slices; sockaddr parsing
  is family-checked with bounded lengths). No unsafe in data handling
  beyond these audited FFI boundaries.

## What lives where (and why)

- `src/state.rs` — shared state + locking discipline docs.
- `src/event_loop.rs` — loop type, lifecycle, scheduler, task bridge.
- `src/fd.rs` — reactor tasks, readiness discipline.
- `src/handles.rs` — cancel handles.
- `python/tokioop/loop.py` — networking/sock/executor/signals/shutdown
  (ported stdlib semantics; never hot-path).
