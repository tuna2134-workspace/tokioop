//! Shared event-loop state.
//!
//! All hot-path scheduling state lives here, behind an `Arc<LoopState>` that
//! is shared between the Python-facing [`crate::event_loop::TokioopLoop`],
//! the Tokio I/O watcher tasks in [`crate::fd`], and any foreign threads
//! calling `call_soon_threadsafe`.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicI64, AtomicU64},
};
use std::time::Instant;

use crossbeam_queue::SegQueue;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use tokio::sync::Notify;
use tokio::task::AbortHandle;

/// One ready-to-run Python callback.
///
/// The ready queue is a lock-free [`SegQueue`]; `call_soon` only does a
/// single queue push plus Python refcount bumps, no locking, no GIL beyond
/// what argument handling already needs.
pub struct ReadyEntry {
    pub callback: Py<PyAny>,
    /// Already-allocated positional-argument tuple from the caller.
    pub args: Py<PyTuple>,
    /// Optional `contextvars.Context` to run the callback in.
    pub context: Option<Py<PyAny>>,
    /// Set by `Handle.cancel()`; checked at execution time (O(1) cancel).
    pub cancelled: Arc<AtomicBool>,
}

/// One scheduled timer. Ordered by `(when, seq)` so expiry is FIFO for
/// equal deadlines, matching `asyncio` timer ordering semantics.
pub struct TimerEntry {
    pub when: f64,
    pub seq: u64,
    pub entry: ReadyEntry,
}

impl PartialEq for TimerEntry {
    fn eq(&self, other: &Self) -> bool {
        self.when == other.when && self.seq == other.seq
    }
}
impl Eq for TimerEntry {}
impl PartialOrd for TimerEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for TimerEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse: BinaryHeap is a max-heap, we need earliest deadline first.
        other
            .when
            .partial_cmp(&self.when)
            .unwrap_or(Ordering::Equal)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

/// Mutable per-direction (read/write) state for a watched file descriptor.
pub struct FdSlotState {
    /// Current callback + args (callback mode). Cleared on detach so
    /// cancelled/replaced handles never run (asyncio `Handle` semantics).
    pub cb: Option<(Py<PyAny>, Py<PyTuple>)>,
    /// Drain-mode delivery callback. Intentionally KEPT on detach: already
    /// pushed completions must still deliver (the transport itself applies
    /// pause/close/cancelled checks and replays paused data), otherwise
    /// kernel-consumed payloads would be silently dropped on replace.
    pub drain: Option<Py<PyAny>>,
    /// Drain-mode error callback (same lifetime as `drain`).
    pub drain_err: Option<Py<PyAny>>,
    /// Set on detach (remove/close/replace/cancel). Drives `is_polling`
    /// and prevents new work; in-flight drain payloads still deliver.
    pub dead: bool,
    /// Cancellation flag of the currently-registered handle. Replaced (and
    /// the old flag set) on every `add_reader`/`add_writer` call, mirroring
    /// CPython cancelling the previous `Handle`.
    pub flag: Option<Arc<AtomicBool>>,
}

/// One registered read or write watcher on an fd.
pub struct FdDirection {
    pub slot: Arc<Mutex<FdSlotState>>,
    pub abort: AbortHandle,
}

/// All watcher state for one fd (keyed by the *original* fd number).
pub struct FdRecord {
    pub reader: Option<FdDirection>,
    pub writer: Option<FdDirection>,
}

/// A fired I/O notification waiting for the loop batch to execute the
/// Python-level reader/writer callback.
pub enum IoCompletion {
    /// Classic watcher firing: run `cb(*args)` from the slot, then release
    /// the watcher task via the shared rendezvous.
    Callback {
        slot: Arc<Mutex<FdSlotState>>,
        /// Shared per-watcher rendezvous: the task waits on this after pushing
        /// the completion; the loop batch notifies it after running the
        /// callback. Strictly 1:1 per fire (a task never has two completions
        /// outstanding), so a single shared `Notify` replaces a per-fire
        /// oneshot allocation. A stored permit covers batch-before-wait; the
        /// loop never waits for watchers, so no deadlock is possible.
        hs: Arc<Notify>,
    },
    /// Drain-mode delivery: the watcher task already performed the socket
    /// read in Rust (precise EAGAIN handling, no rendezvous, no extra
    /// syscalls); the batch only hands the payload to the transport's
    /// drain callback stored in the slot. Skipped silently if the slot is
    /// detached (remove/close replaces CPython's cancelled-handle skip).
    Data {
        slot: Arc<Mutex<FdSlotState>>,
        data: IoData,
    },
}

/// Payload read by a drain-mode watcher task.
pub enum IoData {
    /// One UDP datagram. `b""` payloads are real empty datagrams.
    UdpDatagram { payload: Vec<u8>, addr: AddrRepr },
    /// One TCP stream chunk (up to the transport's max read size).
    TcpChunk { payload: Vec<u8> },
    /// TCP EOF (`recv` returned 0). Never confused with data: stream
    /// `recv` yields `b""` only on EOF.
    TcpEof,
    /// Non-`WouldBlock` socket error observed while draining. The task
    /// keeps its registration (CPython reports `error_received`/fatal per
    /// firing and stays registered).
    ReadError { err: std::io::Error },
}

/// A datagram source address in GIL-free form; converted to the usual
/// asyncio address tuple at batch time.
#[derive(Clone)]
pub enum AddrRepr {
    Inet(std::net::SocketAddr),
    Unix(Vec<u8>),
    Unnamed,
}

/// The complete shared state of one event loop.
///
/// Locking discipline:
/// - `ready` / `io`: lock-free queues, no mutex on the fast path.
/// - `timers`: short mutex critical sections only (push/pop, never while
///   running Python code).
/// - `fds`: short critical sections only, never while running Python code
///   and never while holding `timers`.
/// - `exc_handler`: short critical sections only.
pub struct LoopState {
    pub ready: SegQueue<ReadyEntry>,
    pub timers: Mutex<BinaryHeap<TimerEntry>>,
    pub seq: AtomicU64,
    pub io: SegQueue<IoCompletion>,

    pub running: AtomicBool,
    pub stop_requested: AtomicBool,
    pub closed: AtomicBool,
    pub debug: AtomicBool,

    /// `time.monotonic()` sampled at loop creation; `time()` returns
    /// `t0_wall + t0.elapsed()`, so the clock matches `asyncio` semantics.
    pub t0_wall: f64,
    pub t0: Instant,

    /// Wakeup primitive: stored permit => never loses a wakeup between the
    /// queue drain and the park. Notified by `call_soon_threadsafe`,
    /// `stop`, and every I/O watcher firing.
    pub notify: Notify,

    pub exc_handler: Mutex<Option<Py<PyAny>>>,

    /// `threading.get_ident()` of the running thread, -1 when not running.
    pub thread_id: AtomicI64,

    pub fds: Mutex<HashMap<i32, FdRecord>>,

    /// Number of live fd watchers. Advisory fast path: the loop skips
    /// `yield_now` when no Tokio watcher task can have pending work.
    pub n_watchers: AtomicU64,

    /// Pre-run `sys.get_asyncgen_hooks()` value, restored afterwards.
    pub old_hooks: Mutex<Option<Py<PyAny>>>,

    /// Dedicated Tokio current-thread runtime, created once per loop and
    /// reused for its whole lifetime. Drives the reactor (AsyncFd
    /// watchers), timers used for parking, and the wakeup machinery.
    /// Single-threaded by design: matches CPython asyncio's
    /// predominantly single-threaded execution model with minimum
    /// cross-thread overhead. See [`RuntimeHolder`].
    pub rt: RuntimeHolder,

    // -- metrics (cheap atomics, read via stats()) --
    pub n_callbacks: AtomicU64,
    pub n_timers: AtomicU64,
    pub n_io_events: AtomicU64,
    /// Payload bytes read by drain tasks (for throughput analysis).
    pub n_read_bytes: AtomicU64,
    pub n_batches: AtomicU64,
    /// Park/wake cycles (for latency analysis).
    pub n_parks: AtomicU64,

    /// Cached `asyncio.Task` / `asyncio.Future` classes. `create_task`
    /// and `create_future` sit on hot paths (streams, queues, timeouts
    /// allocate futures constantly); a module import per call costs
    /// microseconds, so the classes are resolved once at loop creation.
    /// (Monkeypatching `asyncio.tasks.Task` after loop creation is not
    /// picked up — documented, matches uvloop-style tradeoffs.)
    pub task_cls: Py<PyAny>,
    pub future_cls: Py<PyAny>,
}

/// Owns the loop's Tokio runtime.
///
/// Dropping a Tokio runtime from inside an async context panics
/// ("Cannot drop a runtime in a context where blocking is not allowed").
/// A loop garbage-collected while *another* loop runs (e.g. cyclic trash
/// freed by `gc.collect()` inside a callback) would hit exactly that, so
/// the drop is shunted to a short-lived helper thread whenever called
/// from inside any runtime context. Loop teardown is off the hot path,
/// so the extra thread hop is irrelevant to benchmarked performance.
pub struct RuntimeHolder(pub Option<tokio::runtime::Runtime>);

impl RuntimeHolder {
    pub fn new(rt: tokio::runtime::Runtime) -> Self {
        Self(Some(rt))
    }
}

impl std::ops::Deref for RuntimeHolder {
    type Target = tokio::runtime::Runtime;
    fn deref(&self) -> &Self::Target {
        self.0.as_ref().expect("tokioop: runtime used after drop")
    }
}

impl Drop for RuntimeHolder {
    fn drop(&mut self) {
        if let Some(rt) = self.0.take() {
            if tokio::runtime::Handle::try_current().is_ok() {
                // Inside a runtime context: must not block here.
                let _ = std::thread::spawn(move || drop(rt)).join();
            } else {
                drop(rt);
            }
        }
    }
}

impl LoopState {
    /// Current loop time in `asyncio` (`time.monotonic()`) units.
    #[inline]
    pub fn now(&self) -> f64 {
        self.t0_wall + self.t0.elapsed().as_secs_f64()
    }

    /// Seconds until the next timer deadline, or `None` if no timers.
    /// Never negative (0.0 means "a timer is already due").
    pub fn next_deadline(&self) -> Option<std::time::Duration> {
        let timers = self.timers.lock().unwrap();
        let top = timers.peek()?;
        let dt = top.when - self.now();
        if dt <= 0.0 {
            Some(std::time::Duration::ZERO)
        } else {
            Some(std::time::Duration::from_secs_f64(dt))
        }
    }
}
