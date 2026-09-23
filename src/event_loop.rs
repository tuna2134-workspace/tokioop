//! The Python-facing `asyncio`-compatible event loop.
//!
//! Architecture (hot path in Rust, compatibility at the boundary):
//!
//! - Ready callbacks live in a lock-free [`SegQueue`][crossbeam_queue::SegQueue];
//!   `call_soon` is a single push, `call_soon_threadsafe` a push plus a
//!   (permit-storing, syscall-free when parked) [`Notify`][tokio::sync::Notify]
//!   wakeup.
//! - Timers live in a [`BinaryHeap`] min-heap keyed by `(deadline, seq)`;
//!   cancellation is O(1) via a flag checked at expiry.
//! - `run_forever` blocks inside the loop's dedicated Tokio current-thread
//!   runtime (`block_on`), so Tokio watcher tasks, timer parking and the
//!   Python callback batches all share one thread with no runtime
//!   creation/teardown per operation.
//! - Each loop iteration acquires the GIL **once** and executes the whole
//!   batch (ready snapshot + expired timers + I/O completions) before
//!   releasing it to park. This is the central boundary-crossing
//!   optimization: per-callback GIL acquisition is avoided.
//! - Python coroutine semantics stay in Python: `create_task` builds a real
//!   `asyncio.Task` bound to this loop; Rust only routes wakeups faster.

use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::sync::{
    Arc,
    Mutex,
    atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
};
use std::time::Instant;

use crossbeam_queue::SegQueue;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyTuple};
use tokio::sync::Notify;

use crate::handles::{FdHandle, ReadyHandle, TimerHandle};
use crate::state::{
    AddrRepr, FdSlotState, IoData, LoopState, ReadyEntry, RuntimeHolder, TimerEntry,
};
use crate::fd::DrainKind;

// ---------------------------------------------------------------------------
// callback invocation + error routing
// ---------------------------------------------------------------------------

fn invoke_entry(
    py: Python,
    cb: &Py<PyAny>,
    args: &Py<PyTuple>,
    ctx: Option<&Py<PyAny>>,
) -> PyResult<()> {
    match ctx {
        None => {
            cb.bind(py).call(args.clone_ref(py), None)?;
            Ok(())
        }
        Some(c) => {
            // contextvars.Context.run(callback, *args)
            let ab = args.bind(py);
            let mut items = Vec::with_capacity(ab.len() + 1);
            items.push(cb.bind(py).clone().into_any());
            items.extend(ab.iter());
            let tup = PyTuple::new(py, items)?;
            c.bind(py).call_method1("run", tup)?;
            Ok(())
        }
    }
}

fn report_callback_error(py: Python, state: &LoopState, err: PyErr, message: &str) {
    let context = PyDict::new(py);
    let _ = context.set_item("message", message);
    let _ = context.set_item("exception", err.value(py));
    if let Err(e) = dispatch_exception_handler(py, state, &context) {
        eprintln!("tokioop: error in exception handler: {e}");
    }
}

/// Deliver one drain-mode payload to the transport's drain callback.
///
/// The transport itself applies pause/close/cancelled checks (replaying
/// paused data) and converts protocol errors into transport teardown, so
/// this stays a thin, fast handoff: one `PyBytes` (+ address tuple) copy
/// per item, no rendezvous, no extra syscalls.
fn deliver_drain_data(
    py: Python,
    state: &LoopState,
    slot: &Arc<Mutex<FdSlotState>>,
    data: IoData,
) {
    let (drain, drain_err) = {
        let guard = slot.lock().unwrap();
        match (
            guard.drain.as_ref().map(|d| d.clone_ref(py)),
            guard.drain_err.as_ref().map(|d| d.clone_ref(py)),
        ) {
            (Some(d), e) => (d, e),
            // Detached between push and execution (remove/close/replace):
            // skip, mirroring cancelled-handle semantics.
            (None, _) => return,
        }
    };
    let res = match data {
        IoData::UdpDatagram { payload, addr } => {
            let bytes = PyBytes::new(py, &payload);
            match addr_to_py(py, &addr) {
                Ok(addr) => drain.bind(py).call1((bytes, addr)),
                Err(err) => Err(err),
            }
        }
        IoData::TcpChunk { payload } => {
            let bytes = PyBytes::new(py, &payload);
            drain.bind(py).call1((bytes,))
        }
        // Stream `recv` yields `b""` only on EOF — unambiguous marker.
        IoData::TcpEof => {
            let empty = PyBytes::new(py, b"");
            drain.bind(py).call1((empty,))
        }
        IoData::ReadError { err } => match drain_err {
            Some(handler) => {
                let exc = PyErr::from(err);
                handler.bind(py).call1((exc,))
            }
            None => Ok(PyDict::new(py).into_any()),
        },
    };
    if let Err(err) = res {
        report_callback_error(py, state, err, "Exception in callback");
    }
}

/// Convert a drain-mode source address to the usual asyncio address tuple.
fn addr_to_py<'py>(py: Python<'py>, addr: &AddrRepr) -> PyResult<Bound<'py, PyAny>> {
    match addr {
        AddrRepr::Inet(sa) => match sa {
            std::net::SocketAddr::V4(v) => {
                (v.ip().to_string(), v.port()).into_pyobject(py).map(|t| t.into_any())
            }
            std::net::SocketAddr::V6(v) => (
                v.ip().to_string(),
                v.port(),
                v.flowinfo(),
                v.scope_id(),
            )
                .into_pyobject(py)
                .map(|t| t.into_any()),
        },
        // Filesystem path; abstract addresses (leading NUL) pass through
        // as bytes, matching CPython's recvfrom conventions.
        AddrRepr::Unix(path) => {
            if path.first() == Some(&0) {
                Ok(PyBytes::new(py, path).into_any())
            } else {
                Ok(String::from_utf8_lossy(path).into_pyobject(py)?.into_any())
            }
        }
        AddrRepr::Unnamed => Ok(py.None().into_bound(py)),
    }
}

fn dispatch_exception_handler(
    py: Python,
    state: &LoopState,
    context: &Bound<'_, PyDict>,
) -> PyResult<()> {    let handler = state
        .exc_handler
        .lock()
        .unwrap()
        .as_ref()
        .map(|h| h.clone_ref(py));
    match handler {
        Some(h) => {
            if let Err(e) = h.bind(py).call1((context,)) {
                eprintln!("tokioop: exception in custom exception handler: {e}");
            }
            Ok(())
        }
        None => default_exception_handler_impl(py, context),
    }
}

fn default_exception_handler_impl(py: Python, context: &Bound<'_, PyDict>) -> PyResult<()> {
    let message: String = context
        .get_item("message")?
        .map(|m| m.extract::<String>())
        .transpose()?
        .unwrap_or_else(|| "Unhandled exception in event loop".to_string());
    eprintln!("tokioop: {message}");
    if let Some(exc) = context.get_item("exception")? {
        let tb = py.import("traceback")?;
        tb.getattr("print_exception")?.call1((exc,))?;
    } else {
        eprintln!("tokioop: context: {context:?}");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// one loop iteration (runs with the GIL held, once per iteration)
// ---------------------------------------------------------------------------

pub(crate) fn run_batch(py: Python, state: &LoopState) {
    state.n_batches.fetch_add(1, Ordering::Relaxed);
    // 1. Ready callbacks. Snapshot the queue length first so callbacks
    // scheduled *during* this batch run next iteration (CPython `_run_once`
    // `ntodo` semantics; also prevents starvation by infinitely
    // rescheduling callbacks).
    let n = state.ready.len();
    for _ in 0..n {
        let Some(e) = state.ready.pop() else {
            break;
        };
        if e.cancelled.load(Ordering::SeqCst) {
            continue;
        }
        state.n_callbacks.fetch_add(1, Ordering::Relaxed);
        if let Err(err) = invoke_entry(py, &e.callback, &e.args, e.context.as_ref()) {
            report_callback_error(py, state, err, "Exception in callback");
        }
    }

    // 2. Expired timers -> run inline in deadline order.
    let now = state.now();
    loop {
        let entry = {
            let mut timers = state.timers.lock().unwrap();
            match timers.peek() {
                Some(top) if top.when <= now => timers.pop().map(|t| t.entry),
                _ => None,
            }
        };
        let Some(e) = entry else { break };
        if e.cancelled.load(Ordering::SeqCst) {
            continue;
        }
        state.n_timers.fetch_add(1, Ordering::Relaxed);
        if let Err(err) = invoke_entry(py, &e.callback, &e.args, e.context.as_ref()) {
            report_callback_error(py, state, err, "Exception in callback");
        }
    }

    // 3. I/O completions.
    while let Some(comp) = state.io.pop() {
        match comp {
            crate::state::IoCompletion::Callback { slot, hs } => {
                let cb = slot
                    .lock()
                    .unwrap()
                    .cb
                    .as_ref()
                    .map(|(c, a)| (c.clone_ref(py), a.clone_ref(py)));
                if let Some((cb, args)) = cb {
                    if let Err(err) = invoke_entry(py, &cb, &args, None) {
                        report_callback_error(py, state, err, "Exception in callback");
                    }
                }
                hs.notify_one();
            }
            crate::state::IoCompletion::Data { slot, data } => {
                deliver_drain_data(py, state, &slot, data);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// the parked loop future (runs without the GIL except inside run_batch)
// ---------------------------------------------------------------------------

async fn run_main(state: Arc<LoopState>) {
    // Consecutive no-park iterations (saturate-spin). `yield_now` hands
    // the runtime to woken watcher tasks for fairness, but at ~0.5-1µs a
    // yield every iteration is pure overhead on compute-only bursts, so it
    // is amortized: park cycles (the common case) drive the runtime anyway.
    let mut spin: u32 = 0;
    loop {
        Python::attach(|py| run_batch(py, &state));

        if state.stop_requested.load(Ordering::SeqCst) {
            break;
        }
        if state.closed.load(Ordering::SeqCst) {
            break;
        }
        // Work scheduled *during* the batch (the common case: a Task step
        // reschedules itself via plain `call_soon`, which carries no
        // wakeup by asyncio contract) must run without parking first.
        // CPython does the equivalent by checking `_ready` at the top of
        // every `_run_once`.
        if !state.ready.is_empty() || !state.io.is_empty() || timers_due(&state) {
            // Let Tokio watcher tasks progress before re-draining — but
            // only when such tasks exist; otherwise the yield is pure
            // overhead on compute-only workloads (e.g. callback chains).
            // NOTE: no amortization here: woken tasks only run at park or
            // yield points, so skipping yields delays their next fire by
            // whole spin windows (head-of-line per connection).
            spin = spin.wrapping_add(1);
            if state.n_watchers.load(Ordering::Relaxed) != 0 {
                tokio::task::yield_now().await;
            }
            continue;
        }
        spin = 0;
        // Park until: a wakeup (threadsafe schedule / I/O / stop) or the
        // next timer deadline. `Notify` stores a permit, so a wakeup that
        // lands between the checks above and this park is never lost.
        state.n_parks.fetch_add(1, Ordering::Relaxed);
        match state.next_deadline() {
            None => {
                state.notify.notified().await;
            }
            Some(d) => {
                if d.is_zero() {
                    // A timer is already due (scheduled during the batch);
                    // yield so Tokio tasks progress, then re-drain.
                    tokio::task::yield_now().await;
                } else {
                    let _ = tokio::time::timeout(d, state.notify.notified()).await;
                }
            }
        }
    }
}

/// True if a timer deadline has already passed (checked without popping).
fn timers_due(state: &LoopState) -> bool {
    let timers = state.timers.lock().unwrap();
    if timers.is_empty() {
        return false;
    }
    match timers.peek() {
        Some(top) => top.when <= state.now(),
        None => false,
    }
}

// ---------------------------------------------------------------------------
// checks
// ---------------------------------------------------------------------------

fn check_closed_state(state: &LoopState) -> PyResult<()> {
    if state.closed.load(Ordering::SeqCst) {
        return Err(PyRuntimeError::new_err("Event loop is closed"));
    }
    Ok(())
}

fn check_running_state(py: Python, state: &LoopState) -> PyResult<()> {
    if state.running.load(Ordering::SeqCst) {
        return Err(PyRuntimeError::new_err(
            "This event loop is already running",
        ));
    }
    let events = py.import("asyncio.events")?;
    let other = events.getattr("_get_running_loop")?.call0()?;
    if !other.is_none() {
        return Err(PyRuntimeError::new_err(
            "Cannot run the event loop while another loop is running",
        ));
    }
    Ok(())
}

/// Accept an int fd or a file object with `fileno()` (like selectors do).
fn extract_fd(fd: &Bound<'_, PyAny>) -> PyResult<i32> {
    if let Ok(n) = fd.extract::<i32>() {
        return Ok(n);
    }
    fd.getattr("fileno")?
        .call0()?
        .extract::<i32>()
        .map_err(|_| {
            pyo3::exceptions::PyValueError::new_err(format!("Invalid file object: {fd:?}"))
        })
}

fn check_callback(py: Python, cb: &Bound<'_, PyAny>, method: &str) -> PyResult<()> {    let co = py.import("asyncio.coroutines")?;
    let is_coro: bool = co.getattr("iscoroutine")?.call1((cb,))?.extract()?;
    let is_coro_fn: bool = co
        .getattr("iscoroutinefunction")?
        .call1((cb,))?
        .extract()?;
    if is_coro || is_coro_fn {
        return Err(PyTypeError::new_err(format!(
            "coroutines cannot be used with {method}()"
        )));
    }
    if !cb.is_callable() {
        return Err(PyTypeError::new_err(format!(
            "a callable object was expected by {method}(), got {cb:?}"
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// run_forever core
// ---------------------------------------------------------------------------

fn run_forever_impl(slf: &Py<TokioopLoop>, py: Python) -> PyResult<()> {
    let state = slf.bind(py).borrow().state.clone();
    check_closed_state(&state)?;
    check_running_state(py, &state)?;

    let obj = slf.bind(py);
    let obj_any = obj.as_any();

    // Mirror BaseEventLoop._run_forever_setup: asyncgen hooks, thread id,
    // running-loop registration.
    let sys = py.import("sys")?;
    let old_hooks = sys.getattr("get_asyncgen_hooks")?.call0()?;
    *state.old_hooks.lock().unwrap() = Some(old_hooks.unbind());
    let hooks_kwargs = PyDict::new(py);
    hooks_kwargs.set_item("firstiter", obj_any.getattr("_asyncgen_firstiter_hook")?)?;
    hooks_kwargs.set_item("finalizer", obj_any.getattr("_asyncgen_finalizer_hook")?)?;
    sys.getattr("set_asyncgen_hooks")?
        .call((), Some(&hooks_kwargs))?;

    let tid: i64 = py
        .import("threading")?
        .getattr("get_ident")?
        .call0()?
        .extract()?;
    state.thread_id.store(tid, Ordering::SeqCst);

    let events = py.import("asyncio.events")?;
    events
        .getattr("_set_running_loop")?
        .call1((obj_any,))?;

    state.stop_requested.store(false, Ordering::SeqCst);
    state.running.store(true, Ordering::SeqCst);

    // Release the GIL while blocked: Tokio drives watchers/timers and other
    // threads can run Python. Re-acquired per batch inside run_main.
    let state2 = state.clone();
    py.detach(move || state2.rt.block_on(run_main(state2.clone())));

    // Mirror _run_forever_cleanup.
    state.running.store(false, Ordering::SeqCst);
    state.stop_requested.store(false, Ordering::SeqCst);
    state.thread_id.store(-1, Ordering::SeqCst);
    let none = py.None();
    events.getattr("_set_running_loop")?.call1((none,))?;
    if let Some(old) = state.old_hooks.lock().unwrap().take() {
        let oldb = old.bind(py);
        let first = oldb.getattr("firstiter")?;
        let finalizer = oldb.getattr("finalizer")?;
        sys.getattr("set_asyncgen_hooks")?.call1((first, finalizer))?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// the loop type
// ---------------------------------------------------------------------------

/// High-performance `asyncio`-compatible event loop backed by Tokio.
///
/// Allocation note: `TokioopLoop` itself is a thin handle; everything lives
/// in the shared [`LoopState`].
#[pyclass(module = "tokioop._tokioop", subclass)]
pub struct TokioopLoop {
    state: Arc<LoopState>,
}

#[pymethods]
impl TokioopLoop {
    #[new]
    fn new(py: Python) -> PyResult<Self> {
        let t0_wall: f64 = py
            .import("time")?
            .getattr("monotonic")?
            .call0()?
            .extract()?;
        // One dedicated current-thread runtime per loop, reused for the
        // loop's whole lifetime. Current-thread (not multi-thread):
        // asyncio workloads are predominantly single-threaded, and this
        // avoids cross-thread scheduler hops on the hot path.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .thread_name("tokioop-loop")
            .build()
            .map_err(|e| PyRuntimeError::new_err(format!("failed to create Tokio runtime: {e}")))?;
        // Resolve Task/Future classes once (see LoopState docs).
        let task_cls = py
            .import("asyncio.tasks")?
            .getattr("Task")?
            .unbind();
        let future_cls = py
            .import("asyncio.futures")?
            .getattr("Future")?
            .unbind();
        Ok(Self {
            state: Arc::new(LoopState {
                ready: SegQueue::new(),
                timers: Mutex::new(BinaryHeap::new()),
                seq: AtomicU64::new(0),
                io: SegQueue::new(),
                running: AtomicBool::new(false),
                stop_requested: AtomicBool::new(false),
                closed: AtomicBool::new(false),
                debug: AtomicBool::new(false),
                t0_wall,
                t0: Instant::now(),
                notify: Notify::new(),
                exc_handler: Mutex::new(None),
                thread_id: AtomicI64::new(-1),
                fds: Mutex::new(HashMap::new()),
                old_hooks: Mutex::new(None),
                rt: RuntimeHolder::new(rt),
                n_callbacks: AtomicU64::new(0),
                n_timers: AtomicU64::new(0),
                n_io_events: AtomicU64::new(0),
                n_batches: AtomicU64::new(0),
                n_parks: AtomicU64::new(0),
                n_watchers: AtomicU64::new(0),
                task_cls,
                future_cls,
            }),
        })
    }

    /// Compatibility attributes expected by transports, `asyncio.run`,
    /// executors and asyncgen shutdown.
    fn __init__(slf: Bound<'_, Self>) -> PyResult<()> {
        let py = slf.py();
        let weakref = py.import("weakref")?;
        let obj = slf.as_any();
        obj.setattr(
            "_transports",
            weakref.getattr("WeakValueDictionary")?.call0()?,
        )?;
        obj.setattr("_asyncgens", weakref.getattr("WeakSet")?.call0()?)?;
        obj.setattr("_asyncgens_shutdown_called", false)?;
        obj.setattr("_default_executor", py.None())?;
        obj.setattr("_executor_shutdown_called", false)?;
        obj.setattr("_task_factory", py.None())?;
        Ok(())
    }

    fn __repr__(&self) -> String {
        format!(
            "<TokioopLoop running={} closed={} debug={}>",
            self.state.running.load(Ordering::SeqCst),
            self.state.closed.load(Ordering::SeqCst),
            self.state.debug.load(Ordering::SeqCst),
        )
    }

    // -- lifecycle --

    /// Run until `stop()` is called.
    fn run_forever(slf: Bound<'_, Self>) -> PyResult<()> {
        let owned: Py<TokioopLoop> = slf.clone().unbind();
        let py = slf.py();
        run_forever_impl(&owned, py)
    }

    /// Run until `future` is done; return its result (or raise).
    fn run_until_complete(
        slf: Bound<'_, Self>,
        future: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let state = slf.borrow().state.clone();
        check_closed_state(&state)?;
        check_running_state(py, &state)?;

        let futures_mod = py.import("asyncio.futures")?;
        let tasks_mod = py.import("asyncio.tasks")?;
        let new_task: bool = !futures_mod
            .getattr("isfuture")?
            .call1((future.bind(py),))?
            .extract::<bool>()?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("loop", slf.as_any())?;
        let fut = tasks_mod
            .getattr("ensure_future")?
            .call((future.bind(py),), Some(&kwargs))?;
        if new_task {
            // No external owner: consume a raised exception quietly like
            // BaseEventLoop does (`_log_destroy_pending = False`).
            fut.setattr("_log_destroy_pending", false)?;
        }
        let cb = py
            .import("asyncio.base_events")?
            .getattr("_run_until_complete_cb")?;
        fut.call_method1("add_done_callback", (cb.clone(),))?;

        let owned: Py<TokioopLoop> = slf.clone().unbind();
        let run_result = run_forever_impl(&owned, py);

        // finally: detach the stop callback (ignore errors: the future may
        // be gone if the loop was closed around us).
        let _ = fut.call_method1("remove_done_callback", (cb,));
        if let Err(e) = run_result {
            if new_task {
                let done: bool = fut.call_method0("done")?.extract()?;
                let cancelled: bool = fut.call_method0("cancelled")?.extract()?;
                if done && !cancelled {
                    let _ = fut.call_method0("exception");
                }
            }
            return Err(e);
        }
        let done: bool = fut.call_method0("done")?.extract()?;
        if !done {
            return Err(PyRuntimeError::new_err(
                "Event loop stopped before Future completed.",
            ));
        }
        Ok(fut.call_method0("result")?.unbind())
    }

    /// Ask the loop to stop after the current batch.
    fn stop(&self) {
        self.state.stop_requested.store(true, Ordering::SeqCst);
        self.state.notify.notify_one();
    }

    /// Close the loop: abort watchers, drop pending work, shut down the
    /// (non-waiting) default executor. Mirrors `BaseEventLoop.close`.
    fn close(slf: Bound<'_, Self>) -> PyResult<()> {
        let py = slf.py();
        let state = slf.borrow().state.clone();
        if state.running.load(Ordering::SeqCst) {
            return Err(PyRuntimeError::new_err(
                "Cannot close a running event loop",
            ));
        }
        if state.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        crate::fd::abort_all(&state);
        while state.ready.pop().is_some() {}
        state.timers.lock().unwrap().clear();
        while state.io.pop().is_some() {}

        let obj = slf.as_any();
        let _ = obj.setattr("_executor_shutdown_called", true);
        let exc = obj.getattr("_default_executor")?;
        if !exc.is_none() {
            let _ = obj.setattr("_default_executor", py.None());
            // Non-blocking shutdown, like BaseEventLoop.close.
            let kw = PyDict::new(py);
            kw.set_item("wait", false)?;
            let _ = exc.call_method("shutdown", (), Some(&kw));
        }
        Ok(())
    }

    /// True while `run_forever`/`run_until_complete` is on the stack.
    fn is_running(&self) -> bool {
        self.state.running.load(Ordering::SeqCst)
    }

    fn is_closed(&self) -> bool {
        self.state.closed.load(Ordering::SeqCst)
    }

    // -- clock --

    /// Monotonic loop time in `time.monotonic()` units.
    fn time(&self) -> f64 {
        self.state.now()
    }

    // -- callbacks --

    #[pyo3(signature = (callback, *args, context=None))]
    fn call_soon(
        &self,
        py: Python,
        callback: Py<PyAny>,
        args: Bound<'_, PyTuple>,
        context: Option<Py<PyAny>>,
    ) -> PyResult<ReadyHandle> {
        let state = &self.state;
        check_closed_state(state)?;
        if state.debug.load(Ordering::SeqCst) {
            self.check_thread_inner(py)?;
            check_callback(py, callback.bind(py), "call_soon")?;
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        state.ready.push(ReadyEntry {
            callback,
            args: args.unbind(),
            context,
            cancelled: cancelled.clone(),
        });
        Ok(ReadyHandle { cancelled })
    }

    #[pyo3(signature = (callback, *args, context=None))]
    fn call_soon_threadsafe(
        &self,
        py: Python,
        callback: Py<PyAny>,
        args: Bound<'_, PyTuple>,
        context: Option<Py<PyAny>>,
    ) -> PyResult<ReadyHandle> {
        let state = &self.state;
        check_closed_state(state)?;
        if state.debug.load(Ordering::SeqCst) {
            check_callback(py, callback.bind(py), "call_soon_threadsafe")?;
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        state.ready.push(ReadyEntry {
            callback,
            args: args.unbind(),
            context,
            cancelled: cancelled.clone(),
        });
        // Wake a parked loop. Cheap when the loop is already awake
        // (stores a permit / no-ops).
        state.notify.notify_one();
        Ok(ReadyHandle { cancelled })
    }

    #[pyo3(signature = (delay, callback, *args, context=None))]
    fn call_later(
        &self,
        py: Python,
        delay: Bound<'_, PyAny>,
        callback: Py<PyAny>,
        args: Bound<'_, PyTuple>,
        context: Option<Py<PyAny>>,
    ) -> PyResult<TimerHandle> {
        if delay.is_none() {
            return Err(PyTypeError::new_err("delay must not be None"));
        }
        let delay: f64 = delay.extract()?;
        let when = self.state.now() + delay;
        self.call_at_impl(py, when, callback, args, context, "call_later")
    }

    #[pyo3(signature = (when, callback, *args, context=None))]
    fn call_at(
        &self,
        py: Python,
        when: Bound<'_, PyAny>,
        callback: Py<PyAny>,
        args: Bound<'_, PyTuple>,
        context: Option<Py<PyAny>>,
    ) -> PyResult<TimerHandle> {
        if when.is_none() {
            return Err(PyTypeError::new_err("when cannot be None"));
        }
        let when: f64 = when.extract()?;
        self.call_at_impl(py, when, callback, args, context, "call_at")
    }

    // -- tasks / futures (Python coroutine semantics stay in Python) --

    #[pyo3(signature = (coro, *, name=None, context=None, **kwargs))]
    fn create_task(
        slf: Bound<'_, Self>,
        coro: Py<PyAny>,
        name: Option<Py<PyAny>>,
        context: Option<Py<PyAny>>,
        kwargs: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        check_closed_state(&slf.borrow().state)?;
        let obj = slf.as_any();
        let factory: Option<Py<PyAny>> = obj.getattr("_task_factory")?.extract()?;
        if let Some(f) = factory {
            let kwargs = kwargs.unwrap_or_else(|| PyDict::new(py));
            if let Some(c) = context {
                kwargs.set_item("context", c)?;
            }
            let task = f.bind(py).call((obj, coro.bind(py)), Some(&kwargs))?;
            if let Some(n) = name {
                task.call_method1("set_name", (n,))?;
            }
            return Ok(task.unbind());
        }
        let state = slf.borrow().state.clone();
        let cls = state.task_cls.bind(py);
        let kwargs = kwargs.unwrap_or_else(|| PyDict::new(py));
        kwargs.set_item("loop", obj)?;
        // Explicit Nones are fine: Task treats them as defaults.
        kwargs.set_item("name", name)?;
        kwargs.set_item("context", context)?;
        Ok(cls.call((coro,), Some(&kwargs))?.unbind())
    }

    fn create_future(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let state = slf.borrow().state.clone();
        check_closed_state(&state)?;
        let cls = state.future_cls.bind(py);
        let kwargs = PyDict::new(py);
        kwargs.set_item("loop", slf.as_any())?;
        Ok(cls.call((), Some(&kwargs))?.unbind())
    }

    // -- readers / writers (Tokio-reactor backed) --
    //
    // These are the *internal* registrations (no transport check), used by
    // transports and `sock_*` helpers. The public `add_reader` etc. with
    // the `_ensure_fd_no_transport` check are thin wrappers in the Python
    // layer. Return `FdHandle` values supporting `cancel()`/`cancelled()`.

    #[pyo3(signature = (fd, callback, *args))]
    fn add_reader(
        &self,
        fd: Bound<'_, PyAny>,
        callback: Py<PyAny>,
        args: Bound<'_, PyTuple>,
    ) -> PyResult<FdHandle> {
        check_closed_state(&self.state)?;
        let fd = extract_fd(&fd)?;
        crate::fd::add_watcher(&self.state, fd, true, callback, args.unbind())
    }

    #[pyo3(signature = (fd, callback, *args))]
    fn add_writer(
        &self,
        fd: Bound<'_, PyAny>,
        callback: Py<PyAny>,
        args: Bound<'_, PyTuple>,
    ) -> PyResult<FdHandle> {
        check_closed_state(&self.state)?;
        let fd = extract_fd(&fd)?;
        crate::fd::add_watcher(&self.state, fd, false, callback, args.unbind())
    }

    fn remove_reader(&self, fd: Bound<'_, PyAny>) -> PyResult<bool> {
        if self.state.closed.load(Ordering::SeqCst) {
            return Ok(false);
        }
        let fd = extract_fd(&fd)?;
        Ok(crate::fd::detach_watcher(&self.state, fd, true))
    }

    fn remove_writer(&self, fd: Bound<'_, PyAny>) -> PyResult<bool> {
        if self.state.closed.load(Ordering::SeqCst) {
            return Ok(false);
        }
        let fd = extract_fd(&fd)?;
        Ok(crate::fd::detach_watcher(&self.state, fd, false))
    }

    /// Drain-mode reader for datagram transports: the watcher task performs
    /// `recvfrom` in Rust and the batch delivers `drain(data, addr)` per
    /// datagram (`drain_err(exc)` on socket errors). Same handle/pause/
    /// close semantics as `add_reader`.
    #[pyo3(signature = (fd, callback, err_callback))]
    fn _add_udp_reader(
        &self,
        fd: Bound<'_, PyAny>,
        callback: Py<PyAny>,
        err_callback: Py<PyAny>,
    ) -> PyResult<FdHandle> {
        check_closed_state(&self.state)?;
        let fd = extract_fd(&fd)?;
        crate::fd::add_drain_watcher(
            &self.state,
            fd,
            callback,
            err_callback,
            DrainKind::Udp,
        )
    }

    /// Drain-mode reader for stream transports: the watcher task performs
    /// `recv` in Rust (up to `max_size` per chunk) and the batch delivers
    /// `drain(data)` per chunk, `drain(b"")` on EOF, `drain_err(exc)` on
    /// socket errors. Same handle/pause/close semantics as `add_reader`.
    #[pyo3(signature = (fd, callback, err_callback, max_size))]
    fn _add_tcp_reader(
        &self,
        fd: Bound<'_, PyAny>,
        callback: Py<PyAny>,
        err_callback: Py<PyAny>,
        max_size: usize,
    ) -> PyResult<FdHandle> {
        check_closed_state(&self.state)?;
        let fd = extract_fd(&fd)?;
        let chunk = max_size.clamp(4096, 256 * 1024);
        crate::fd::add_drain_watcher(
            &self.state,
            fd,
            callback,
            err_callback,
            DrainKind::Tcp { chunk },
        )
    }

    // -- debug / exceptions / introspection --

    fn get_debug(&self) -> bool {
        self.state.debug.load(Ordering::SeqCst)
    }

    fn set_debug(&self, enabled: bool) {
        self.state.debug.store(enabled, Ordering::SeqCst);
    }

    fn set_exception_handler(&self, handler: Option<Py<PyAny>>) {
        *self.state.exc_handler.lock().unwrap() = handler;
    }

    fn get_exception_handler(&self, py: Python) -> Option<Py<PyAny>> {
        self.state
            .exc_handler
            .lock()
            .unwrap()
            .as_ref()
            .map(|h| h.clone_ref(py))
    }

    fn call_exception_handler(
        &self,
        py: Python,
        context: Bound<'_, PyDict>,
    ) -> PyResult<()> {
        dispatch_exception_handler(py, &self.state, &context)
    }

    fn default_exception_handler(
        &self,
        py: Python,
        context: Bound<'_, PyDict>,
    ) -> PyResult<()> {
        default_exception_handler_impl(py, &context)
    }

    fn _check_closed(&self) -> PyResult<()> {
        check_closed_state(&self.state)
    }

    fn _check_running(&self, py: Python) -> PyResult<()> {
        check_running_state(py, &self.state)
    }

    fn _check_thread(&self, py: Python) -> PyResult<()> {
        self.check_thread_inner(py)
    }

    /// Whether an fd direction currently has a live watcher.
    fn _is_polling(&self, fd: i32, write: bool) -> bool {
        crate::fd::is_polling(&self.state, fd, !write)
    }

    /// Counters + queue depths for benchmarks and the performance report.
    fn stats(&self) -> String {
        format!(
            "callbacks={} timers={} io={} batches={} parks={} ready={} timers_pending={}",
            self.state.n_callbacks.load(Ordering::Relaxed),
            self.state.n_timers.load(Ordering::Relaxed),
            self.state.n_io_events.load(Ordering::Relaxed),
            self.state.n_batches.load(Ordering::Relaxed),
            self.state.n_parks.load(Ordering::Relaxed),
            self.state.ready.len(),
            self.state.timers.lock().unwrap().len(),
        )
    }

    // -- asyncgen hooks (mirror BaseEventLoop) --

    fn _asyncgen_firstiter_hook(slf: Bound<'_, Self>, agen: Py<PyAny>) -> PyResult<()> {
        let py = slf.py();
        let obj = slf.as_any();
        let shutdown_called: bool = obj.getattr("_asyncgens_shutdown_called")?.extract()?;
        if shutdown_called {
            let warnings = py.import("warnings")?;
            let msg = format!(
                "asynchronous generator {:?} was scheduled after loop.shutdown_asyncgens() call",
                agen.bind(py).repr()?
            );
            let kwargs = PyDict::new(py);
            kwargs.set_item("source", obj)?;
            warnings.getattr("warn")?.call(
                (msg, py.import("builtins")?.getattr("ResourceWarning")?),
                Some(&kwargs),
            )?;
        }
        obj.getattr("_asyncgens")?.call_method1("add", (agen,))?;
        Ok(())
    }

    fn _asyncgen_finalizer_hook(slf: Bound<'_, Self>, agen: Py<PyAny>) -> PyResult<()> {
        let py = slf.py();
        let obj = slf.as_any();
        obj.getattr("_asyncgens")?
            .call_method1("discard", (agen.clone_ref(py),))?;
        let closed: bool = obj.call_method0("is_closed")?.extract()?;
        if !closed {
            let aclose = agen.bind(py).getattr("aclose")?.call0()?;
            let create_task = obj.getattr("create_task")?;
            obj.call_method("call_soon_threadsafe", (create_task, aclose), None)?;
        }
        Ok(())
    }
}

impl TokioopLoop {
    fn check_thread_inner(&self, py: Python) -> PyResult<()> {
        let tid = self.state.thread_id.load(Ordering::SeqCst);
        if tid < 0 {
            return Ok(());
        }
        let cur: i64 = py
            .import("threading")?
            .getattr("get_ident")?
            .call0()?
            .extract()?;
        if cur != tid {
            return Err(PyRuntimeError::new_err(
                "Non-thread-safe operation invoked on an event loop other than the current one",
            ));
        }
        Ok(())
    }

    fn call_at_impl(
        &self,
        py: Python,
        when: f64,
        callback: Py<PyAny>,
        args: Bound<'_, PyTuple>,
        context: Option<Py<PyAny>>,
        method: &str,
    ) -> PyResult<TimerHandle> {
        let state = &self.state;
        check_closed_state(state)?;
        if state.debug.load(Ordering::SeqCst) {
            self.check_thread_inner(py)?;
            check_callback(py, callback.bind(py), method)?;
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        let seq = state.seq.fetch_add(1, Ordering::Relaxed);
        state.timers.lock().unwrap().push(TimerEntry {
            when,
            seq,
            entry: ReadyEntry {
                callback,
                args: args.unbind(),
                context,
                cancelled: cancelled.clone(),
            },
        });
        Ok(TimerHandle { cancelled, when })
    }
}
