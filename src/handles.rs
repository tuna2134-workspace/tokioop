//! Python-visible handle objects (`call_soon` / timer / fd registrations).
//!
//! All handles are O(1) to cancel: cancellation only flips an `AtomicBool`
//! (and, for fd handles, detaches the watcher); queued entries are skipped
//! lazily at execution time. No queue scans, no heap removals.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use pyo3::prelude::*;

use crate::state::LoopState;

/// Return value of `call_later` / `call_at`.
#[pyclass(module = "tokioop._tokioop")]
pub struct TimerHandle {
    pub(crate) cancelled: Arc<AtomicBool>,
    pub(crate) when: f64,
}

#[pymethods]
impl TimerHandle {
    /// Cancel the timer. Returns `True` unless it was already cancelled,
    /// matching `asyncio.TimerHandle.cancel`.
    fn cancel(&self) -> bool {
        !self.cancelled.swap(true, Ordering::SeqCst)
    }

    fn cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Absolute deadline in `loop.time()` units.
    fn when(&self) -> f64 {
        self.when
    }

    fn __repr__(&self) -> String {
        format!(
            "<TimerHandle when={} cancelled={}>",
            self.when,
            self.cancelled()
        )
    }
}

/// Return value of `call_soon` / `call_soon_threadsafe`.
#[pyclass(module = "tokioop._tokioop")]
pub struct ReadyHandle {
    pub(crate) cancelled: Arc<AtomicBool>,
}

#[pymethods]
impl ReadyHandle {
    /// Cancel the callback. Returns `True` unless already cancelled,
    /// matching `asyncio.Handle.cancel`.
    fn cancel(&self) -> bool {
        !self.cancelled.swap(true, Ordering::SeqCst)
    }

    fn cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    fn __repr__(&self) -> String {
        format!("<ReadyHandle cancelled={}>", self.cancelled())
    }
}

/// Return value of the internal `_add_reader` / `_add_writer` used by
/// transports and `sock_*` helpers.
///
/// `cancel()` detaches the watcher (mirroring CPython, where a cancelled
/// reader `Handle` is dropped from the selector on next pass); the fd stays
/// registered only if a live watcher remains.
#[pyclass(module = "tokioop._tokioop")]
pub struct FdHandle {
    pub(crate) state: Arc<LoopState>,
    pub(crate) fd: i32,
    pub(crate) is_read: bool,
    pub(crate) flag: Arc<AtomicBool>,
}

#[pymethods]
impl FdHandle {
    fn cancel(&self) -> bool {
        if self.flag.swap(true, Ordering::SeqCst) {
            return false; // already cancelled
        }
        crate::fd::detach_watcher(&self.state, self.fd, self.is_read);
        true
    }

    fn cancelled(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    fn __repr__(&self) -> String {
        format!(
            "<FdHandle fd={} {} cancelled={}>",
            self.fd,
            if self.is_read { "read" } else { "write" },
            self.cancelled()
        )
    }
}
