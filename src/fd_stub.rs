//! Stub fd reactor for platforms without a Tokio `AsyncFd` reactor
//! (currently Windows).
//!
//! Scheduling, timers, tasks and the full non-I/O loop work everywhere;
//! only fd-backed I/O (`add_reader`/`add_writer`, transports, `sock_*`)
//! is unavailable here. Registration entry points raise
//! `NotImplementedError` — the same failure mode asyncio itself uses for
//! unsupported transports — instead of silently misbehaving. A Windows
//! reactor (IOCP-backed) is future work; Linux stays the first-class
//! target.

use std::sync::Arc;

use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use crate::handles::FdHandle;
use crate::state::LoopState;
use super::DrainKind;

fn unsupported(op: &str) -> PyErr {
    PyNotImplementedError::new_err(format!(
        "tokioop fd I/O ({op}) is not supported on this platform yet; \
         scheduling, timers and tasks work normally"
    ))
}

/// Stub registration: always fails with `NotImplementedError`.
pub fn add_watcher(
    state: &Arc<LoopState>,
    fd: i32,
    _is_read: bool,
    _callback: Py<PyAny>,
    _args: Py<PyTuple>,
) -> PyResult<FdHandle> {
    let _ = (state, fd);
    Err(unsupported("add_reader/add_writer"))
}

/// Stub drain registration: always fails with `NotImplementedError`.
pub fn add_drain_watcher(
    state: &Arc<LoopState>,
    fd: i32,
    _drain: Py<PyAny>,
    _drain_err: Py<PyAny>,
    _kind: DrainKind,
) -> PyResult<FdHandle> {
    let _ = (state, fd);
    Err(unsupported("transports"))
}
