//! `tokioop._tokioop`: Tokio-backed `asyncio` event-loop core.
//!
//! The performance-critical scheduling machinery (ready queue, timer heap,
//! I/O reactor wakeups, batched GIL callback execution) lives in Rust on
//! top of a dedicated per-loop Tokio current-thread runtime. Python-facing
//! compatibility (transports, `sock_*` coroutines, executors, policies)
//! lives in the `tokioop` Python package, which subclasses the loop type
//! defined here.

mod event_loop;
mod fd;
mod handles;
mod state;

use pyo3::prelude::*;

use event_loop::TokioopLoop;
use handles::{FdHandle, ReadyHandle, TimerHandle};

/// Native module: scheduling core for [`crate`] `RustEventLoop`.
#[pymodule]
mod _tokioop {
    #[pymodule_export]
    use super::TokioopLoop;
    #[pymodule_export]
    use super::TimerHandle;
    #[pymodule_export]
    use super::ReadyHandle;
    #[pymodule_export]
    use super::FdHandle;
}
