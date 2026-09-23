//! fd reactor: Tokio-backed `add_reader` / `add_writer` on unix, explicit
//! stub elsewhere.
//!
//! The reactor core is platform-specific: unix uses `AsyncFd` watcher tasks
//! (`fd_unix.rs`); other platforms (currently Windows) compile a stub whose
//! registration entry points raise `NotImplementedError`, matching how
//! asyncio surfaces unsupported transports. Everything else — watcher
//! bookkeeping, removal/close semantics, polling queries — is shared here
//! so the Python API surface is identical on all platforms.

use std::sync::atomic::Ordering;

use crate::state::LoopState;

/// Drain flavor for [`add_drain_watcher`](imp::add_drain_watcher).
#[derive(Clone, Copy)]
pub enum DrainKind {
    Udp,
    Tcp { chunk: usize },
}

#[cfg(unix)]
#[path = "fd_unix.rs"]
mod imp;

#[cfg(not(unix))]
#[path = "fd_stub.rs"]
mod imp;

pub use imp::{add_drain_watcher, add_watcher};

/// Detach one direction of an fd watcher. Returns `true` if a watcher was
/// registered (CPython `remove_reader`/`remove_writer` contract).
pub fn detach_watcher(state: &LoopState, fd: i32, is_read: bool) -> bool {
    let mut fds = state.fds.lock().unwrap();
    let Some(record) = fds.get_mut(&fd) else {
        return false;
    };
    let dir = if is_read {
        &mut record.reader
    } else {
        &mut record.writer
    };
    let Some(old) = dir.take() else {
        return false;
    };
    old.abort.abort();
    if let Ok(mut s) = old.slot.lock() {
        if let Some(f) = s.flag.take() {
            f.store(true, Ordering::SeqCst);
        }
        s.cb = None;
        s.dead = true;
    }
    state.n_watchers.fetch_sub(1, Ordering::Relaxed);
    if record.reader.is_none() && record.writer.is_none() {
        fds.remove(&fd);
    }
    true
}

/// Abort every watcher (used by `close()`).
pub fn abort_all(state: &LoopState) {
    let mut fds = state.fds.lock().unwrap();
    for (_, record) in fds.iter() {
        for dir in [&record.reader, &record.writer].into_iter().flatten() {
            dir.abort.abort();
            if let Ok(mut s) = dir.slot.lock() {
                if let Some(f) = s.flag.take() {
                    f.store(true, Ordering::SeqCst);
                }
                s.cb = None;
                s.dead = true;
            }
        }
    }
    fds.clear();
    state.n_watchers.store(0, Ordering::SeqCst);
}

/// Whether a live watcher is currently registered (for `__repr__` and tests).
pub fn is_polling(state: &LoopState, fd: i32, is_read: bool) -> bool {
    let fds = state.fds.lock().unwrap();
    let Some(record) = fds.get(&fd) else {
        return false;
    };
    let dir = if is_read {
        &record.reader
    } else {
        &record.writer
    };
    match dir {
        Some(d) => d
            .slot
            .lock()
            .map(|s| !s.dead && (s.cb.is_some() || s.drain.is_some()))
            .unwrap_or(false),
        None => false,
    }
}
