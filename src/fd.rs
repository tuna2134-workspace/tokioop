//! Tokio-reactor-backed `add_reader` / `add_writer`.
//!
//! Design (Linux-first):
//!
//! - Each registered direction (read or write) of an fd gets a `dup`'d
//!   file descriptor owned by Rust, registered with the loop's dedicated
//!   Tokio current-thread runtime via [`tokio::io::unix::AsyncFd`]. The
//!   original fd number stays the registry key, so the Python-level API
//!   (`add_reader(fd, ...)`, transports, `sock_*`) is unchanged.
//! - A lightweight Tokio task per direction waits for readiness edges.
//!   Tokio readiness is edge-triggered while `asyncio` is level-triggered;
//!   the task emulates level triggering with a single `poll(2)` probe
//!   after every callback (still readable/writable → re-fire immediately,
//!   otherwise clear the flag and park). Exactly one poll syscall per fire
//!   in steady state; a priming poll only on fresh registration covers
//!   re-registration with already-pending I/O (`pause_reading` →
//!   `resume_reading` fires immediately instead of stalling).
//! - The Python callback always runs in the loop's batched GIL section
//!   (see [`crate::event_loop::run_batch`]); a shared per-watcher
//!   [`Notify`][tokio::sync::Notify] rendezvous (no per-fire allocation)
//!   tells the task when the callback finished. The main loop never blocks
//!   on watchers.

use std::os::fd::{FromRawFd, OwnedFd, RawFd};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use pyo3::prelude::*;
use pyo3::types::PyTuple;
use tokio::io::unix::{AsyncFd, AsyncFdReadyGuard};
use tokio::sync::Notify;

use crate::handles::FdHandle;
use crate::state::{
    AddrRepr, FdDirection, FdRecord, FdSlotState, IoCompletion, IoData, LoopState,
};

// ---------------------------------------------------------------------------
// readiness probing
// ---------------------------------------------------------------------------

/// Zero-timeout level-triggered readiness test on our owned `dup`'d fd.
///
/// `poll(2)` with timeout 0 reports the *current* readiness state for both
/// sockets and pipes, which is exactly the level-triggered semantic
/// `asyncio` expects. One syscall per I/O event is negligible next to the
/// cost of entering the Python callback.
fn poll_ready(fd: RawFd, is_read: bool) -> bool {
    let events = if is_read { libc::POLLIN } else { libc::POLLOUT };
    let mut pfd = libc::pollfd {
        fd,
        events,
        revents: 0,
    };
    // SAFETY: `pfd` is a valid stack-allocated `pollfd`; `fd` is an owned
    // duplicate held alive by the watcher task for the whole call.
    loop {
        let r = unsafe { libc::poll(&mut pfd as *mut libc::pollfd, 1, 0) };
        if r < 0 {
            if std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            // Fail open: let the Python callback observe (and surface)
            // the real error instead of stalling the watcher.
            return true;
        }
        break;
    }
    let re = pfd.revents;
    if is_read {
        (re & (libc::POLLIN | libc::POLLHUP | libc::POLLERR | libc::POLLNVAL)) != 0
    } else {
        (re & (libc::POLLOUT | libc::POLLERR | libc::POLLHUP | libc::POLLNVAL)) != 0
    }
}

// ---------------------------------------------------------------------------
// watcher task
// ---------------------------------------------------------------------------

async fn fd_task(
    state: Arc<LoopState>,
    afd: Arc<AsyncFd<OwnedFd>>,
    dup_fd: RawFd,
    slot: Arc<Mutex<FdSlotState>>,
    hs: Arc<Notify>,
    is_read: bool,
) {
    // Currently held readiness guard, if the Tokio flag is (maybe) set.
    //
    // Exactly one poll(2) per fire in steady state (after the callback).
    // Parking (`await`) needs no probe: we park exclusively after a
    // confirmed drain + explicit flag clear, or on a fresh registration
    // whose flag starts clear — so any new arrival generates a new edge
    // and wakes us. A stale flag yields at most one bounded spurious
    // wakeup (callback observes EAGAIN), never a spin, never a stall.
    let mut guard: Option<AsyncFdReadyGuard<'_, OwnedFd>> = None;
    // Priming poll, once per registration: fresh watchers may cover
    // already-pending I/O whose edge predates us. Without it we would
    // park forever on a dead edge.
    let mut primed = poll_ready(dup_fd, is_read);

    loop {
        if !primed {
            let g = if is_read {
                afd.readable().await
            } else {
                afd.writable().await
            };
            match g {
                Ok(g) => guard = Some(g),
                Err(_) => break, // closed / reactor gone
            }
        }
        primed = false;

        {
            let has_cb = slot.lock().unwrap().cb.is_some();
            if !has_cb {
                return; // removed or cancelled
            }
        }
        state.io.push(IoCompletion::Callback {
            slot: slot.clone(),
            hs: hs.clone(),
        });
        state
            .n_io_events
            .fetch_add(1, Ordering::Relaxed);
        // Wake a parked loop so the callback runs promptly.
        state.notify.notify_one();

        // Wait until the loop batch executed the callback (shared per-
        // watcher Notify rendezvous; a stored permit covers batch-before-
        // wait). The loop never waits for us, so this cannot deadlock.
        hs.notified().await;
        {
            let has_cb = slot.lock().unwrap().cb.is_some();
            if !has_cb {
                return;
            }
        }

        if poll_ready(dup_fd, is_read) {
            // Level-triggered: I/O still pending, re-fire without parking.
            // The flag stays asserted for a later genuine wait.
            if let Some(g) = guard.as_mut() {
                g.retain_ready();
            }
            primed = true;
            continue;
        }
        // Drained: clear a possibly-set flag so the next park is genuine.
        // `clear_ready` only clears pre-guard edges; an edge landing during
        // the callback is preserved and wakes the next wait (at most one
        // bounded spurious wakeup if its data was already consumed).
        if let Some(g) = guard.as_mut() {
            g.clear_ready();
            guard = None;
        } else {
            // Fired from the primed path without ever holding a guard: the
            // priming edge's flag may still be set (it would cause exactly
            // one spurious wakeup below). Clear it without parking via
            // `try_io`. This is airtight on a single-threaded runtime: no
            // await runs between the drain-observing poll above and here,
            // so the driver cannot change the flag in between — `try_io`
            // clears exactly the drained-observed state.
            use tokio::io::Interest;
            let interest = if is_read {
                Interest::READABLE
            } else {
                Interest::WRITABLE
            };
            let _ = afd.try_io(interest, |_| {
                Err::<(), std::io::Error>(std::io::ErrorKind::WouldBlock.into())
            });
        }
    }
}

// ---------------------------------------------------------------------------
// registration
// ---------------------------------------------------------------------------

/// Register (or replace) a read/write watcher. Mirrors CPython's
/// `_add_reader`/`_add_writer`: replacing cancels the previous handle.
pub fn add_watcher(
    state: &Arc<LoopState>,
    fd: i32,
    is_read: bool,
    callback: Py<PyAny>,
    args: Py<PyTuple>,
) -> PyResult<FdHandle> {
    if fd < 0 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "file descriptor cannot be negative",
        ));
    }
    // Own a duplicate so our registration lifetime is independent of the
    // user's socket object, and our `poll(2)` probes race-free.
    // SAFETY: on success `libc::dup` returns a new, owned file descriptor.
    let newfd = unsafe { libc::dup(fd) };
    if newfd < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: `newfd` is a fresh fd from `dup` checked for errors above;
    // `OwnedFd` takes over its lifetime (closed on drop).
    let owned = unsafe { OwnedFd::from_raw_fd(newfd) };

    // `AsyncFd::new` requires an entered runtime context; `add_reader`
    // is called from arbitrary Python code, so enter explicitly.
    let afd = {
        let _guard = state.rt.enter();
        AsyncFd::new(owned).map_err(|e| PyErr::from(e))?
    };
    let afd = Arc::new(afd);

    let flag = Arc::new(AtomicBool::new(false));
    let slot = Arc::new(Mutex::new(FdSlotState {
        cb: Some((callback, args)),
        drain: None,
        drain_err: None,
        dead: false,
        flag: Some(flag.clone()),
    }));

    // Shared rendezvous for every firing of this watcher (one allocation
    // per registration, not per event).
    let hs = Arc::new(Notify::new());

    let task = fd_task(state.clone(), afd, newfd, slot.clone(), hs, is_read);
    let abort = state.rt.spawn(task).abort_handle();

    {
        let mut fds = state.fds.lock().unwrap();
        let record = fds.entry(fd).or_insert_with(|| FdRecord {
            reader: None,
            writer: None,
        });
        let dir = if is_read {
            &mut record.reader
        } else {
            &mut record.writer
        };
        if let Some(old) = dir.take() {
            old.abort.abort();
            if let Ok(mut s) = old.slot.lock() {
                if let Some(f) = s.flag.take() {
                    // Mirror CPython cancelling the replaced Handle.
                    f.store(true, Ordering::SeqCst);
                }
                s.cb = None;
                s.dead = true;
            }
            // Replacing: one watcher out, one in — counter unchanged.
        } else {
            state.n_watchers.fetch_add(1, Ordering::Relaxed);
        }
        *dir = Some(FdDirection {
            slot: slot.clone(),
            abort,
        });
    }

    Ok(FdHandle {
        state: state.clone(),
        fd,
        is_read,
        flag,
    })
}

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

// ---------------------------------------------------------------------------
// drain mode: the watcher task performs socket reads itself
// ---------------------------------------------------------------------------

/// Quantum bounds per drain iteration: small enough that batches deliver
/// early (pipelined latency) and other fds stay fair under flood, large
/// enough that yield overhead (~0.5µs) is noise next to the reads.
const UDP_QUANTUM: usize = 16;
const TCP_QUANTUM_ITEMS: usize = 4;
const TCP_QUANTUM_BYTES: usize = 256 * 1024;
/// Biggest possible UDP payload; the task buffer is allocated once.
const UDP_BUF: usize = 65536 + 64;

/// Drain flavor for [`add_drain_watcher`].
#[derive(Clone, Copy)]
pub enum DrainKind {
    Udp,
    Tcp { chunk: usize },
}

fn is_wouldblock(e: &std::io::Error) -> bool {
    e.kind() == std::io::ErrorKind::WouldBlock
}

/// Non-blocking stream read. `MSG_DONTWAIT` (not the socket flags) is used
/// deliberately: a blocking read here would freeze the whole event loop,
/// and this way registration never mutates user-visible socket flags.
fn tcp_recv(dup_fd: RawFd, buf: &mut [u8]) -> std::io::Result<usize> {
    loop {
        // SAFETY: `buf` is a valid writable slice for its whole length;
        // `dup_fd` is our owned duplicate, alive for the task's lifetime;
        // `MSG_DONTWAIT` guarantees the call never blocks.
        let r = unsafe {
            libc::recv(
                dup_fd,
                buf.as_mut_ptr() as *mut libc::c_void,
                buf.len(),
                libc::MSG_DONTWAIT,
            )
        };
        if r < 0 {
            let e = std::io::Error::last_os_error();
            if e.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
        return Ok(r as usize);
    }
}

/// Non-blocking datagram read with source address.
fn udp_recvfrom(dup_fd: RawFd, buf: &mut [u8]) -> std::io::Result<(usize, AddrRepr)> {
    use std::mem::size_of;
    loop {
        let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        let mut len = size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        // SAFETY: as for `tcp_recv`; `storage` fits any sockaddr and `len`
        // is initialized in/out.
        let r = unsafe {
            libc::recvfrom(
                dup_fd,
                buf.as_mut_ptr() as *mut libc::c_void,
                buf.len(),
                libc::MSG_DONTWAIT,
                &mut storage as *mut _ as *mut libc::sockaddr,
                &mut len,
            )
        };
        if r < 0 {
            let e = std::io::Error::last_os_error();
            if e.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
        let addr = parse_addr(&storage, len as usize);
        return Ok((r as usize, addr));
    }
}

fn parse_addr(storage: &libc::sockaddr_storage, len: usize) -> AddrRepr {
    match storage.ss_family as libc::c_int {
        libc::AF_INET => {
            // SAFETY: family checked; `sockaddr_in` fits in the storage.
            let a =
                unsafe { &*(storage as *const _ as *const libc::sockaddr_in) };
            let ip = std::net::Ipv4Addr::from(u32::from_be(a.sin_addr.s_addr));
            let port = u16::from_be(a.sin_port);
            AddrRepr::Inet(std::net::SocketAddr::new(ip.into(), port))
        }
        libc::AF_INET6 => {
            // SAFETY: family checked; `sockaddr_in6` fits in the storage.
            let a =
                unsafe { &*(storage as *const _ as *const libc::sockaddr_in6) };
            let ip = std::net::Ipv6Addr::from(a.sin6_addr.s6_addr);
            let port = u16::from_be(a.sin6_port);
            let v6 = std::net::SocketAddrV6::new(
                ip,
                port,
                u32::from_be(a.sin6_flowinfo),
                a.sin6_scope_id,
            );
            AddrRepr::Inet(std::net::SocketAddr::V6(v6))
        }
        libc::AF_UNIX => {
            // SAFETY: family checked; reading `sun_path` within `len`.
            let sun =
                unsafe { &*(storage as *const _ as *const libc::sockaddr_un) };
            let off = std::mem::offset_of!(libc::sockaddr_un, sun_path);
            let raw = unsafe {
                std::slice::from_raw_parts(
                    sun.sun_path.as_ptr() as *const u8,
                    len.saturating_sub(off).min(sun.sun_path.len()),
                )
            };
            if raw.is_empty() {
                AddrRepr::Unnamed
            } else if raw[0] == 0 {
                // Abstract socket: address is the raw bytes incl. NUL.
                AddrRepr::Unix(raw.to_vec())
            } else {
                let end = raw.iter().position(|&b| b == 0).unwrap_or(raw.len());
                if end == 0 {
                    AddrRepr::Unnamed
                } else {
                    AddrRepr::Unix(raw[..end].to_vec())
                }
            }
        }
        _ => AddrRepr::Unnamed,
    }
}

// ---------------------------------------------------------------------------
// drain-mode watcher task
// ---------------------------------------------------------------------------

/// Drain-mode watcher: performs the socket reads in Rust and hands payloads
/// to the transport. No rendezvous, no `poll(2)`: reads run until `EAGAIN`
/// (precise drain observation), which doubles as the level-triggered probe.
/// Parking is stall-free by construction — the flag is cleared only after
/// an observed drain, so any new arrival generates a new edge.
///
/// Reads run in small quanta with a yield between quanta whenever more
/// work may follow: batches deliver early (pipelined latency instead of
/// one long synchronous burst blocking the loop) and other fds stay fair
/// under flood.
async fn drain_task(
    state: Arc<LoopState>,
    afd: Arc<AsyncFd<OwnedFd>>,
    dup_fd: RawFd,
    slot: Arc<Mutex<FdSlotState>>,
    kind: DrainKind,
) {
    let mut buf = vec![
        0u8;
        match kind {
            DrainKind::Udp => UDP_BUF,
            DrainKind::Tcp { chunk } => chunk,
        }
    ];

    // Priming quanta: serve already-pending I/O whose edge predates our
    // AsyncFd registration (e.g. data sent before accept finished
    // registering the server transport). Awaiting first would park forever
    // on that dead edge. Afterwards the flag is untouched-but-clear (fresh
    // registration), so the first real wait below parks genuinely.
    if !slot.lock().unwrap().dead {
        drain_loop(&state, dup_fd, &slot, &mut buf, kind, None).await;
        if slot.lock().unwrap().dead {
            return;
        }
    }

    loop {
        let guard = match afd.readable().await {
            Ok(g) => g,
            Err(_) => break, // closed / reactor gone
        };
        {
            let alive = !slot.lock().unwrap().dead;
            if !alive {
                return; // removed or cancelled while parked
            }
        }

        drain_loop(&state, dup_fd, &slot, &mut buf, kind, Some(guard)).await;
        if slot.lock().unwrap().dead {
            return;
        }
    }
}

/// Read quanta until `EAGAIN`/error/EOF; yields between quanta when more
/// work may follow so batches deliver incrementally. `guard` (if any) is
/// cleared on observed drain, retained otherwise; `None` (priming) leaves
/// the fresh flag untouched.
async fn drain_loop(
    state: &Arc<LoopState>,
    dup_fd: RawFd,
    slot: &Arc<Mutex<FdSlotState>>,
    buf: &mut [u8],
    kind: DrainKind,
    mut guard: Option<AsyncFdReadyGuard<'_, OwnedFd>>,
) {
    loop {
        let mut pushed = false;
        let mut drained = false;
        let mut eof = false;
        let mut n = 0usize;
        let mut bytes = 0usize;
        match kind {
            DrainKind::Udp => {
                while n < UDP_QUANTUM {
                    match udp_recvfrom(dup_fd, buf) {
                        Ok((len, addr)) => {
                            state.n_read_bytes.fetch_add(len as u64, Ordering::Relaxed);
                            state.io.push(IoCompletion::Data {
                                slot: slot.clone(),
                                data: IoData::UdpDatagram {
                                    payload: buf[..len].to_vec(),
                                    addr,
                                },
                            });
                            pushed = true;
                            n += 1;
                        }
                        Err(e) if is_wouldblock(&e) => {
                            drained = true;
                            break;
                        }
                        Err(e) => {
                            state.io.push(IoCompletion::Data {
                                slot: slot.clone(),
                                data: IoData::ReadError { err: e },
                            });
                            pushed = true;
                            drained = true;
                            break;
                        }
                    }
                }
            }
            DrainKind::Tcp { .. } => {
                while n < TCP_QUANTUM_ITEMS && bytes < TCP_QUANTUM_BYTES {
                    match tcp_recv(dup_fd, buf) {
                        Ok(0) => {
                            state.io.push(IoCompletion::Data {
                                slot: slot.clone(),
                                data: IoData::TcpEof,
                            });
                            pushed = true;
                            eof = true;
                            drained = true;
                            break;
                        }
                        Ok(len) => {
                            bytes += len;
                            state.n_read_bytes.fetch_add(len as u64, Ordering::Relaxed);
                            state.io.push(IoCompletion::Data {
                                slot: slot.clone(),
                                data: IoData::TcpChunk {
                                    payload: buf[..len].to_vec(),
                                },
                            });
                            pushed = true;
                            n += 1;
                        }
                        Err(e) if is_wouldblock(&e) => {
                            drained = true;
                            break;
                        }
                        Err(e) => {
                            state.io.push(IoCompletion::Data {
                                slot: slot.clone(),
                                data: IoData::ReadError { err: e },
                            });
                            pushed = true;
                            drained = true;
                            break;
                        }
                    }
                }
            }
        }

        if pushed {
            state
                .n_io_events
                .fetch_add(1, Ordering::Relaxed);
            state.notify.notify_one();
        }
        if eof {
            // The transport removes the watcher synchronously in the next
            // batch (close/remove on EOF); yield so it runs, then exit if
            // gone. Without the yield a persistently-EOF-readable fd would
            // spin without ever letting the batch process the removal.
            tokio::task::yield_now().await;
            if slot.lock().unwrap().dead {
                return;
            }
            // Still registered (exotic): re-read (recv yields 0 again);
            // each cycle yields, so the batch always gets to run.
            continue;
        }
        if drained {
            if let Some(g) = guard.as_mut() {
                g.clear_ready();
            }
            return;
        }
        // Quantum full with data possibly remaining: yield so the batch
        // delivers what we pushed (pipelined latency + fairness under
        // flood), then keep draining without parking.
        tokio::task::yield_now().await;
        if slot.lock().unwrap().dead {
            return;
        }
    }
}

/// Register a drain-mode reader: the watcher task reads the socket and the
/// batch delivers payloads to `drain`/`drain_err`. Replaces any existing
/// reader like [`add_watcher`] does.
pub fn add_drain_watcher(
    state: &Arc<LoopState>,
    fd: i32,
    drain: Py<PyAny>,
    drain_err: Py<PyAny>,
    kind: DrainKind,
) -> PyResult<FdHandle> {
    if fd < 0 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "file descriptor cannot be negative",
        ));
    }
    // SAFETY: on success `libc::dup` returns a new, owned file descriptor.
    let newfd = unsafe { libc::dup(fd) };
    if newfd < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: `newfd` is a fresh fd from `dup` checked for errors above.
    let owned = unsafe { OwnedFd::from_raw_fd(newfd) };

    // `AsyncFd::new` requires an entered runtime context.
    let afd = {
        let _guard = state.rt.enter();
        AsyncFd::new(owned).map_err(PyErr::from)?
    };
    let afd = Arc::new(afd);

    let flag = Arc::new(AtomicBool::new(false));
    let slot = Arc::new(Mutex::new(FdSlotState {
        cb: None,
        drain: Some(drain),
        drain_err: Some(drain_err),
        dead: false,
        flag: Some(flag.clone()),
    }));

    let task = drain_task(state.clone(), afd, newfd, slot.clone(), kind);
    let abort = state.rt.spawn(task).abort_handle();

    {
        let mut fds = state.fds.lock().unwrap();
        let record = fds.entry(fd).or_insert_with(|| FdRecord {
            reader: None,
            writer: None,
        });
        if let Some(old) = record.reader.take() {
            old.abort.abort();
            if let Ok(mut s) = old.slot.lock() {
                if let Some(f) = s.flag.take() {
                    f.store(true, Ordering::SeqCst);
                }
                s.cb = None;
                s.dead = true;
            }
        } else {
            state.n_watchers.fetch_add(1, Ordering::Relaxed);
        }
        record.reader = Some(FdDirection {
            slot: slot.clone(),
            abort,
        });
    }

    Ok(FdHandle {
        state: state.clone(),
        fd,
        is_read: true,
        flag,
    })
}
