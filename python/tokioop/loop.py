"""``RustEventLoop``: the user-facing asyncio event loop.

The hot path (ready queue, timer heap, I/O reactor wakeups, batched GIL
callback execution, lifecycle) is implemented in Rust
(``tokioop._tokioop.TokioopLoop``). This subclass adds the broader
``AbstractEventLoop`` surface — transports, ``sock_*`` coroutines,
executors, signals, subprocess stubs, ``asyncio.run`` shutdown helpers —
ported from CPython 3.13's ``base_events``/``selector_events`` so semantics
match the stdlib exactly.
"""

import collections.abc
import concurrent.futures
import errno
import functools
import itertools
import os
import signal
import socket
import ssl
import stat
import sys
import threading
import warnings

from asyncio import base_events, constants, coroutines, events, exceptions, futures, staggered, tasks, timeouts
from asyncio import sslproto
from asyncio import trsock
from asyncio.log import logger

from tokioop import _tokioop
from tokioop.transports import (
    _SelectorDatagramTransport,
    _SelectorSocketTransport,
)

__all__ = ("RustEventLoop",)


class RustEventLoop(_tokioop.TokioopLoop, events.AbstractEventLoop):
    """Asyncio event loop with a Rust/Tokio scheduling core.

    Inherits the ``AbstractEventLoop`` interface so ``isinstance`` checks
    pass and any genuinely unimplemented method fails with the stdlib's
    own ``NotImplementedError`` stubs. All hot-path methods resolve to
    the Rust implementation (first in the MRO).
    """

    def __init__(self):
        super().__init__()
        self._signal_handlers = {}
        self._unix_server_sockets = {}

    # -- introspection helpers ------------------------------------------------

    @property
    def _debug(self):
        return self.get_debug()

    @_debug.setter
    def _debug(self, value):
        self.set_debug(bool(value))

    def _ensure_fd_no_transport(self, fd):
        fileno = fd
        if not isinstance(fileno, int):
            try:
                fileno = int(fileno.fileno())
            except (AttributeError, TypeError, ValueError):
                # This code matches selectors._fileobj_to_fd function.
                raise ValueError(f"Invalid file object: {fd!r}") from None
        transport = self._transports.get(fileno)
        if transport is not None and not transport.is_closing():
            raise RuntimeError(
                f"File descriptor {fd!r} is used by transport {transport!r}"
            )

    # Internal registrations (no transport check): bound directly to the
    # Rust reactor methods.
    _add_reader = _tokioop.TokioopLoop.add_reader
    _add_writer = _tokioop.TokioopLoop.add_writer
    _remove_reader = _tokioop.TokioopLoop.remove_reader
    _remove_writer = _tokioop.TokioopLoop.remove_writer
    _add_udp_reader = _tokioop.TokioopLoop._add_udp_reader
    _add_tcp_reader = _tokioop.TokioopLoop._add_tcp_reader

    def add_reader(self, fd, callback, *args):
        """Add a reader callback."""
        self._ensure_fd_no_transport(fd)
        self._add_reader(fd, callback, *args)

    def remove_reader(self, fd):
        """Remove a reader callback."""
        self._ensure_fd_no_transport(fd)
        return self._remove_reader(fd)

    def add_writer(self, fd, callback, *args):
        """Add a writer callback."""
        self._ensure_fd_no_transport(fd)
        self._add_writer(fd, callback, *args)

    def remove_writer(self, fd):
        """Remove a writer callback."""
        self._ensure_fd_no_transport(fd)
        return self._remove_writer(fd)

    # -- task factory / callbacks ---------------------------------------------

    def set_task_factory(self, factory):
        if factory is not None and not callable(factory):
            raise TypeError("task factory must be a callable or None")
        self._task_factory = factory

    def get_task_factory(self):
        return self._task_factory

    def _check_callback(self, callback, method):
        if coroutines.iscoroutine(callback) or coroutines.iscoroutinefunction(callback):
            raise TypeError(f"coroutines cannot be used with {method}()")
        if not callable(callback):
            raise TypeError(
                f"a callable object was expected by {method}(), got {callback!r}"
            )

    # -- executors -------------------------------------------------------------

    def _check_default_executor(self):
        if self._executor_shutdown_called:
            raise RuntimeError("Executor shutdown has been called")

    def run_in_executor(self, executor, func, *args):
        self._check_closed()
        if self.get_debug():
            self._check_callback(func, "run_in_executor")
        if executor is None:
            executor = self._default_executor
            # Only check when the default executor is being used
            self._check_default_executor()
            if executor is None:
                executor = concurrent.futures.ThreadPoolExecutor(
                    thread_name_prefix="asyncio"
                )
                self._default_executor = executor
        return futures.wrap_future(executor.submit(func, *args), loop=self)

    def set_default_executor(self, executor):
        if not isinstance(executor, concurrent.futures.ThreadPoolExecutor):
            raise TypeError("executor must be ThreadPoolExecutor instance")
        self._default_executor = executor

    async def shutdown_asyncgens(self):
        """Shutdown all active asynchronous generators."""
        self._asyncgens_shutdown_called = True

        if not len(self._asyncgens):
            return

        closing_agens = list(self._asyncgens)
        self._asyncgens.clear()

        results = await tasks.gather(
            *[ag.aclose() for ag in closing_agens], return_exceptions=True
        )

        for result, agen in zip(results, closing_agens):
            if isinstance(result, Exception):
                self.call_exception_handler(
                    {
                        "message": "an error occurred during closing of "
                        f"asynchronous generator {agen!r}",
                        "exception": result,
                        "asyncgen": agen,
                    }
                )

    async def shutdown_default_executor(self, timeout=None):
        """Schedule the shutdown of the default executor."""
        self._executor_shutdown_called = True
        if self._default_executor is None:
            return
        future = self.create_future()
        thread = threading.Thread(target=self._do_shutdown, args=(future,))
        thread.start()
        try:
            async with timeouts.timeout(timeout):
                await future
        except TimeoutError:
            warnings.warn(
                "The executor did not finishing joining "
                f"its threads within {timeout} seconds.",
                RuntimeWarning,
                stacklevel=2,
            )
            self._default_executor.shutdown(wait=False)
        else:
            thread.join()

    def _do_shutdown(self, future):
        try:
            self._default_executor.shutdown(wait=True)
            if not self.is_closed():
                self.call_soon_threadsafe(
                    futures._set_result_unless_cancelled, future, None
                )
        except Exception as ex:
            if not self.is_closed() and not future.cancelled():
                self.call_soon_threadsafe(future.set_exception, ex)

    # -- name resolution --------------------------------------------------------

    def _getaddrinfo_debug(self, host, port, family, type, proto, flags):
        msg = [f"{host}:{port!r}"]
        if family:
            msg.append(f"family={family!r}")
        if type:
            msg.append(f"type={type!r}")
        if proto:
            msg.append(f"proto={proto!r}")
        if flags:
            msg.append(f"flags={flags!r}")
        logger.info("Get address info %s", "".join(msg))
        return socket.getaddrinfo(host, port, family, type, proto, flags)

    async def getaddrinfo(self, host, port, *, family=0, type=0, proto=0, flags=0):
        if self.get_debug():
            getaddr_func = self._getaddrinfo_debug
        else:
            getaddr_func = socket.getaddrinfo

        return await self.run_in_executor(
            None, getaddr_func, host, port, family, type, proto, flags
        )

    async def getnameinfo(self, sockaddr, flags=0):
        return await self.run_in_executor(None, socket.getnameinfo, sockaddr, flags)

    # -- sock_* coroutines (ported from selector_events) ------------------------

    async def sock_recv(self, sock, n):
        base_events._check_ssl_socket(sock)
        if self.get_debug() and sock.gettimeout() != 0:
            raise ValueError("the socket must be non-blocking")
        try:
            return sock.recv(n)
        except (BlockingIOError, InterruptedError):
            pass
        fut = self.create_future()
        fd = sock.fileno()
        self._ensure_fd_no_transport(fd)
        handle = self._add_reader(fd, self._sock_recv, fut, sock, n)
        fut.add_done_callback(functools.partial(self._sock_read_done, fd, handle=handle))
        return await fut

    def _sock_read_done(self, fd, fut, handle=None):
        if handle is None or not handle.cancelled():
            self.remove_reader(fd)

    def _sock_recv(self, fut, sock, n):
        if fut.done():
            return
        try:
            data = sock.recv(n)
        except (BlockingIOError, InterruptedError):
            return  # try again next time
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            fut.set_exception(exc)
        else:
            fut.set_result(data)

    async def sock_recv_into(self, sock, buf):
        base_events._check_ssl_socket(sock)
        if self.get_debug() and sock.gettimeout() != 0:
            raise ValueError("the socket must be non-blocking")
        try:
            return sock.recv_into(buf)
        except (BlockingIOError, InterruptedError):
            pass
        fut = self.create_future()
        fd = sock.fileno()
        self._ensure_fd_no_transport(fd)
        handle = self._add_reader(fd, self._sock_recv_into, fut, sock, buf)
        fut.add_done_callback(functools.partial(self._sock_read_done, fd, handle=handle))
        return await fut

    def _sock_recv_into(self, fut, sock, buf):
        if fut.done():
            return
        try:
            nbytes = sock.recv_into(buf)
        except (BlockingIOError, InterruptedError):
            return  # try again next time
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            fut.set_exception(exc)
        else:
            fut.set_result(nbytes)

    async def sock_recvfrom(self, sock, bufsize):
        base_events._check_ssl_socket(sock)
        if self.get_debug() and sock.gettimeout() != 0:
            raise ValueError("the socket must be non-blocking")
        try:
            return sock.recvfrom(bufsize)
        except (BlockingIOError, InterruptedError):
            pass
        fut = self.create_future()
        fd = sock.fileno()
        self._ensure_fd_no_transport(fd)
        handle = self._add_reader(fd, self._sock_recvfrom, fut, sock, bufsize)
        fut.add_done_callback(functools.partial(self._sock_read_done, fd, handle=handle))
        return await fut

    def _sock_recvfrom(self, fut, sock, bufsize):
        if fut.done():
            return
        try:
            result = sock.recvfrom(bufsize)
        except (BlockingIOError, InterruptedError):
            return  # try again next time
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            fut.set_exception(exc)
        else:
            fut.set_result(result)

    async def sock_recvfrom_into(self, sock, buf, nbytes=0):
        base_events._check_ssl_socket(sock)
        if self.get_debug() and sock.gettimeout() != 0:
            raise ValueError("the socket must be non-blocking")
        if not nbytes:
            nbytes = len(buf)

        try:
            return sock.recvfrom_into(buf, nbytes)
        except (BlockingIOError, InterruptedError):
            pass
        fut = self.create_future()
        fd = sock.fileno()
        self._ensure_fd_no_transport(fd)
        handle = self._add_reader(fd, self._sock_recvfrom_into, fut, sock, buf, nbytes)
        fut.add_done_callback(functools.partial(self._sock_read_done, fd, handle=handle))
        return await fut

    def _sock_recvfrom_into(self, fut, sock, buf, bufsize):
        if fut.done():
            return
        try:
            result = sock.recvfrom_into(buf, bufsize)
        except (BlockingIOError, InterruptedError):
            return  # try again next time
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            fut.set_exception(exc)
        else:
            fut.set_result(result)

    async def sock_sendall(self, sock, data):
        base_events._check_ssl_socket(sock)
        if self.get_debug() and sock.gettimeout() != 0:
            raise ValueError("the socket must be non-blocking")
        try:
            n = sock.send(data)
        except (BlockingIOError, InterruptedError):
            n = 0

        if n == len(data):
            # all data sent
            return

        fut = self.create_future()
        fd = sock.fileno()
        self._ensure_fd_no_transport(fd)
        # use a trick with a list in closure to store a mutable state
        handle = self._add_writer(
            fd, self._sock_sendall, fut, sock, memoryview(data), [n]
        )
        fut.add_done_callback(functools.partial(self._sock_write_done, fd, handle=handle))
        return await fut

    def _sock_sendall(self, fut, sock, view, pos):
        if fut.done():
            # Future cancellation can be scheduled on previous loop iteration
            return
        start = pos[0]
        try:
            n = sock.send(view[start:])
        except (BlockingIOError, InterruptedError):
            return
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            fut.set_exception(exc)
            return

        start += n

        if start == len(view):
            fut.set_result(None)
        else:
            pos[0] = start

    async def sock_sendto(self, sock, data, address):
        base_events._check_ssl_socket(sock)
        if self.get_debug() and sock.gettimeout() != 0:
            raise ValueError("the socket must be non-blocking")
        try:
            return sock.sendto(data, address)
        except (BlockingIOError, InterruptedError):
            pass

        fut = self.create_future()
        fd = sock.fileno()
        self._ensure_fd_no_transport(fd)
        handle = self._add_writer(fd, self._sock_sendto, fut, sock, data, address)
        fut.add_done_callback(functools.partial(self._sock_write_done, fd, handle=handle))
        return await fut

    def _sock_sendto(self, fut, sock, data, address):
        if fut.done():
            # Future cancellation can be scheduled on previous loop iteration
            return
        try:
            n = sock.sendto(data, 0, address)
        except (BlockingIOError, InterruptedError):
            return
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            fut.set_exception(exc)
        else:
            fut.set_result(n)

    async def sock_connect(self, sock, address):
        base_events._check_ssl_socket(sock)
        if self.get_debug() and sock.gettimeout() != 0:
            raise ValueError("the socket must be non-blocking")

        if sock.family == socket.AF_INET or (
            base_events._HAS_IPv6 and sock.family == socket.AF_INET6
        ):
            resolved = await self._ensure_resolved(
                address,
                family=sock.family,
                type=sock.type,
                proto=sock.proto,
                loop=self,
            )
            _, _, _, _, address = resolved[0]

        fut = self.create_future()
        self._sock_connect(fut, sock, address)
        try:
            return await fut
        finally:
            # Needed to break cycles when an exception occurs.
            fut = None

    def _sock_connect(self, fut, sock, address):
        fd = sock.fileno()
        try:
            sock.connect(address)
        except (BlockingIOError, InterruptedError):
            # Issue #23618: When the C function connect() fails with EINTR,
            # the connection runs in background. We have to wait until the
            # socket becomes writable to be notified when the connection
            # succeed or fails.
            self._ensure_fd_no_transport(fd)
            handle = self._add_writer(fd, self._sock_connect_cb, fut, sock, address)
            fut.add_done_callback(
                functools.partial(self._sock_write_done, fd, handle=handle)
            )
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            fut.set_exception(exc)
        else:
            fut.set_result(None)
        finally:
            fut = None

    def _sock_write_done(self, fd, fut, handle=None):
        if handle is None or not handle.cancelled():
            self.remove_writer(fd)

    def _sock_connect_cb(self, fut, sock, address):
        if fut.done():
            return

        try:
            err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if err != 0:
                # Jump to any except clause below.
                raise OSError(err, f"Connect call failed {address}")
        except (BlockingIOError, InterruptedError):
            # socket is still registered, the callback will be retried later
            pass
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            fut.set_exception(exc)
        else:
            fut.set_result(None)
        finally:
            fut = None

    async def sock_accept(self, sock):
        base_events._check_ssl_socket(sock)
        if self.get_debug() and sock.gettimeout() != 0:
            raise ValueError("the socket must be non-blocking")
        fut = self.create_future()
        self._sock_accept(fut, sock)
        return await fut

    def _sock_accept(self, fut, sock):
        # gh-153761: _sock_accept must not scheduled with already cancelled future
        if fut.done():
            return
        fd = sock.fileno()
        try:
            conn, address = sock.accept()
            conn.setblocking(False)
        except (BlockingIOError, InterruptedError):
            self._ensure_fd_no_transport(fd)
            handle = self._add_reader(fd, self._sock_accept, fut, sock)
            fut.add_done_callback(
                functools.partial(self._sock_read_done, fd, handle=handle)
            )
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            fut.set_exception(exc)
        else:
            fut.set_result((conn, address))

    # -- sendfile ---------------------------------------------------------------

    def _check_sendfile_params(self, sock, file, offset, count):
        if "b" not in getattr(file, "mode", "b"):
            raise ValueError("file should be opened in binary mode")
        if not sock.type == socket.SOCK_STREAM:
            raise ValueError("only SOCK_STREAM type sockets are supported")
        if count is not None:
            if not isinstance(count, int):
                raise TypeError(
                    "count must be a positive integer (got {!r})".format(count)
                )
            if count <= 0:
                raise ValueError(
                    "count must be a positive integer (got {!r})".format(count)
                )
        if not isinstance(offset, int):
            raise TypeError(
                "offset must be a non-negative integer (got {!r})".format(offset)
            )
        if offset < 0:
            raise ValueError(
                "offset must be a non-negative integer (got {!r})".format(offset)
            )

    async def sock_sendfile(self, sock, file, offset=0, count=None, *, fallback=True):
        if self.get_debug() and sock.gettimeout() != 0:
            raise ValueError("the socket must be non-blocking")
        base_events._check_ssl_socket(sock)
        self._check_sendfile_params(sock, file, offset, count)
        try:
            return await self._sock_sendfile_native(sock, file, offset, count)
        except exceptions.SendfileNotAvailableError as exc:
            if not fallback:
                raise
        return await self._sock_sendfile_fallback(sock, file, offset, count)

    async def _sock_sendfile_native(self, sock, file, offset, count):
        # No native sendfile integration yet: always use the fallback path
        # (executor reads + sock_sendall), which is correct on every platform.
        raise exceptions.SendfileNotAvailableError(
            f"syscall sendfile is not available for socket {sock!r} "
            f"and file {file!r} combination"
        )

    async def _sock_sendfile_fallback(self, sock, file, offset, count):
        if hasattr(file, "seek"):
            file.seek(offset)
        blocksize = (
            min(count, constants.SENDFILE_FALLBACK_READBUFFER_SIZE)
            if count
            else constants.SENDFILE_FALLBACK_READBUFFER_SIZE
        )
        buf = bytearray(blocksize)
        total_sent = 0
        try:
            while True:
                if count:
                    blocksize = min(count - total_sent, blocksize)
                    if blocksize <= 0:
                        break
                view = memoryview(buf)[:blocksize]
                read = await self.run_in_executor(None, file.readinto, view)
                if not read:
                    break  # EOF
                await self.sock_sendall(sock, view[:read])
                total_sent += read
            return total_sent
        finally:
            if total_sent > 0 and hasattr(file, "seek"):
                file.seek(offset + total_sent)

    async def _sendfile_native(self, transp, file, offset, count):
        del self._transports[transp._sock_fd]
        resume_reading = transp.is_reading()
        transp.pause_reading()
        await transp._make_empty_waiter()
        try:
            return await self.sock_sendfile(
                transp._sock, file, offset, count, fallback=False
            )
        finally:
            transp._reset_empty_waiter()
            if resume_reading:
                transp.resume_reading()
            self._transports[transp._sock_fd] = transp

    async def sendfile(self, transport, file, offset=0, count=None, *, fallback=True):
        """Send a file to transport."""
        if transport.is_closing():
            raise RuntimeError("Transport is closing")
        mode = getattr(transport, "_sendfile_compatible", constants._SendfileMode.UNSUPPORTED)
        if mode is constants._SendfileMode.UNSUPPORTED:
            raise RuntimeError(f"sendfile is not supported for transport {transport!r}")
        if mode is constants._SendfileMode.TRY_NATIVE:
            try:
                return await self._sendfile_native(transport, file, offset, count)
            except exceptions.SendfileNotAvailableError as exc:
                if not fallback:
                    raise

        if not fallback:
            raise RuntimeError(
                f"fallback is disabled and native sendfile is not "
                f"supported for transport {transport!r}"
            )
        return await self._sendfile_fallback(transport, file, offset, count)

    async def _sendfile_fallback(self, transp, file, offset, count):
        if hasattr(file, "seek"):
            file.seek(offset)
        blocksize = min(count, 16384) if count else 16384
        buf = bytearray(blocksize)
        total_sent = 0
        proto = base_events._SendfileFallbackProtocol(transp)
        try:
            while True:
                if count:
                    blocksize = min(count - total_sent, blocksize)
                    if blocksize <= 0:
                        return total_sent
                view = memoryview(buf)[:blocksize]
                read = await self.run_in_executor(None, file.readinto, view)
                if not read:
                    return total_sent  # EOF
                transp.write(view[:read])
                await proto.drain()
                total_sent += read
        finally:
            if total_sent > 0 and hasattr(file, "seek"):
                file.seek(offset + total_sent)
            await proto.restore()

    # -- transports ---------------------------------------------------------------

    def _make_socket_transport(self, sock, protocol, waiter=None, *, extra=None, server=None):
        self._ensure_fd_no_transport(sock)
        return _SelectorSocketTransport(self, sock, protocol, waiter, extra, server)

    def _make_ssl_transport(
        self,
        rawsock,
        protocol,
        sslcontext,
        waiter=None,
        *,
        server_side=False,
        server_hostname=None,
        extra=None,
        server=None,
        ssl_handshake_timeout=constants.SSL_HANDSHAKE_TIMEOUT,
        ssl_shutdown_timeout=constants.SSL_SHUTDOWN_TIMEOUT,
    ):
        self._ensure_fd_no_transport(rawsock)
        ssl_protocol = sslproto.SSLProtocol(
            self,
            protocol,
            sslcontext,
            waiter,
            server_side,
            server_hostname,
            ssl_handshake_timeout=ssl_handshake_timeout,
            ssl_shutdown_timeout=ssl_shutdown_timeout,
        )
        _SelectorSocketTransport(self, rawsock, ssl_protocol, extra=extra, server=server)
        return ssl_protocol._app_transport

    def _make_datagram_transport(self, sock, protocol, address=None, waiter=None, extra=None):
        self._ensure_fd_no_transport(sock)
        return _SelectorDatagramTransport(self, sock, protocol, address, waiter, extra)

    async def start_tls(
        self,
        transport,
        protocol,
        sslcontext,
        *,
        server_side=False,
        server_hostname=None,
        ssl_handshake_timeout=None,
        ssl_shutdown_timeout=None,
    ):
        """Upgrade transport to TLS."""
        if ssl is None:
            raise RuntimeError("Python ssl module is not available")

        if not isinstance(sslcontext, ssl.SSLContext):
            raise TypeError(
                "sslcontext is expected to be an instance of ssl.SSLContext, "
                f"got {sslcontext!r}"
            )

        if not getattr(transport, "_start_tls_compatible", False):
            raise TypeError(f"transport {transport!r} is not supported by start_tls()")

        waiter = self.create_future()
        ssl_protocol = sslproto.SSLProtocol(
            self,
            protocol,
            sslcontext,
            waiter,
            server_side,
            server_hostname,
            ssl_handshake_timeout=ssl_handshake_timeout,
            ssl_shutdown_timeout=ssl_shutdown_timeout,
            call_connection_made=False,
        )

        # Pause early so that "ssl_protocol.data_received()" doesn't
        # have a chance to get called before "ssl_protocol.connection_made()".
        transport.pause_reading()

        # gh-142352: move buffered StreamReader data to SSLProtocol
        if server_side:
            from asyncio.streams import StreamReaderProtocol

            if isinstance(protocol, StreamReaderProtocol):
                stream_reader = getattr(protocol, "_stream_reader", None)
                if stream_reader is not None:
                    buffer = stream_reader._buffer
                    if buffer:
                        ssl_protocol._incoming.write(buffer)
                        buffer.clear()

        transport.set_protocol(ssl_protocol)
        conmade_cb = self.call_soon(ssl_protocol.connection_made, transport)
        resume_cb = self.call_soon(transport.resume_reading)

        try:
            await waiter
        except BaseException:
            transport.close()
            conmade_cb.cancel()
            resume_cb.cancel()
            raise

        return ssl_protocol._app_transport

    # -- connections / servers (ported from base_events) ---------------------------

    async def _connect_sock(self, exceptions, addr_info, local_addr_infos=None):
        """Create, bind and connect one socket."""
        my_exceptions = []
        exceptions.append(my_exceptions)
        family, type_, proto, _, address = addr_info
        sock = None
        try:
            try:
                sock = socket.socket(family=family, type=type_, proto=proto)
                sock.setblocking(False)
                if local_addr_infos is not None:
                    for lfamily, _, _, _, laddr in local_addr_infos:
                        # skip local addresses of different family
                        if lfamily != family:
                            continue
                        try:
                            sock.bind(laddr)
                            break
                        except OSError as exc:
                            msg = (
                                "error while attempting to bind on "
                                f"address {laddr!r}: {str(exc).lower()}"
                            )
                            exc = OSError(exc.errno, msg)
                            my_exceptions.append(exc)
                    else:  # all bind attempts failed
                        if my_exceptions:
                            raise my_exceptions.pop()
                        else:
                            raise OSError(
                                f"no matching local address with {family=} found"
                            )
                await self.sock_connect(sock, address)
                return sock
            except OSError as exc:
                my_exceptions.append(exc)
                raise
        except BaseException:
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass
            raise
        finally:
            exceptions = my_exceptions = None

    async def create_connection(
        self,
        protocol_factory,
        host=None,
        port=None,
        *,
        ssl=None,
        family=0,
        proto=0,
        flags=0,
        sock=None,
        local_addr=None,
        server_hostname=None,
        ssl_handshake_timeout=None,
        ssl_shutdown_timeout=None,
        happy_eyeballs_delay=None,
        interleave=None,
        all_errors=False,
    ):
        """Connect to a TCP server; return a (transport, protocol) pair."""
        if server_hostname is not None and not ssl:
            raise ValueError("server_hostname is only meaningful with ssl")

        if server_hostname is None and ssl:
            if not host:
                raise ValueError(
                    "You must set server_hostname when using ssl without a host"
                )
            server_hostname = host

        if ssl_handshake_timeout is not None and not ssl:
            raise ValueError("ssl_handshake_timeout is only meaningful with ssl")

        if ssl_shutdown_timeout is not None and not ssl:
            raise ValueError("ssl_shutdown_timeout is only meaningful with ssl")

        if sock is not None:
            base_events._check_ssl_socket(sock)

        if happy_eyeballs_delay is not None and interleave is None:
            interleave = 1

        if host is not None or port is not None:
            if sock is not None:
                raise ValueError("host/port and sock can not be specified at the same time")

            infos = await self._ensure_resolved(
                (host, port),
                family=family,
                type=socket.SOCK_STREAM,
                proto=proto,
                flags=flags,
                loop=self,
            )
            if not infos:
                raise OSError("getaddrinfo() returned empty list")

            if local_addr is not None:
                laddr_infos = await self._ensure_resolved(
                    local_addr,
                    family=family,
                    type=socket.SOCK_STREAM,
                    proto=proto,
                    flags=flags,
                    loop=self,
                )
                if not laddr_infos:
                    raise OSError("getaddrinfo() returned empty list")
            else:
                laddr_infos = None

            if interleave:
                infos = base_events._interleave_addrinfos(infos, interleave)

            exceptions = []
            if happy_eyeballs_delay is None:
                # not using happy eyeballs
                for addrinfo in infos:
                    try:
                        sock = await self._connect_sock(exceptions, addrinfo, laddr_infos)
                        break
                    except OSError:
                        continue
            else:  # using happy eyeballs
                sock = (
                    await staggered.staggered_race(
                        (
                            lambda addrinfo=addrinfo: self._connect_sock(
                                exceptions, addrinfo, laddr_infos
                            )
                            for addrinfo in infos
                        ),
                        happy_eyeballs_delay,
                        loop=self,
                    )
                )[0]

            if sock is None:
                exceptions = [exc for sub in exceptions for exc in sub]
                try:
                    if all_errors:
                        raise ExceptionGroup("create_connection failed", exceptions)
                    if len(exceptions) == 1:
                        raise exceptions[0]
                    elif exceptions:
                        model = str(exceptions[0])
                        if all(str(exc) == model for exc in exceptions):
                            raise exceptions[0]
                        raise OSError(
                            "Multiple exceptions: {}".format(
                                ", ".join(str(exc) for exc in exceptions)
                            )
                        )
                    else:
                        raise TimeoutError("create_connection failed")
                finally:
                    exceptions = None

        else:
            if sock is None:
                raise ValueError("host and port was not specified and no sock specified")
            if sock.type != socket.SOCK_STREAM:
                raise ValueError(f"A Stream Socket was expected, got {sock!r}")

        transport, protocol = await self._create_connection_transport(
            sock,
            protocol_factory,
            ssl,
            server_hostname,
            ssl_handshake_timeout=ssl_handshake_timeout,
            ssl_shutdown_timeout=ssl_shutdown_timeout,
        )
        if self.get_debug():
            sock = transport.get_extra_info("socket")
            logger.debug("%r connected to %s:%r: (%r, %r)", sock, host, port, transport, protocol)
        return transport, protocol

    async def _create_connection_transport(
        self,
        sock,
        protocol_factory,
        ssl,
        server_hostname,
        server_side=False,
        ssl_handshake_timeout=None,
        ssl_shutdown_timeout=None,
    ):
        sock.setblocking(False)

        protocol = protocol_factory()
        waiter = self.create_future()
        if ssl:
            sslcontext = None if isinstance(ssl, bool) else ssl
            transport = self._make_ssl_transport(
                sock,
                protocol,
                sslcontext,
                waiter,
                server_side=server_side,
                server_hostname=server_hostname,
                ssl_handshake_timeout=ssl_handshake_timeout,
                ssl_shutdown_timeout=ssl_shutdown_timeout,
            )
        else:
            transport = self._make_socket_transport(sock, protocol, waiter)

        try:
            await waiter
        except BaseException:
            transport.close()
            raise

        return transport, protocol

    async def create_datagram_endpoint(
        self,
        protocol_factory,
        local_addr=None,
        remote_addr=None,
        *,
        family=0,
        proto=0,
        flags=0,
        reuse_port=None,
        allow_broadcast=None,
        sock=None,
    ):
        """Create datagram connection."""
        if sock is not None:
            if sock.type == socket.SOCK_STREAM:
                raise ValueError(f"A datagram socket was expected, got {sock!r}")
            if local_addr or remote_addr or family or proto or flags or reuse_port or allow_broadcast:
                opts = dict(
                    local_addr=local_addr,
                    remote_addr=remote_addr,
                    family=family,
                    proto=proto,
                    flags=flags,
                    reuse_port=reuse_port,
                    allow_broadcast=allow_broadcast,
                )
                problems = ", ".join(f"{k}={v}" for k, v in opts.items() if v)
                raise ValueError(
                    "socket modifier keyword arguments can not be used "
                    f"when sock is specified. ({problems})"
                )
            sock.setblocking(False)
            r_addr = None
        else:
            if not (local_addr or remote_addr):
                if family == 0:
                    raise ValueError("unexpected address family")
                addr_pairs_info = (((family, proto), (None, None)),)
            elif hasattr(socket, "AF_UNIX") and family == socket.AF_UNIX:
                for addr in (local_addr, remote_addr):
                    if addr is not None and not isinstance(addr, str):
                        raise TypeError("string is expected")

                if local_addr and local_addr[0] not in (0, "\x00"):
                    try:
                        if stat.S_ISSOCK(os.stat(local_addr).st_mode):
                            os.remove(local_addr)
                    except FileNotFoundError:
                        pass
                    except OSError as err:
                        logger.error(
                            "Unable to check or remove stale UNIX socket %r: %r",
                            local_addr,
                            err,
                        )

                addr_pairs_info = (((family, proto), (local_addr, remote_addr)),)
            else:
                # join address by (family, protocol)
                addr_infos = {}  # Using order preserving dict
                for idx, addr in ((0, local_addr), (1, remote_addr)):
                    if addr is not None:
                        if not (isinstance(addr, tuple) and len(addr) == 2):
                            raise TypeError("2-tuple is expected")

                        infos = await self._ensure_resolved(
                            addr,
                            family=family,
                            type=socket.SOCK_DGRAM,
                            proto=proto,
                            flags=flags,
                            loop=self,
                        )
                        if not infos:
                            raise OSError("getaddrinfo() returned empty list")

                        for fam, _, pro, _, address in infos:
                            key = (fam, pro)
                            if key not in addr_infos:
                                addr_infos[key] = [None, None]
                            addr_infos[key][idx] = address

                # each addr has to have info for each (family, proto) pair
                addr_pairs_info = [
                    (key, addr_pair)
                    for key, addr_pair in addr_infos.items()
                    if not (
                        (local_addr and addr_pair[0] is None)
                        or (remote_addr and addr_pair[1] is None)
                    )
                ]

                if not addr_pairs_info:
                    raise ValueError("can not get address information")

            exceptions = []

            for (family, proto), (local_address, remote_address) in addr_pairs_info:
                sock = None
                r_addr = None
                try:
                    sock = socket.socket(family=family, type=socket.SOCK_DGRAM, proto=proto)
                    if reuse_port:
                        base_events._set_reuseport(sock)
                    if allow_broadcast:
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                    sock.setblocking(False)

                    if local_addr:
                        sock.bind(local_address)
                    if remote_addr:
                        if not allow_broadcast:
                            await self.sock_connect(sock, remote_address)
                        r_addr = remote_address
                except OSError as exc:
                    if sock is not None:
                        sock.close()
                    exceptions.append(exc)
                except BaseException:
                    if sock is not None:
                        sock.close()
                    raise
                else:
                    break
            else:
                raise exceptions[0]

        protocol = protocol_factory()
        waiter = self.create_future()
        transport = self._make_datagram_transport(sock, protocol, r_addr, waiter)
        if self.get_debug():
            if local_addr:
                logger.info(
                    "Datagram endpoint local_addr=%r remote_addr=%r created: (%r, %r)",
                    local_addr,
                    remote_addr,
                    transport,
                    protocol,
                )
            else:
                logger.debug(
                    "Datagram endpoint remote_addr=%r created: (%r, %r)",
                    remote_addr,
                    transport,
                    protocol,
                )

        try:
            await waiter
        except BaseException:
            transport.close()
            raise

        return transport, protocol

    async def _ensure_resolved(
        self, address, *, family=0, type=socket.SOCK_STREAM, proto=0, flags=0, loop
    ):
        host, port = address[:2]
        info = base_events._ipaddr_info(host, port, family, type, proto, *address[2:])
        if info is not None:
            # "host" is already a resolved IP.
            return [info]
        else:
            return await loop.getaddrinfo(
                host, port, family=family, type=type, proto=proto, flags=flags
            )

    async def _create_server_getaddrinfo(self, host, port, family, flags):
        infos = await self._ensure_resolved(
            (host, port), family=family, type=socket.SOCK_STREAM, flags=flags, loop=self
        )
        if not infos:
            raise OSError(f"getaddrinfo({host!r}) returned empty list")
        return infos

    async def create_server(
        self,
        protocol_factory,
        host=None,
        port=None,
        *,
        family=socket.AF_UNSPEC,
        flags=socket.AI_PASSIVE,
        sock=None,
        backlog=100,
        ssl=None,
        reuse_address=None,
        reuse_port=None,
        keep_alive=None,
        ssl_handshake_timeout=None,
        ssl_shutdown_timeout=None,
        start_serving=True,
    ):
        """Create a TCP server."""
        if isinstance(ssl, bool):
            raise TypeError("ssl argument must be an SSLContext or None")

        if ssl_handshake_timeout is not None and ssl is None:
            raise ValueError("ssl_handshake_timeout is only meaningful with ssl")

        if ssl_shutdown_timeout is not None and ssl is None:
            raise ValueError("ssl_shutdown_timeout is only meaningful with ssl")

        if sock is not None:
            base_events._check_ssl_socket(sock)

        server = None
        if host is not None or port is not None:
            if sock is not None:
                raise ValueError("host/port and sock can not be specified at the same time")

            if reuse_address is None:
                reuse_address = os.name == "posix" and sys.platform != "cygwin"
            sockets = []
            if host == "":
                hosts = [None]
            elif isinstance(host, str) or not isinstance(host, collections.abc.Iterable):
                hosts = [host]
            else:
                hosts = host

            fs = [
                self._create_server_getaddrinfo(host, port, family=family, flags=flags)
                for host in hosts
            ]
            infos = await tasks.gather(*fs)
            infos = set(itertools.chain.from_iterable(infos))

            completed = False
            try:
                for res in infos:
                    af, socktype, proto, canonname, sa = res
                    try:
                        sock = socket.socket(af, socktype, proto)
                    except socket.error:
                        if self.get_debug():
                            logger.warning(
                                "create_server() failed to create socket.socket(%r, %r, %r)",
                                af,
                                socktype,
                                proto,
                                exc_info=True,
                            )
                        continue
                    sockets.append(sock)
                    if reuse_address:
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
                    # Since Linux 6.12.9, SO_REUSEPORT is not allowed
                    # on other address families than AF_INET/AF_INET6.
                    if reuse_port and af in (socket.AF_INET, socket.AF_INET6):
                        base_events._set_reuseport(sock)
                    if keep_alive:
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)
                    # Disable IPv4/IPv6 dual stack support (enabled by
                    # default on Linux) which makes a single socket
                    # listen on both address families.
                    if (
                        base_events._HAS_IPv6
                        and af == socket.AF_INET6
                        and hasattr(socket, "IPPROTO_IPV6")
                    ):
                        sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, True)
                    try:
                        sock.bind(sa)
                    except OSError as err:
                        msg = "error while attempting to bind on address %r: %s" % (
                            sa,
                            str(err).lower(),
                        )
                        if err.errno == errno.EADDRNOTAVAIL:
                            sockets.pop()
                            sock.close()
                            if self.get_debug():
                                logger.warning(msg)
                            continue
                        raise OSError(err.errno, msg) from None

                if not sockets:
                    raise OSError(
                        "could not bind on any address out of %r"
                        % ([info[4] for info in infos],)
                    )

                completed = True
            finally:
                if not completed:
                    for sock in sockets:
                        sock.close()
        else:
            if sock is None:
                raise ValueError("Neither host/port nor sock were specified")
            if sock.type != socket.SOCK_STREAM:
                raise ValueError(f"A Stream Socket was expected, got {sock!r}")
            sockets = [sock]

        for sock in sockets:
            sock.setblocking(False)

        server = base_events.Server(
            self, sockets, protocol_factory, ssl, backlog, ssl_handshake_timeout,
            ssl_shutdown_timeout,
        )
        if start_serving:
            server._start_serving()
            # Skip one loop iteration so that all 'loop.add_reader'
            # go through.
            await tasks.sleep(0)

        if self.get_debug():
            logger.info("%r is serving", server)
        return server

    async def connect_accepted_socket(
        self,
        protocol_factory,
        sock,
        *,
        ssl=None,
        ssl_handshake_timeout=None,
        ssl_shutdown_timeout=None,
    ):
        if sock.type != socket.SOCK_STREAM:
            raise ValueError(f"A Stream Socket was expected, got {sock!r}")

        if ssl_handshake_timeout is not None and not ssl:
            raise ValueError("ssl_handshake_timeout is only meaningful with ssl")

        if ssl_shutdown_timeout is not None and not ssl:
            raise ValueError("ssl_shutdown_timeout is only meaningful with ssl")

        if sock is not None:
            base_events._check_ssl_socket(sock)

        transport, protocol = await self._create_connection_transport(
            sock,
            protocol_factory,
            ssl,
            "",
            server_side=True,
            ssl_handshake_timeout=ssl_handshake_timeout,
            ssl_shutdown_timeout=ssl_shutdown_timeout,
        )
        if self.get_debug():
            sock = transport.get_extra_info("socket")
            logger.debug("%r handled: (%r, %r)", sock, transport, protocol)
        return transport, protocol

    def _start_serving(
        self,
        protocol_factory,
        sock,
        sslcontext=None,
        server=None,
        backlog=100,
        ssl_handshake_timeout=constants.SSL_HANDSHAKE_TIMEOUT,
        ssl_shutdown_timeout=constants.SSL_SHUTDOWN_TIMEOUT,
    ):
        self._add_reader(
            sock.fileno(),
            self._accept_connection,
            protocol_factory,
            sock,
            sslcontext,
            server,
            backlog,
            ssl_handshake_timeout,
            ssl_shutdown_timeout,
        )

    def _accept_connection(
        self,
        protocol_factory,
        sock,
        sslcontext=None,
        server=None,
        backlog=100,
        ssl_handshake_timeout=constants.SSL_HANDSHAKE_TIMEOUT,
        ssl_shutdown_timeout=constants.SSL_SHUTDOWN_TIMEOUT,
    ):
        # This method is only called once for each event loop tick where the
        # listening socket has triggered a read. There may be multiple
        # connections waiting for an .accept() so it is called in a loop.
        # See https://bugs.python.org/issue27906 for more details.
        for _ in range(backlog + 1):
            try:
                conn, addr = sock.accept()
                if self.get_debug():
                    logger.debug("%r got a new connection from %r: %r", server, addr, conn)
                conn.setblocking(False)
            except ConnectionAbortedError:
                # Discard connections that were aborted before accept().
                continue
            except (BlockingIOError, InterruptedError):
                # Early exit because of a signal or
                # the socket accept buffer is empty.
                return
            except OSError as exc:
                # There's nowhere to send the error, so just log it.
                if exc.errno in (errno.EMFILE, errno.ENFILE, errno.ENOBUFS, errno.ENOMEM):
                    # Some platforms (e.g. Linux keep reporting the FD as
                    # ready, so we remove the read handler temporarily.
                    # We'll try again in a while.
                    self.call_exception_handler(
                        {
                            "message": "socket.accept() out of system resource",
                            "exception": exc,
                            "socket": trsock.TransportSocket(sock),
                        }
                    )
                    self._remove_reader(sock.fileno())
                    self.call_later(
                        constants.ACCEPT_RETRY_DELAY,
                        self._start_serving,
                        protocol_factory,
                        sock,
                        sslcontext,
                        server,
                        backlog,
                        ssl_handshake_timeout,
                        ssl_shutdown_timeout,
                    )
                else:
                    raise  # The event loop will catch, log and ignore it.
            else:
                extra = {"peername": addr}
                accept = self._accept_connection2(
                    protocol_factory,
                    conn,
                    extra,
                    sslcontext,
                    server,
                    ssl_handshake_timeout,
                    ssl_shutdown_timeout,
                )
                self.create_task(accept)

    async def _accept_connection2(
        self,
        protocol_factory,
        conn,
        extra,
        sslcontext=None,
        server=None,
        ssl_handshake_timeout=constants.SSL_HANDSHAKE_TIMEOUT,
        ssl_shutdown_timeout=constants.SSL_SHUTDOWN_TIMEOUT,
    ):
        protocol = None
        transport = None
        try:
            protocol = protocol_factory()
            waiter = self.create_future()
            if sslcontext:
                transport = self._make_ssl_transport(
                    conn,
                    protocol,
                    sslcontext,
                    waiter=waiter,
                    server_side=True,
                    extra=extra,
                    server=server,
                    ssl_handshake_timeout=ssl_handshake_timeout,
                    ssl_shutdown_timeout=ssl_shutdown_timeout,
                )
            else:
                transport = self._make_socket_transport(
                    conn, protocol, waiter=waiter, extra=extra, server=server
                )

            try:
                await waiter
            except BaseException:
                transport.close()
                # gh-109534: When an exception is raised by the SSLProtocol
                # object the exception set in this future can keep the
                # protocol object alive and cause a reference cycle.
                waiter = None
                raise
                # It's now up to the protocol to handle the connection.

        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            if self.get_debug():
                context = {
                    "message": "Error on transport creation for incoming connection",
                    "exception": exc,
                }
                if protocol is not None:
                    context["protocol"] = protocol
                if transport is not None:
                    context["transport"] = transport
                self.call_exception_handler(context)

    def _stop_serving(self, sock):
        self._remove_reader(sock.fileno())
        sock.close()

    async def create_unix_connection(
        self,
        protocol_factory,
        path=None,
        *,
        ssl=None,
        sock=None,
        server_hostname=None,
        ssl_handshake_timeout=None,
        ssl_shutdown_timeout=None,
    ):
        assert server_hostname is None or isinstance(server_hostname, str)
        if ssl:
            if server_hostname is None:
                raise ValueError("you have to pass server_hostname when using ssl")
        else:
            if server_hostname is not None:
                raise ValueError("server_hostname is only meaningful with ssl")
            if ssl_handshake_timeout is not None:
                raise ValueError("ssl_handshake_timeout is only meaningful with ssl")
            if ssl_shutdown_timeout is not None:
                raise ValueError("ssl_shutdown_timeout is only meaningful with ssl")

        if path is not None:
            if sock is not None:
                raise ValueError("path and sock can not be specified at the same time")

            path = os.fspath(path)
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0)
            try:
                sock.setblocking(False)
                await self.sock_connect(sock, path)
            except BaseException:
                sock.close()
                raise

        else:
            if sock is None:
                raise ValueError("no path and sock were specified")
            if sock.family != socket.AF_UNIX or sock.type != socket.SOCK_STREAM:
                raise ValueError(f"A UNIX Domain Stream Socket was expected, got {sock!r}")
            sock.setblocking(False)

        transport, protocol = await self._create_connection_transport(
            sock,
            protocol_factory,
            ssl,
            server_hostname,
            ssl_handshake_timeout=ssl_handshake_timeout,
            ssl_shutdown_timeout=ssl_shutdown_timeout,
        )
        return transport, protocol

    async def create_unix_server(
        self,
        protocol_factory,
        path=None,
        *,
        sock=None,
        backlog=100,
        ssl=None,
        ssl_handshake_timeout=None,
        ssl_shutdown_timeout=None,
        start_serving=True,
        cleanup_socket=True,
    ):
        if isinstance(ssl, bool):
            raise TypeError("ssl argument must be an SSLContext or None")

        if ssl_handshake_timeout is not None and not ssl:
            raise ValueError("ssl_handshake_timeout is only meaningful with ssl")

        if ssl_shutdown_timeout is not None and not ssl:
            raise ValueError("ssl_shutdown_timeout is only meaningful with ssl")

        if path is not None:
            if sock is not None:
                raise ValueError("path and sock can not be specified at the same time")

            path = os.fspath(path)
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

            # Check for abstract socket. `str` and `bytes` paths are supported.
            if path[0] not in (0, "\x00"):
                try:
                    if stat.S_ISSOCK(os.stat(path).st_mode):
                        os.remove(path)
                except FileNotFoundError:
                    pass
                except OSError as err:
                    # Directory may have permissions only to create socket.
                    logger.error(
                        "Unable to check or remove stale UNIX socket %r: %r", path, err
                    )

            try:
                sock.bind(path)
            except OSError as exc:
                sock.close()
                if exc.errno == errno.EADDRINUSE:
                    # Let's improve the error message by adding
                    # with what exact address it occurs.
                    msg = f"Address {path!r} is already in use"
                    raise OSError(errno.EADDRINUSE, msg) from None
                else:
                    raise
            except BaseException:
                sock.close()
                raise
        else:
            if sock is None:
                raise ValueError("path was not specified, and no sock specified")

            if sock.family != socket.AF_UNIX or sock.type != socket.SOCK_STREAM:
                raise ValueError(f"A UNIX Domain Stream Socket was expected, got {sock!r}")

        if cleanup_socket:
            path = sock.getsockname()
            # Check for abstract socket. `str` and `bytes` paths are supported.
            if path[0] not in (0, "\x00"):
                try:
                    self._unix_server_sockets[sock] = os.stat(path).st_ino
                except FileNotFoundError:
                    pass

        sock.setblocking(False)
        server = base_events.Server(
            self, [sock], protocol_factory, ssl, backlog, ssl_handshake_timeout,
            ssl_shutdown_timeout,
        )
        if start_serving:
            server._start_serving()
            # Skip one loop iteration so that all 'loop.add_reader'
            # go through.
            await tasks.sleep(0)

        return server

    # -- signals ---------------------------------------------------------------

    def _check_signal(self, sig):
        """Validate a signal number (simplified unix_events check)."""
        if not isinstance(sig, int):
            raise TypeError(f"sig must be an int, not {sig!r}")
        if sig not in range(1, signal.NSIG):
            raise ValueError(f"invalid signal number {sig}")
        if sig in (signal.SIGKILL, signal.SIGSTOP):
            raise RuntimeError(f"signals SIGKILL and SIGSTOP cannot be caught")

    def add_signal_handler(self, sig, callback, *args):
        """Add a handler for a signal."""
        self._check_closed()
        self._check_signal(sig)
        self._check_callback(callback, "add_signal_handler")

        def _handler(signum, frame):
            self.call_soon_threadsafe(callback, *args)

        try:
            signal.signal(sig, _handler)
        except OSError as exc:
            raise RuntimeError(str(exc)) from exc
        self._signal_handlers[sig] = _handler

    def remove_signal_handler(self, sig):
        """Remove a handler for a signal; return True if one was removed."""
        self._check_signal(sig)
        try:
            del self._signal_handlers[sig]
        except KeyError:
            return False
        try:
            signal.signal(sig, signal.SIG_DFL)
        except OSError:
            pass
        return True

    # -- subprocess / pipes: unsupported, fail like asyncio expects -----------

    async def subprocess_exec(self, *args, **kwargs):
        raise NotImplementedError("subprocesses are not supported by tokioop")

    async def subprocess_shell(self, *args, **kwargs):
        raise NotImplementedError("subprocesses are not supported by tokioop")

    async def connect_read_pipe(self, protocol_factory, pipe):
        raise NotImplementedError("pipes are not supported by tokioop")

    async def connect_write_pipe(self, protocol_factory, pipe):
        raise NotImplementedError("pipes are not supported by tokioop")
