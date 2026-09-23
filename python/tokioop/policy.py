"""Event-loop policy for tokioop."""

import asyncio
import threading

__all__ = ("RustEventLoopPolicy",)


class _LoopLocal(threading.local):
    _loop = None


class RustEventLoopPolicy(asyncio.AbstractEventLoopPolicy):
    """Policy creating :class:`tokioop.loop.RustEventLoop` loops."""

    def __init__(self):
        self._local = _LoopLocal()
        # Imported late to avoid a module cycle (loop.py never imports policy).
        from tokioop.loop import RustEventLoop

        self._loop_factory = RustEventLoop

    def get_event_loop(self):
        if self._local._loop is None:
            raise RuntimeError(
                "There is no current event loop in thread %r."
                % threading.current_thread().name
            )
        return self._local._loop

    def set_event_loop(self, loop):
        self._local._loop = loop

    def new_event_loop(self):
        return self._loop_factory()
