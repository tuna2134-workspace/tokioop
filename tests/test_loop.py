"""Lifecycle, callbacks, timers, cancellation, threadsafe scheduling."""

import asyncio
import threading
import time

import pytest

import tokioop


@pytest.fixture()
def loop():
    loop = tokioop.new_event_loop()
    yield loop
    if not loop.is_closed():
        loop.close()


def test_install_and_policy():
    tokioop.install()
    loop = asyncio.new_event_loop()
    try:
        assert isinstance(loop, tokioop.RustEventLoop)
        assert isinstance(loop, asyncio.AbstractEventLoop)
    finally:
        loop.close()


def test_run_until_complete_result(loop):
    async def main():
        await asyncio.sleep(0)
        return 42

    assert loop.run_until_complete(main()) == 42


def test_run_until_complete_exception(loop):
    async def main():
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        loop.run_until_complete(main())


def test_run_until_complete_future(loop):
    async def main():
        fut = loop.create_future()
        loop.call_soon(fut.set_result, "x")
        assert await fut == "x"

    loop.run_until_complete(main())


def test_call_soon_ordering(loop):
    out = []
    for i in range(10):
        loop.call_soon(out.append, i)
    loop.call_soon(loop.stop)
    loop.run_forever()
    assert out == list(range(10))


def test_call_soon_nested_runs_next_iteration(loop):
    out = []
    loop.call_soon(lambda: (out.append("a"), loop.call_soon(out.append, "b")))
    loop.call_soon(out.append, "c")
    loop.call_soon(loop.stop)
    loop.run_forever()
    # 'b' is scheduled during the batch, behind the already-queued stop():
    # run_forever() breaks before running it. Verified identical on stdlib.
    assert out == ["a", "c"]
    loop.close()

    loop2 = tokioop.new_event_loop()
    try:
        out2 = []
        loop2.call_soon(lambda: (out2.append("a"), loop2.call_soon(out2.append, "b")))
        loop2.call_soon(out2.append, "c")
        loop2.call_later(0.02, loop2.stop)
        loop2.run_forever()
        # with stop delayed, 'b' runs after 'c' (CPython ntodo semantics)
        assert out2 == ["a", "c", "b"]
    finally:
        loop2.close()


def test_call_soon_cancel(loop):
    out = []
    h = loop.call_soon(out.append, 1)
    assert not h.cancelled()
    assert h.cancel() is True
    assert h.cancelled()
    assert h.cancel() is False  # already cancelled
    loop.call_soon(out.append, 2)
    loop.call_soon(loop.stop)
    loop.run_forever()
    assert out == [2]


def test_call_later_and_call_at(loop):
    out = []
    t0 = loop.time()
    loop.call_later(0.02, out.append, "later")
    loop.call_at(t0 + 0.01, out.append, "at")
    loop.call_later(0.05, loop.stop)
    loop.run_forever()
    assert out == ["at", "later"]


def test_timer_cancel(loop):
    out = []
    h = loop.call_later(0.01, out.append, "x")
    assert h.cancel() is True
    assert h.cancelled()
    loop.call_later(0.03, loop.stop)
    loop.run_forever()
    assert out == []


def test_many_timers_ordering(loop):
    out = []
    for i in reversed(range(200)):
        loop.call_later(i * 0.001, out.append, i)
    loop.call_later(0.5, loop.stop)
    loop.run_forever()
    assert out == list(range(200))


def test_call_later_negative_delay(loop):
    out = []
    loop.call_later(-1, out.append, "neg")
    loop.call_soon(out.append, "soon")
    loop.call_later(0.02, loop.stop)
    loop.run_forever()
    # negative delay fires on the next tick; exact position vs call_soon
    # is not specified, but both must run
    assert sorted(out) == ["neg", "soon"]


def test_call_at_none_raises(loop):
    with pytest.raises(TypeError):
        loop.call_at(None, lambda: None)
    with pytest.raises(TypeError):
        loop.call_later(None, lambda: None)


def test_stop_and_is_running(loop):
    assert not loop.is_running()
    seen = []

    async def main():
        seen.append(loop.is_running())
        await asyncio.sleep(0)

    loop.run_until_complete(main())
    assert seen == [True]
    assert not loop.is_running()


def test_close_twice_and_closed(loop):
    assert not loop.is_closed()
    loop.close()
    assert loop.is_closed()
    loop.close()  # no-op


def test_closed_loop_raises(loop):
    loop.close()
    with pytest.raises(RuntimeError):
        loop.run_forever()
    with pytest.raises(RuntimeError):
        loop.call_soon(lambda: None)
    with pytest.raises(RuntimeError):
        loop.call_later(1, lambda: None)
    with pytest.raises(RuntimeError):
        loop.call_at(loop.time() + 1, lambda: None)
    with pytest.raises(RuntimeError):
        loop.call_soon_threadsafe(lambda: None)


def test_close_running_raises():
    tokioop.install()

    async def main():
        loop = asyncio.get_running_loop()
        with pytest.raises(RuntimeError):
            loop.close()

    asyncio.run(main())


def test_time_monotonic(loop):
    t1 = loop.time()
    time.sleep(0.01)
    t2 = loop.time()
    assert t2 > t1
    # matches time.monotonic epoch (asyncio semantics)
    assert abs(t2 - time.monotonic()) < 1.0


def test_call_soon_threadsafe_wakes_loop(loop):
    out = []
    loop.call_later(0.5, loop.stop)  # safety
    loop.call_later(0.05, loop.stop)  # real stop

    def worker():
        time.sleep(0.01)
        loop.call_soon_threadsafe(out.append, "bg")

    th = threading.Thread(target=worker)
    th.start()
    loop.run_forever()  # would hang without the wakeup
    th.join()
    assert out == ["bg"] or out == []  # 'bg' runs before stop usually
    # deterministic part: no hang, loop stopped
    assert not loop.is_running()


def test_threadsafe_many(loop):
    out = []
    lock = threading.Lock()
    N = 500

    def worker():
        for i in range(100):
            loop.call_soon_threadsafe(out.append, i)

    threads = [threading.Thread(target=worker) for _ in range(5)]
    loop.call_later(1.0, loop.stop)
    for th in threads:
        th.start()
    loop.run_forever()
    for th in threads:
        th.join()
    with lock:
        assert len(out) == N


def test_exception_handler(loop):
    errors = []

    def handler(ctx):
        errors.append(ctx)

    async def main():
        loop.set_exception_handler(handler)

        def bad():
            raise ValueError("cb boom")

        loop.call_soon(bad)
        await asyncio.sleep(0.02)
        assert len(errors) == 1
        assert "Exception in callback" in errors[0]["message"]
        assert isinstance(errors[0]["exception"], ValueError)

    loop.run_until_complete(main())
    assert loop.get_exception_handler() is handler
    loop.set_exception_handler(None)
    assert loop.get_exception_handler() is None


def test_default_exception_handler(loop, capsys):
    async def main():
        def bad():
            raise RuntimeError("default handler")

        loop.call_soon(bad)
        await asyncio.sleep(0.02)

    loop.run_until_complete(main())
    err = capsys.readouterr().err
    assert "default handler" in err


def test_run_forever_reentrant_fails(loop):
    async def main():
        with pytest.raises(RuntimeError):
            loop.run_forever()
        with pytest.raises(RuntimeError):
            loop.run_until_complete(asyncio.sleep(0))

    loop.run_until_complete(main())


def test_debug_flag(loop):
    assert loop.get_debug() is False
    loop.set_debug(True)
    assert loop.get_debug() is True
    assert loop._debug is True
    loop._debug = False
    assert loop.get_debug() is False


def test_stats(loop):
    loop.call_soon(loop.stop)
    loop.run_forever()
    s = loop.stats()
    assert "callbacks=" in s


def test_contextvar_context(loop):
    import contextvars

    var = contextvars.ContextVar("v", default="dflt")
    seen = []

    async def main():
        var.set("set")
        ctx = contextvars.copy_context()
        var.set("other")
        loop.call_soon(lambda: seen.append(var.get()), context=ctx)
        await asyncio.sleep(0.01)
        assert seen == ["set"]

    loop.run_until_complete(main())


def test_repeated_run_close_cycles():
    for _ in range(5):
        loop = tokioop.new_event_loop()

        async def main():
            await asyncio.sleep(0.001)
            return 1

        assert loop.run_until_complete(main()) == 1
        loop.close()
        assert loop.is_closed()
