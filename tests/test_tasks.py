"""Tasks, futures, cancellation, timeouts, sync primitives."""

import asyncio

import pytest

import tokioop


@pytest.fixture(autouse=True)
def _install():
    tokioop.install()


def test_create_task(loop=None):
    async def main():
        async def coro(x):
            await asyncio.sleep(0)
            return x + 1

        t = asyncio.create_task(coro(1))
        assert isinstance(t, asyncio.Task)
        assert await t == 2

    asyncio.run(main())


def test_gather():
    async def main():
        async def f(x):
            await asyncio.sleep(x * 0.001)
            return x

        assert await asyncio.gather(f(1), f(2), f(3)) == [1, 2, 3]
        # exception propagation
        async def boom():
            raise RuntimeError("gather boom")

        with pytest.raises(RuntimeError, match="gather boom"):
            await asyncio.gather(f(1), boom())

    asyncio.run(main())


def test_gather_return_exceptions():
    async def main():
        async def boom():
            raise ValueError("x")

        res = await asyncio.gather(boom(), asyncio.sleep(0, result=5), return_exceptions=True)
        assert isinstance(res[0], ValueError)
        assert res[1] == 5

    asyncio.run(main())


def test_wait():
    async def main():
        async def f(x):
            await asyncio.sleep(0.001 * x)
            return x

        ts = [asyncio.create_task(f(i)) for i in range(5)]
        done, pending = await asyncio.wait(ts, timeout=1.0)
        assert len(done) == 5 and not pending

    asyncio.run(main())


def test_wait_for_ok():
    async def main():
        async def f():
            await asyncio.sleep(0.01)
            return "fast"

        assert await asyncio.wait_for(f(), timeout=1.0) == "fast"

    asyncio.run(main())


def test_wait_for_timeout():
    async def main():
        async def slow():
            await asyncio.sleep(10)
            return "slow"

        with pytest.raises(TimeoutError):
            await asyncio.wait_for(slow(), timeout=0.02)

    asyncio.run(main())


def test_task_cancel():
    async def main():
        cancelled = []

        async def worker():
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                cancelled.append(True)
                raise

        t = asyncio.create_task(worker())
        await asyncio.sleep(0.01)
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t
        assert cancelled == [True]
        assert t.cancelled()

    asyncio.run(main())


def test_future_cancel():
    async def main():
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        assert not fut.done()
        fut.cancel()
        assert fut.cancelled()
        with pytest.raises(asyncio.CancelledError):
            await fut

    asyncio.run(main())


def test_timeout_context():
    async def main():
        try:
            async with asyncio.timeout(0.02):
                await asyncio.sleep(10)
        except TimeoutError:
            return "timed-out"
        return "no-timeout"

    assert asyncio.run(main()) == "timed-out"


def test_shield():
    async def main():
        async def f():
            await asyncio.sleep(0.01)
            return 7

        inner = asyncio.create_task(f())
        assert await asyncio.shield(inner) == 7

    asyncio.run(main())


def test_queue():
    async def main():
        q = asyncio.Queue(maxsize=2)
        await q.put(1)
        await q.put(2)
        assert q.full()
        assert await q.get() == 1
        assert await q.get() == 2

        order = []

        async def producer():
            for i in range(5):
                await q.put(i)
                order.append(f"p{i}")

        async def consumer():
            for _ in range(5):
                order.append(f"c{await q.get()}")

        await asyncio.gather(producer(), consumer())
        assert len(order) == 10

    asyncio.run(main())


def test_event():
    async def main():
        ev = asyncio.Event()
        seen = []

        async def waiter():
            await ev.wait()
            seen.append(True)

        t = asyncio.create_task(waiter())
        await asyncio.sleep(0.01)
        assert seen == []
        ev.set()
        await t
        assert seen == [True]
        assert ev.is_set()

    asyncio.run(main())


def test_lock():
    async def main():
        lock = asyncio.Lock()
        order = []

        async def worker(i):
            async with lock:
                order.append(i)
                await asyncio.sleep(0.005)

        await asyncio.gather(*[worker(i) for i in range(5)])
        assert sorted(order) == list(range(5))

    asyncio.run(main())


def test_semaphore():
    async def main():
        sem = asyncio.Semaphore(2)
        active = 0
        peak = 0

        async def worker():
            nonlocal active, peak
            async with sem:
                active += 1
                peak = max(peak, active)
                await asyncio.sleep(0.005)
                active -= 1

        await asyncio.gather(*[worker() for _ in range(6)])
        assert peak <= 2

    asyncio.run(main())


def test_condition():
    async def main():
        cond = asyncio.Condition()
        items = []

        async def consumer():
            async with cond:
                await cond.wait_for(lambda: len(items) > 0)
                return items.pop(0)

        async def producer():
            await asyncio.sleep(0.01)
            async with cond:
                items.append(42)
                cond.notify_all()

        res, _ = await asyncio.gather(consumer(), producer())
        assert res == 42

    asyncio.run(main())


def test_task_factory():
    async def main():
        loop = asyncio.get_running_loop()
        seen = []

        def factory(loop, coro, **kwargs):
            seen.append(True)
            return asyncio.Task(coro, loop=loop, **kwargs)

        loop.set_task_factory(factory)
        t = loop.create_task(asyncio.sleep(0), name="custom")
        await t
        # stdlib parity: the factory is called as factory(loop, coro);
        # the name is applied afterwards via set_name()
        assert seen == [True]
        assert t.get_name() == "custom"
        loop.set_task_factory(None)
        assert loop.get_task_factory() is None

    asyncio.run(main())


def test_ensure_future_and_wrap():
    async def main():
        loop = asyncio.get_running_loop()
        t = asyncio.ensure_future(asyncio.sleep(0, result="ef"))
        assert await t == "ef"

        fut = loop.create_future()
        loop.call_soon(fut.set_result, "cf")
        assert await asyncio.ensure_future(fut) == "cf"

    asyncio.run(main())


def test_all_tasks_current_task():
    async def main():
        me = asyncio.current_task()
        assert me is not None
        others = asyncio.all_tasks()
        assert me in others

    asyncio.run(main())


def test_exception_in_task_goes_to_handler():
    errors = []

    async def main():
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(lambda ctx: errors.append(ctx))

        async def boom():
            raise RuntimeError("task boom")

        t = asyncio.create_task(boom())
        await asyncio.sleep(0.05)
        assert t.done()
        # unretrieved task exception -> exception handler
        del t
        import gc

        gc.collect()
        await asyncio.sleep(0.01)

    asyncio.run(main())
    assert any(
        "task boom" in str(e.get("exception", "")) for e in errors
    ), errors


def test_nested_run_fails():
    async def main():
        with pytest.raises(RuntimeError):
            asyncio.run(asyncio.sleep(0))

    asyncio.run(main())


def test_task_eager_start():
    async def main():
        loop = asyncio.get_running_loop()
        t = loop.create_task(asyncio.sleep(0, result="eager"), eager_start=True)
        assert await t == "eager"

    asyncio.run(main())
