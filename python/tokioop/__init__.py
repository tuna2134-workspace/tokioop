"""tokioop: a Rust + Tokio backed asyncio event loop.

Usage::

    import asyncio
    import tokioop

    tokioop.install()

    async def main(): ...

    asyncio.run(main())
"""

import asyncio

from tokioop._tokioop import FdHandle, ReadyHandle, TimerHandle, TokioopLoop
from tokioop.loop import RustEventLoop
from tokioop.policy import RustEventLoopPolicy

__all__ = (
    "RustEventLoop",
    "RustEventLoopPolicy",
    "TokioopLoop",
    "TimerHandle",
    "ReadyHandle",
    "FdHandle",
    "install",
    "new_event_loop",
    "__version__",
)

__version__ = "0.1.0"


def new_event_loop():
    """Create a new :class:`RustEventLoop`."""
    return RustEventLoop()


def install(policy=None):
    """Install tokioop as the default asyncio event-loop backend.

    After this call, ``asyncio.run()``, ``asyncio.new_event_loop()`` and
    friends create :class:`RustEventLoop` instances. Existing asyncio
    application code needs no changes.
    """
    if policy is None:
        policy = RustEventLoopPolicy()
    asyncio.set_event_loop_policy(policy)
    return policy
