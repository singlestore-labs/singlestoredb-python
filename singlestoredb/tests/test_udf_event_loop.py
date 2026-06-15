"""Tests for the dedicated async UDF dispatch event loop."""
import asyncio
import contextvars
import json as jsonlib
import threading
import time
import unittest
from typing import Any
from typing import Dict
from typing import List
from typing import Set
from typing import Tuple

from ..functions import udf
from ..functions.ext.asgi import _cancellable_run
from ..functions.ext.asgi import _dispatch_to_async_loop
from ..functions.ext.asgi import _get_async_dispatch_loop
from ..functions.ext.asgi import _get_async_dispatch_thread
from ..functions.ext.asgi import Application
from ..functions.ext.asgi import to_thread


class TestCancellableRun(unittest.TestCase):
    """Unit tests for ``_cancellable_run`` and the ``to_thread`` helper."""

    def test_cancel_event_cancels_blocked_coroutine(self) -> None:
        """Tripping ``cancel_event`` interrupts a coroutine blocked on I/O.

        The coroutine sleeps far longer than the test could tolerate, so the
        test only completes if the cancel signal actually unblocks it.
        """
        cancel_event = threading.Event()

        async def blocked() -> str:
            await asyncio.sleep(999)
            return 'unreachable'

        def trip_cancel_soon() -> None:
            time.sleep(0.1)
            cancel_event.set()

        timer = threading.Thread(target=trip_cancel_soon)
        timer.start()
        try:
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(_cancellable_run(cancel_event, blocked()))
        finally:
            timer.join()

    def test_exception_propagates_unwrapped(self) -> None:
        """A user exception surfaces unchanged through ``_cancellable_run``."""
        cancel_event = threading.Event()

        class CustomUDFError(Exception):
            pass

        async def failing_udf() -> None:
            raise CustomUDFError('embedding service unavailable')

        with self.assertRaises(CustomUDFError) as ctx:
            asyncio.run(_cancellable_run(cancel_event, failing_udf()))
        self.assertEqual(str(ctx.exception), 'embedding service unavailable')

    def test_successful_run_returns_result_and_leaves_event_unset(self) -> None:
        """A successful run returns its value without tripping the event."""
        cancel_event = threading.Event()

        async def quick() -> int:
            return 42 + 1

        result = asyncio.run(_cancellable_run(cancel_event, quick()))
        self.assertEqual(result, 43)
        self.assertFalse(cancel_event.is_set())

    def test_context_vars_propagate_through_to_thread(self) -> None:
        """Context variables are visible inside the ``to_thread`` executor."""
        test_var: contextvars.ContextVar[str] = contextvars.ContextVar(
            'test_var',
        )
        test_var.set('hello_from_parent')

        def read_context_var() -> str:
            return test_var.get('NOT_FOUND')

        async def run_in_thread() -> str:
            return await to_thread(read_context_var)

        self.assertEqual(asyncio.run(run_in_thread()), 'hello_from_parent')


class TestAsyncDispatchLoop(unittest.TestCase):
    """All async UDF dispatches share a single dedicated event-loop thread.

    The dispatch loop is process-global and lazily started; resources bound
    to it (HTTP pools, async clients, connection caches) are reused across
    every async UDF request. New requests are scheduled immediately and run
    concurrently on that loop instead of being serialized behind earlier
    in-flight requests.
    """

    def test_dispatch_uses_single_dedicated_thread_and_loop(self) -> None:
        """Every dispatch runs on the one dedicated thread/loop, never the
        caller's thread."""
        seen_threads: Set[int] = set()
        seen_loops: List[asyncio.AbstractEventLoop] = []

        async def capture() -> int:
            seen_threads.add(threading.get_ident())
            seen_loops.append(asyncio.get_running_loop())
            return 1

        async def run_many() -> None:
            await asyncio.gather(
                *[
                    _dispatch_to_async_loop(capture()) for _ in range(8)
                ],
            )

        caller_thread = threading.get_ident()
        asyncio.run(run_many())

        # One dedicated thread, distinct from the caller, and it is the
        # singleton dispatch thread.
        self.assertEqual(len(seen_threads), 1)
        self.assertNotIn(caller_thread, seen_threads)
        dispatch_thread = _get_async_dispatch_thread()
        assert dispatch_thread is not None
        self.assertEqual(seen_threads.pop(), dispatch_thread.ident)

        # Every coroutine observed the same loop, and it is the singleton.
        self.assertEqual(len(seen_loops), 8)
        for loop in seen_loops:
            self.assertIs(loop, seen_loops[0])
        self.assertIs(seen_loops[0], _get_async_dispatch_loop())

    def test_new_requests_run_during_one_in_flight_request(self) -> None:
        """Requests fired while a long one is in-flight all start AND finish
        before it does, proving they are not serialized behind it.

        Assertions compare event ordering (relative timestamps) rather than
        absolute wall-clock durations, so they are robust to CI load.
        """
        long_sleep = 1.0
        n_quick = 8
        ts: Dict[str, float] = {}
        quick_finished: List[float] = []
        # Created lazily on the dispatch loop so the asyncio.Event is bound
        # to the correct loop.
        signals: Dict[str, asyncio.Event] = {}

        async def long_running() -> str:
            ts['long_started'] = time.monotonic()
            signals['started'] = asyncio.Event()
            signals['started'].set()
            await asyncio.sleep(long_sleep)
            ts['long_finished'] = time.monotonic()
            return 'long'

        async def quick(i: int) -> int:
            await asyncio.sleep(0.01)
            quick_finished.append(time.monotonic())
            return i

        async def driver() -> None:
            long_task = asyncio.create_task(
                _dispatch_to_async_loop(long_running()),
            )
            # Wait until the long task is actually running on the dispatch
            # loop before firing the others.
            for _ in range(100):
                await asyncio.sleep(0.01)
                if 'started' in signals and signals['started'].is_set():
                    break

            results = await asyncio.gather(
                *[
                    _dispatch_to_async_loop(quick(i)) for i in range(n_quick)
                ],
            )
            self.assertEqual(results, list(range(n_quick)))
            await long_task

        asyncio.run(driver())

        # All quick requests finished between the long request's start and
        # finish, proving they were not queued behind it.
        self.assertEqual(len(quick_finished), n_quick)
        for finish in quick_finished:
            self.assertGreater(finish, ts['long_started'])
            self.assertLess(finish, ts['long_finished'])

    def test_loop_bound_resource_reused_across_dispatches(self) -> None:
        """A resource keyed by id(loop) is shared by every async request,
        even across separate caller event loops (separate parent runs)."""
        cache: Dict[int, object] = {}

        async def acquire() -> int:
            loop = asyncio.get_running_loop()
            key = id(loop)
            if key not in cache:
                cache[key] = object()
            return id(cache[key])

        async def run_one() -> int:
            return await _dispatch_to_async_loop(acquire())

        first = asyncio.run(run_one())
        second = asyncio.run(run_one())
        third = asyncio.run(run_one())

        self.assertEqual(first, second)
        self.assertEqual(second, third)
        self.assertEqual(len(cache), 1)

    def test_dispatch_propagates_exception(self) -> None:
        """Exceptions from the dispatched coroutine surface to the caller."""
        class DispatchedError(Exception):
            pass

        async def failing() -> None:
            raise DispatchedError('boom')

        async def driver() -> None:
            await _dispatch_to_async_loop(failing())

        with self.assertRaises(DispatchedError) as ctx:
            asyncio.run(driver())
        self.assertEqual(str(ctx.exception), 'boom')

    def test_dispatch_loop_survives_after_cancellation(self) -> None:
        """A cancelled dispatch (via cancel_event) cancels the work on the
        loop, and the loop stays usable for later requests."""
        cancel_event = threading.Event()
        cancel_event.set()

        async def blocked() -> str:
            await asyncio.sleep(999)
            return 'unreachable'

        async def driver_cancel() -> None:
            await _dispatch_to_async_loop(
                _cancellable_run(cancel_event, blocked()),
            )

        with self.assertRaises(asyncio.CancelledError):
            asyncio.run(driver_cancel())

        async def quick() -> str:
            return 'ok'

        async def driver_ok() -> str:
            return await _dispatch_to_async_loop(quick())

        self.assertEqual(asyncio.run(driver_ok()), 'ok')


# Module-level UDFs used by the Application integration tests below. They
# must be defined at module scope so the signature inspection helpers can
# resolve their type hints.

# Records the thread that actually executes each UDF body, keyed by tag.
_dispatch_observation: Dict[str, int] = {}
_dispatch_observation_lock = threading.Lock()


def _record(tag: str) -> None:
    with _dispatch_observation_lock:
        _dispatch_observation[tag] = threading.get_ident()


@udf
async def _async_record_udf(tag: str) -> int:
    _record(tag)
    await asyncio.sleep(0)
    return len(tag)


@udf
def _sync_record_udf(tag: str) -> int:
    _record(tag)
    return len(tag)


def _make_invoke_args(
    name: str,
    rows: List[Tuple[Any, ...]],
) -> Tuple[Dict[str, Any], Any, Any, List[Dict[str, Any]]]:
    """Build a minimal ASGI scope/receive/send for an /invoke request."""
    payload = jsonlib.dumps({
        'data': [[i, *row] for i, row in enumerate(rows)],
    }).encode('utf-8')

    received: Dict[str, bool] = {'sent': False}

    async def receive() -> Dict[str, Any]:
        if received['sent']:
            await asyncio.sleep(60)
            return {'type': 'http.disconnect'}
        received['sent'] = True
        return {'type': 'http.request', 'body': payload, 'more_body': False}

    sent: List[Dict[str, Any]] = []

    async def send(msg: Dict[str, Any]) -> None:
        sent.append(msg)

    scope = {
        'type': 'http',
        'method': 'POST',
        'path': '/invoke',
        'scheme': 'http',
        'headers': [
            (b'content-type', b'application/json'),
            (b'accepts', b'application/json'),
            (b's2-ef-name', name.encode('utf-8')),
            (b's2-ef-version', b'1.0'),
            (b's2-ef-ignore-cancel', b'true'),
        ],
    }
    return scope, receive, send, sent


def _reset_dispatch_observation() -> None:
    with _dispatch_observation_lock:
        _dispatch_observation.clear()


class TestApplicationDispatchRouting(unittest.TestCase):
    """End-to-end: ``Application`` routes async UDFs to the dispatch loop and
    sync UDFs to a worker thread."""

    def setUp(self) -> None:
        _reset_dispatch_observation()
        self.app = Application(
            functions=[
                _async_record_udf,
                _sync_record_udf,
            ],
            disable_metrics=True,
        )

    def _invoke(self, name: str, rows: List[Tuple[Any, ...]]) -> List[Dict[str, Any]]:
        scope, receive, send, sent = _make_invoke_args(name, rows)
        scope['headers'] = list(scope['headers'])
        asyncio.run(self.app(scope, receive, send))
        return sent

    def test_async_udf_runs_on_dispatch_thread(self) -> None:
        """An async UDF body executes on the dedicated dispatch thread."""
        sent = self._invoke('_async_record_udf', [('alpha',)])
        statuses = [m for m in sent if m.get('type') == 'http.response.start']
        self.assertTrue(statuses and statuses[0]['status'] == 200, sent)

        dispatch_thread = _get_async_dispatch_thread()
        assert dispatch_thread is not None
        with _dispatch_observation_lock:
            self.assertEqual(_dispatch_observation['alpha'], dispatch_thread.ident)

    def test_sync_udf_runs_on_a_worker_thread_not_dispatch(self) -> None:
        """A sync UDF body runs on a worker thread, NOT the dispatch thread
        and NOT the caller thread."""
        # Force the dispatch thread to exist so we can compare ids.
        _get_async_dispatch_loop()
        dispatch_thread = _get_async_dispatch_thread()
        assert dispatch_thread is not None

        sent = self._invoke('_sync_record_udf', [('beta',)])
        statuses = [m for m in sent if m.get('type') == 'http.response.start']
        self.assertTrue(statuses and statuses[0]['status'] == 200, sent)

        with _dispatch_observation_lock:
            sync_thread = _dispatch_observation['beta']

        self.assertNotEqual(sync_thread, threading.get_ident())
        self.assertNotEqual(sync_thread, dispatch_thread.ident)


if __name__ == '__main__':
    unittest.main()
