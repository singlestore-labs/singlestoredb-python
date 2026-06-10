"""Tests for the async UDF persistent per-thread event loop."""
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
from ..functions.ext.asgi import _get_thread_loop
from ..functions.ext.asgi import _run_on_thread_loop
from ..functions.ext.asgi import Application
from ..functions.ext.asgi import to_thread


class TestUDFDispatchEdgeCases(unittest.TestCase):
    """Test edge cases in the UDF dispatch stack."""

    def test_timeout_cancels_running_function(self) -> None:
        """Cancel event set from timer thread cancels a blocked coroutine."""
        cancel_event = threading.Event()

        async def long_running() -> str:
            await asyncio.sleep(999)
            return 'should not reach'

        def set_cancel_after_delay() -> None:
            time.sleep(0.2)
            cancel_event.set()

        timer = threading.Thread(target=set_cancel_after_delay)
        timer.start()

        start = time.monotonic()
        with self.assertRaises(asyncio.CancelledError):
            _run_on_thread_loop(
                _cancellable_run(cancel_event, long_running()),
            )
        elapsed = time.monotonic() - start
        timer.join()
        # 0.2s delay + up to 0.1s poll interval + margin
        self.assertLess(elapsed, 0.5)

    def test_exception_propagates_through_full_stack(self) -> None:
        """User exception propagates unwrapped through the entire dispatch."""
        cancel_event = threading.Event()

        class CustomUDFError(Exception):
            pass

        async def failing_udf() -> None:
            raise CustomUDFError('embedding service unavailable')

        with self.assertRaises(CustomUDFError) as ctx:
            _run_on_thread_loop(
                _cancellable_run(cancel_event, failing_udf()),
            )
        self.assertEqual(str(ctx.exception), 'embedding service unavailable')

    def test_cancel_event_detected_within_poll_interval(self) -> None:
        """Cancellation is detected within one poll cycle (0.1s)."""
        cancel_event = threading.Event()

        async def blocked() -> str:
            await asyncio.sleep(999)
            return 'unreachable'

        def set_cancel() -> None:
            time.sleep(0.05)
            cancel_event.set()

        timer = threading.Thread(target=set_cancel)
        timer.start()

        start = time.monotonic()
        with self.assertRaises(asyncio.CancelledError):
            _run_on_thread_loop(
                _cancellable_run(cancel_event, blocked()),
            )
        elapsed = time.monotonic() - start
        timer.join()
        # 0.05s delay + 0.1s poll interval + margin
        self.assertLess(elapsed, 0.25)

    def test_context_vars_propagate_through_to_thread(self) -> None:
        """Context variables are visible inside to_thread executor."""
        test_var: contextvars.ContextVar[str] = contextvars.ContextVar(
            'test_var',
        )
        test_var.set('hello_from_parent')
        captured: List[str] = []

        def read_context_var() -> str:
            val = test_var.get('NOT_FOUND')
            captured.append(val)
            return val

        async def run_in_thread() -> str:
            return await to_thread(read_context_var)

        result = _run_on_thread_loop(run_in_thread())
        self.assertEqual(result, 'hello_from_parent')
        self.assertEqual(captured, ['hello_from_parent'])

    def test_concurrent_requests_isolated(self) -> None:
        """Parallel executions don't share state."""
        results: List[Any] = [None, None, None]

        def run_isolated(index: int) -> None:
            async def compute() -> int:
                await asyncio.sleep(0.05)
                return index * 10

            results[index] = _run_on_thread_loop(compute())

        threads = [
            threading.Thread(target=run_isolated, args=(i,))
            for i in range(3)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(results, [0, 10, 20])

    def test_sync_function_through_async_wrapper(self) -> None:
        """Synchronous function works when wrapped as async coroutine."""
        cancel_event = threading.Event()

        async def sync_as_async() -> int:
            # Simulates what decorator.py's async_wrapper does for sync UDFs
            return 42 + 1

        result = _run_on_thread_loop(
            _cancellable_run(cancel_event, sync_as_async()),
        )
        self.assertEqual(result, 43)

    def test_cancel_event_not_set_on_success(self) -> None:
        """Cancel event remains unset after successful execution."""
        cancel_event = threading.Event()

        async def quick() -> str:
            return 'fast'

        result = _run_on_thread_loop(
            _cancellable_run(cancel_event, quick()),
        )
        self.assertEqual(result, 'fast')
        self.assertFalse(cancel_event.is_set())


class TestRunOnThreadLoop(unittest.TestCase):
    """Test _run_on_thread_loop reuses a persistent per-thread event loop."""

    def test_basic_coroutine(self) -> None:
        async def simple() -> int:
            return 42

        self.assertEqual(_run_on_thread_loop(simple()), 42)

    def test_loop_reused_across_calls(self) -> None:
        """The same loop object is reused for successive calls in a thread."""
        loops: List[asyncio.AbstractEventLoop] = []

        async def capture_loop() -> bool:
            loops.append(asyncio.get_running_loop())
            return True

        _run_on_thread_loop(capture_loop())
        _run_on_thread_loop(capture_loop())

        self.assertIs(loops[0], loops[1])

    def test_loop_not_closed_between_calls(self) -> None:
        """The persistent loop stays open so resources survive requests."""
        captured: List[asyncio.AbstractEventLoop] = []

        async def capture_loop() -> bool:
            captured.append(asyncio.get_running_loop())
            return True

        _run_on_thread_loop(capture_loop())
        loop = captured[0]
        self.assertFalse(loop.is_closed())

        # Still usable for the next request.
        _run_on_thread_loop(capture_loop())
        self.assertFalse(loop.is_closed())

    def test_async_resource_survives_between_calls(self) -> None:
        """An object bound to the loop can be reused on the next call.

        This mirrors caching e.g. an httpx.AsyncClient keyed by the loop and
        reusing its connection pool on subsequent requests.
        """
        clients: dict = {}

        async def get_or_create_client() -> int:
            loop = asyncio.get_running_loop()
            if loop not in clients:
                clients[loop] = object()
            return id(clients[loop])

        first = _run_on_thread_loop(get_or_create_client())
        second = _run_on_thread_loop(get_or_create_client())

        self.assertEqual(first, second)
        self.assertEqual(len(clients), 1)

    def test_separate_threads_get_separate_loops(self) -> None:
        """Each worker thread owns its own persistent loop."""
        loops: List[asyncio.AbstractEventLoop] = []
        lock = threading.Lock()

        def run_in_thread() -> None:
            async def capture() -> bool:
                with lock:
                    loops.append(asyncio.get_running_loop())
                return True

            _run_on_thread_loop(capture())

        threads = [threading.Thread(target=run_in_thread) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(loops), 3)
        self.assertEqual(len({id(loop) for loop in loops}), 3)

    def test_get_thread_loop_idempotent(self) -> None:
        """_get_thread_loop returns the same loop on repeated calls."""
        def run_in_thread(out: List[asyncio.AbstractEventLoop]) -> None:
            out.append(_get_thread_loop())
            out.append(_get_thread_loop())

        out: List[asyncio.AbstractEventLoop] = []
        t = threading.Thread(target=run_in_thread, args=(out,))
        t.start()
        t.join()

        self.assertIs(out[0], out[1])

    def test_exception_propagates(self) -> None:
        async def failing() -> None:
            raise ValueError('test error')

        with self.assertRaises(ValueError) as ctx:
            _run_on_thread_loop(failing())
        self.assertEqual(str(ctx.exception), 'test error')

    def test_cancellable_run_integration(self) -> None:
        """_cancellable_run works on the persistent loop."""
        cancel_event = threading.Event()

        async def slow_func() -> str:
            return 'completed'

        result = _run_on_thread_loop(
            _cancellable_run(cancel_event, slow_func()),
        )
        self.assertEqual(result, 'completed')

    def test_cancellation_via_event(self) -> None:
        """Cancellation propagates through the persistent-loop stack."""
        cancel_event = threading.Event()
        cancel_event.set()

        async def blocked_func() -> str:
            await asyncio.sleep(999)
            return 'should not reach'

        with self.assertRaises(asyncio.CancelledError):
            _run_on_thread_loop(
                _cancellable_run(cancel_event, blocked_func()),
            )

        # Loop must remain usable after a cancelled request.
        async def quick() -> str:
            return 'ok'

        self.assertEqual(
            _run_on_thread_loop(_cancellable_run(threading.Event(), quick())),
            'ok',
        )


class TestAsyncDispatchLoop(unittest.TestCase):
    """All async UDF dispatches share a single dedicated event-loop thread.

    The dispatch loop is process-global and lazily started; resources bound
    to it (HTTP pools, async clients, connection caches) are reused across
    every async UDF request. New requests are scheduled immediately and run
    concurrently on that loop instead of being serialized behind earlier
    in-flight requests.
    """

    def test_dispatch_loop_is_single_dedicated_thread(self) -> None:
        """All dispatches run on the same dedicated thread (not the caller)."""
        seen_threads: Set[int] = set()

        async def capture() -> int:
            seen_threads.add(threading.get_ident())
            return 1

        async def run_many() -> None:
            await asyncio.gather(*[
                _dispatch_to_async_loop(capture()) for _ in range(8)
            ])

        caller_thread = threading.get_ident()
        asyncio.run(run_many())

        self.assertEqual(len(seen_threads), 1)
        self.assertNotIn(caller_thread, seen_threads)
        # The thread we observed is the singleton dispatch thread.
        dispatch_thread = _get_async_dispatch_thread()
        assert dispatch_thread is not None
        self.assertEqual(seen_threads.pop(), dispatch_thread.ident)

    def test_dispatch_loop_is_single_event_loop(self) -> None:
        """All dispatches run on the SAME event loop instance."""
        captured: List[asyncio.AbstractEventLoop] = []

        async def capture() -> int:
            captured.append(asyncio.get_running_loop())
            return 1

        async def run_many() -> None:
            await asyncio.gather(*[
                _dispatch_to_async_loop(capture()) for _ in range(5)
            ])

        asyncio.run(run_many())

        self.assertEqual(len(captured), 5)
        first = captured[0]
        for loop in captured:
            self.assertIs(loop, first)
        self.assertIs(first, _get_async_dispatch_loop())

    def test_concurrent_dispatches_do_not_serialize(self) -> None:
        """Slow dispatches run in parallel on the loop; new requests do not
        wait for earlier ones to finish."""
        n = 6
        per_call_sleep = 0.3

        async def slow() -> str:
            await asyncio.sleep(per_call_sleep)
            return 'done'

        async def run_many() -> List[str]:
            return await asyncio.gather(*[
                _dispatch_to_async_loop(slow()) for _ in range(n)
            ])

        start = time.monotonic()
        results = asyncio.run(run_many())
        elapsed = time.monotonic() - start

        self.assertEqual(results, ['done'] * n)
        # Serialized would be ~ n * per_call_sleep. Parallel ~ per_call_sleep.
        # Allow generous margin for CI noise.
        self.assertLess(elapsed, per_call_sleep * 2)

    def test_new_request_does_not_wait_for_in_flight_request(self) -> None:
        """A new async request is submitted to the dispatch thread
        immediately and runs while an earlier request is still in-flight.

        This is the explicit guarantee that async UDF dispatch is not
        serialized: a request fired AFTER another long one has started
        must (a) start before the long one finishes, (b) finish before
        the long one finishes, and (c) be submitted with negligible
        latency from the caller's perspective.
        """
        long_sleep = 1.0
        ts: Dict[str, float] = {}
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

        async def quick() -> str:
            ts['quick_started'] = time.monotonic()
            await asyncio.sleep(0)
            ts['quick_finished'] = time.monotonic()
            return 'quick'

        async def driver() -> None:
            long_task = asyncio.create_task(
                _dispatch_to_async_loop(long_running()),
            )
            # Wait until the long task has actually started on the
            # dispatch loop. Only after this point can we be sure the
            # next dispatch is "during" an in-flight request.
            for _ in range(100):
                await asyncio.sleep(0.01)
                if 'started' in signals and signals['started'].is_set():
                    break
            self.assertIn('long_started', ts)

            ts['quick_dispatch_called'] = time.monotonic()
            quick_result = await _dispatch_to_async_loop(quick())
            ts['quick_dispatch_returned'] = time.monotonic()
            self.assertEqual(quick_result, 'quick')

            long_result = await long_task
            self.assertEqual(long_result, 'long')

        asyncio.run(driver())

        # The new request actually overlapped the in-flight one.
        self.assertGreater(ts['quick_started'], ts['long_started'])
        self.assertLess(ts['quick_started'], ts['long_finished'])
        self.assertLess(ts['quick_finished'], ts['long_finished'])

        # Submission of the new request to the dispatch thread is
        # non-blocking: the awaiter returned in well under the long
        # request's remaining time.
        dispatch_latency = ts['quick_dispatch_returned'] \
            - ts['quick_dispatch_called']
        self.assertLess(dispatch_latency, long_sleep / 2)

    def test_many_new_requests_run_during_one_in_flight_request(self) -> None:
        """Many new async requests, each fired sequentially while a single
        long-running request is in-flight, all start AND finish before the
        long one finishes."""
        long_sleep = 1.0
        n_quick = 8
        ts: Dict[str, float] = {}
        quick_finished: List[float] = []
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
            # Wait for the long task to start.
            for _ in range(100):
                await asyncio.sleep(0.01)
                if 'started' in signals and signals['started'].is_set():
                    break

            results = await asyncio.gather(*[
                _dispatch_to_async_loop(quick(i)) for i in range(n_quick)
            ])
            self.assertEqual(results, list(range(n_quick)))
            await long_task

        asyncio.run(driver())

        # All quick requests finished before the long one did, proving
        # they were not queued behind it.
        self.assertEqual(len(quick_finished), n_quick)
        for finish in quick_finished:
            self.assertLess(finish, ts['long_finished'])
            self.assertGreater(finish, ts['long_started'])

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

    def test_dispatch_with_cancel_event(self) -> None:
        """`_cancellable_run` on the dispatch loop honors the cancel event."""
        cancel_event = threading.Event()

        async def blocked() -> str:
            await asyncio.sleep(999)
            return 'unreachable'

        def trip_cancel() -> None:
            time.sleep(0.1)
            cancel_event.set()

        timer = threading.Thread(target=trip_cancel)
        timer.start()

        async def driver() -> None:
            await _dispatch_to_async_loop(
                _cancellable_run(cancel_event, blocked()),
            )

        start = time.monotonic()
        with self.assertRaises(asyncio.CancelledError):
            asyncio.run(driver())
        elapsed = time.monotonic() - start
        timer.join()
        # 0.1s delay + 0.1s poll interval + margin
        self.assertLess(elapsed, 0.5)

    def test_dispatch_loop_survives_after_cancellation(self) -> None:
        """The dispatch loop remains usable after a cancelled request."""
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
# Per-tag start / finish timestamps, used by the "no waiting for in-flight"
# test below to assert overlap between concurrent requests.
_dispatch_started_at: Dict[str, float] = {}
_dispatch_finished_at: Dict[str, float] = {}


def _record(tag: str) -> None:
    with _dispatch_observation_lock:
        _dispatch_observation[tag] = threading.get_ident()
        _dispatch_started_at[tag] = time.monotonic()


def _record_finish(tag: str) -> None:
    with _dispatch_observation_lock:
        _dispatch_finished_at[tag] = time.monotonic()


@udf
async def _async_record_udf(tag: str) -> int:
    _record(tag)
    await asyncio.sleep(0)
    _record_finish(tag)
    return len(tag)


@udf
async def _async_slow_udf(tag: str) -> int:
    _record(tag)
    await asyncio.sleep(0.4)
    _record_finish(tag)
    return len(tag)


@udf
async def _async_long_udf(tag: str) -> int:
    """Long-running async UDF used to verify that newly arriving async
    requests do not have to wait for it to finish."""
    _record(tag)
    await asyncio.sleep(1.0)
    _record_finish(tag)
    return len(tag)


@udf
def _sync_record_udf(tag: str) -> int:
    _record(tag)
    _record_finish(tag)
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
        _dispatch_started_at.clear()
        _dispatch_finished_at.clear()


class TestApplicationDispatchRouting(unittest.TestCase):
    """End-to-end: Application routes async UDFs to the dispatch loop and
    sync UDFs to a worker thread, and concurrent async requests run in
    parallel on the dispatch loop."""

    def setUp(self) -> None:
        _reset_dispatch_observation()
        self.app = Application(
            functions=[
                _async_record_udf,
                _async_slow_udf,
                _async_long_udf,
                _sync_record_udf,
            ],
            disable_metrics=True,
        )

    @staticmethod
    def _headers_dict(scope: Dict[str, Any]) -> Dict[bytes, bytes]:
        return {k: v for k, v in scope['headers']}

    def _invoke(self, name: str, rows: List[Tuple[Any, ...]]) -> List[Dict[str, Any]]:
        scope, receive, send, sent = _make_invoke_args(name, rows)
        scope['headers'] = list(scope['headers'])
        # Application reads headers as a dict via ``dict(scope['headers'])``,
        # which works for our list of tuples.
        asyncio.run(self.app(scope, receive, send))
        return sent

    async def _invoke_async(
        self, name: str, rows: List[Tuple[Any, ...]],
    ) -> List[Dict[str, Any]]:
        scope, receive, send, sent = _make_invoke_args(name, rows)
        scope['headers'] = list(scope['headers'])
        await self.app(scope, receive, send)
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
        """A sync UDF body runs on a worker thread, NOT the dispatch thread."""
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

    def test_concurrent_async_requests_share_dispatch_thread(self) -> None:
        """Two concurrent async UDF requests both execute on the dispatch thread."""

        async def driver() -> None:
            await asyncio.gather(
                self._invoke_async('_async_record_udf', [('one',)]),
                self._invoke_async('_async_record_udf', [('two',)]),
                self._invoke_async('_async_record_udf', [('three',)]),
            )

        asyncio.run(driver())

        dispatch_thread = _get_async_dispatch_thread()
        assert dispatch_thread is not None
        with _dispatch_observation_lock:
            for tag in ('one', 'two', 'three'):
                self.assertEqual(
                    _dispatch_observation[tag], dispatch_thread.ident,
                    f'tag {tag} ran on wrong thread',
                )

    def test_concurrent_async_requests_do_not_serialize(self) -> None:
        """Concurrent async UDF requests run in parallel on the dispatch loop;
        a new request does not wait for in-flight ones."""
        n = 4
        per_call_sleep = 0.4

        async def driver() -> None:
            await asyncio.gather(*[
                self._invoke_async('_async_slow_udf', [(f'r{i}',)])
                for i in range(n)
            ])

        start = time.monotonic()
        asyncio.run(driver())
        elapsed = time.monotonic() - start

        # Serialized would be ~ n * per_call_sleep. Parallel ~ per_call_sleep.
        self.assertLess(elapsed, per_call_sleep * 2)

    def test_new_async_request_runs_during_in_flight_request(self) -> None:
        """An async request arriving while another is still running gets
        dispatched onto the async thread immediately and finishes before
        the in-flight one — i.e., a new request does not wait for any
        existing async request to be served."""

        async def driver() -> None:
            long_call = asyncio.create_task(
                self._invoke_async('_async_long_udf', [('long',)]),
            )
            # Spin until the long request has actually started executing
            # on the dispatch thread, so any new dispatch we fire after
            # this point is genuinely "during" an in-flight request.
            for _ in range(200):
                await asyncio.sleep(0.01)
                with _dispatch_observation_lock:
                    if 'long' in _dispatch_started_at:
                        break
            self.assertIn('long', _dispatch_started_at)

            t_call = time.monotonic()
            await self._invoke_async('_async_record_udf', [('quick',)])
            t_returned = time.monotonic()
            await long_call

        asyncio.run(driver())

        with _dispatch_observation_lock:
            long_started = _dispatch_started_at['long']
            long_finished = _dispatch_finished_at['long']
            quick_started = _dispatch_started_at['quick']
            quick_finished = _dispatch_finished_at['quick']

        # quick must have started AFTER long started (it was fired later)
        # but BEFORE long finished, and itself finished before long did.
        self.assertGreater(quick_started, long_started)
        self.assertLess(quick_started, long_finished)
        self.assertLess(quick_finished, long_finished)

        # Sanity: the long UDF body really did span the long sleep.
        self.assertGreaterEqual(long_finished - long_started, 0.9)


if __name__ == '__main__':
    unittest.main()
