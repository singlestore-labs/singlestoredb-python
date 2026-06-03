"""Tests for the async UDF persistent per-thread event loop."""
import asyncio
import contextvars
import threading
import time
import unittest
from typing import Any
from typing import List

from ..functions.ext.asgi import _cancellable_run
from ..functions.ext.asgi import _get_thread_loop
from ..functions.ext.asgi import _run_on_thread_loop
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


if __name__ == '__main__':
    unittest.main()
