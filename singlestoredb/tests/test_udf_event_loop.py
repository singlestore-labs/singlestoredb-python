"""Tests for async UDF event loop graceful shutdown."""
import asyncio
import contextvars
import threading
import time
import unittest
from typing import Any
from typing import List

from ..functions.ext.asgi import _cancellable_run
from ..functions.ext.asgi import _run_with_graceful_shutdown
from ..functions.ext.asgi import to_thread


class TestRunWithGracefulShutdown(unittest.TestCase):
    """Test _run_with_graceful_shutdown handles loop cleanup properly."""

    def test_basic_coroutine(self) -> None:
        async def simple() -> int:
            return 42

        result = _run_with_graceful_shutdown(simple())
        self.assertEqual(result, 42)

    def test_callbacks_drained_before_close(self) -> None:
        """Simulate httpx/anyio scheduling call_soon during teardown.

        This is the exact pattern that causes 'Event loop is closed' with
        asyncio.run() -- a library schedules a callback in its __del__ or
        aclose() that fires after the loop is closed.
        """
        callback_executed: List[bool] = []

        async def coroutine_with_cleanup_callback() -> str:
            loop = asyncio.get_running_loop()
            loop.call_soon(lambda: callback_executed.append(True))
            return 'done'

        result = _run_with_graceful_shutdown(coroutine_with_cleanup_callback())
        self.assertEqual(result, 'done')
        self.assertEqual(callback_executed, [True])

    def test_no_event_loop_closed_error(self) -> None:
        """Verify no RuntimeError when cleanup schedules on the loop."""
        errors: List[RuntimeError] = []

        async def simulate_httpx_teardown() -> str:
            loop = asyncio.get_running_loop()

            def deferred_cleanup() -> None:
                try:
                    loop.call_soon(lambda: None)
                except RuntimeError as e:
                    errors.append(e)

            loop.call_soon(deferred_cleanup)
            return 'ok'

        result = _run_with_graceful_shutdown(simulate_httpx_teardown())
        self.assertEqual(result, 'ok')
        self.assertEqual(errors, [])

    def test_exception_propagates(self) -> None:
        async def failing() -> None:
            raise ValueError('test error')

        with self.assertRaises(ValueError) as ctx:
            _run_with_graceful_shutdown(failing())
        self.assertEqual(str(ctx.exception), 'test error')

    def test_callbacks_drained_even_on_exception(self) -> None:
        """Cleanup callbacks still run even if coroutine raises."""
        callback_executed: List[bool] = []

        async def failing_with_callback() -> None:
            loop = asyncio.get_running_loop()
            loop.call_soon(lambda: callback_executed.append(True))
            raise ValueError('boom')

        with self.assertRaises(ValueError):
            _run_with_graceful_shutdown(failing_with_callback())
        self.assertEqual(callback_executed, [True])

    def test_pending_tasks_cancelled(self) -> None:
        """Background tasks are cancelled during shutdown."""
        async def background() -> None:
            await asyncio.sleep(999)

        async def main_with_background_task() -> str:
            asyncio.create_task(background())
            return 'done'

        result = _run_with_graceful_shutdown(main_with_background_task())
        self.assertEqual(result, 'done')

    def test_isolation_between_calls(self) -> None:
        """Each call gets its own event loop that is closed after use."""
        loops: List[asyncio.AbstractEventLoop] = []

        async def capture_loop() -> bool:
            loops.append(asyncio.get_running_loop())
            return True

        _run_with_graceful_shutdown(capture_loop())
        first_loop = loops[0]
        self.assertTrue(first_loop.is_closed())

        _run_with_graceful_shutdown(capture_loop())
        second_loop = loops[1]
        self.assertTrue(second_loop.is_closed())

    def test_cancellable_run_integration(self) -> None:
        """Verify _cancellable_run works inside _run_with_graceful_shutdown."""
        cancel_event = threading.Event()

        async def slow_func() -> str:
            return 'completed'

        result = _run_with_graceful_shutdown(
            _cancellable_run(cancel_event, slow_func()),
        )
        self.assertEqual(result, 'completed')

    def test_cancellation_via_event(self) -> None:
        """Verify cancellation propagates through the full stack."""
        cancel_event = threading.Event()
        cancel_event.set()

        async def blocked_func() -> str:
            await asyncio.sleep(999)
            return 'should not reach'

        with self.assertRaises(asyncio.CancelledError):
            _run_with_graceful_shutdown(
                _cancellable_run(cancel_event, blocked_func()),
            )


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
            _run_with_graceful_shutdown(
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
            _run_with_graceful_shutdown(
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
            _run_with_graceful_shutdown(
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

        result = _run_with_graceful_shutdown(run_in_thread())
        self.assertEqual(result, 'hello_from_parent')
        self.assertEqual(captured, ['hello_from_parent'])

    def test_concurrent_requests_isolated(self) -> None:
        """Parallel executions don't share state."""
        results: List[Any] = [None, None, None]

        def run_isolated(index: int) -> None:
            async def compute() -> int:
                await asyncio.sleep(0.05)
                return index * 10

            results[index] = _run_with_graceful_shutdown(compute())

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

        result = _run_with_graceful_shutdown(
            _cancellable_run(cancel_event, sync_as_async()),
        )
        self.assertEqual(result, 43)

    def test_cancel_event_not_set_on_success(self) -> None:
        """Cancel event remains unset after successful execution."""
        cancel_event = threading.Event()

        async def quick() -> str:
            return 'fast'

        result = _run_with_graceful_shutdown(
            _cancellable_run(cancel_event, quick()),
        )
        self.assertEqual(result, 'fast')
        self.assertFalse(cancel_event.is_set())

    def test_callbacks_from_cancelled_tasks_still_drain(self) -> None:
        """Background task callbacks drain even when task is cancelled."""
        drained: List[bool] = []

        async def bg_with_callback() -> None:
            loop = asyncio.get_running_loop()
            loop.call_soon(lambda: drained.append(True))
            await asyncio.sleep(999)

        async def main() -> str:
            asyncio.create_task(bg_with_callback())
            await asyncio.sleep(0.05)  # Let background task start
            return 'done'

        result = _run_with_graceful_shutdown(main())
        self.assertEqual(result, 'done')
        self.assertEqual(drained, [True])


if __name__ == '__main__':
    unittest.main()
