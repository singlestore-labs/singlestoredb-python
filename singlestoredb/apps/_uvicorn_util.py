import asyncio
import contextvars
import logging
import socket
import threading
from concurrent.futures import Future
from typing import List
from typing import Optional
try:
    import uvicorn
except ImportError:
    raise ImportError('package uvicorn is required')


class AwaitableUvicornServer(uvicorn.Server):
    """
    Adds `wait_for_startup` method.
    The function (asynchronously) blocks until the server
    starts listening or throws an error.
    """

    def __init__(self, config: 'uvicorn.Config') -> None:
        super().__init__(config)
        self._startup_future: Future[None] = Future()

    async def startup(self, sockets: Optional[List[socket.socket]] = None) -> None:
        try:
            result = await super().startup(sockets)
            self._startup_future.set_result(None)
            return result
        except Exception as error:
            self._startup_future.set_exception(error)
            raise error

    async def wait_for_startup(self) -> None:
        await asyncio.shield(asyncio.wrap_future(self._startup_future))

    async def shutdown(self, sockets: Optional[list[socket.socket]] = None) -> None:
        if self.started:
            await super().shutdown(sockets)


class ThreadedUvicornServer(AwaitableUvicornServer):
    """Serve interactive UDFs independently of a blocking notebook cell."""

    should_exit: bool

    def __init__(self, config: 'uvicorn.Config') -> None:
        super().__init__(config)
        self._finished: Future[None] = Future()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._lock = threading.Lock()
        self._shutdown_requested = False
        self._shutdown_timeout: Optional[float] = None
        self._thread = threading.Thread(
            target=contextvars.copy_context().run,
            args=(self._run_server_thread,),
            name='singlestoredb-udf-server',
            daemon=True,
        )

    def start(self) -> Future[None]:
        try:
            self._thread.start()
        except Exception as exc:
            self._startup_future.set_exception(exc)
            self._finished.set_exception(exc)
        return self._finished

    def request_shutdown(self, timeout: Optional[float]) -> None:
        with self._lock:
            if not self._shutdown_requested:
                self._shutdown_timeout = timeout
                self._shutdown_requested = True
            if self._loop is not None:
                self._loop.call_soon_threadsafe(self._begin_shutdown)

    def _begin_shutdown(self) -> None:
        if not self.should_exit:
            self.config.timeout_graceful_shutdown = self._shutdown_timeout
            self.should_exit = True

    async def shutdown(self, sockets: Optional[List[socket.socket]] = None) -> None:
        await super().shutdown(sockets)
        if not self.force_exit:
            await asyncio.gather(*self.server_state.tasks, return_exceptions=True)

    def join(self) -> None:
        if self._thread.ident is not None:
            self._thread.join()

    def _run_server_thread(self) -> None:
        try:
            self._run_loop()
        except BaseException as exc:
            logging.getLogger('uvicorn.error').exception('UDF server thread failed')
            error: BaseException = exc
            if isinstance(exc, SystemExit):
                error = RuntimeError(f'UDF server exited with code {exc.code}')
                error.__cause__ = exc
            if not self._startup_future.done():
                self._startup_future.set_exception(error)
            self._finished.set_exception(error)
        else:
            if not self._startup_future.done():
                self._startup_future.set_exception(
                    RuntimeError('UDF server stopped before startup'),
                )
            self._finished.set_result(None)

    def _run_loop(self) -> None:
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            with self._lock:
                self._loop = loop
                if self._shutdown_requested:
                    self._begin_shutdown()
            try:
                loop.run_until_complete(self.serve())
            finally:
                with self._lock:
                    self._loop = None
                if self.started:
                    for listener in self.servers:
                        listener.close()
                for connection in list(self.server_state.connections):
                    connection.transport.abort()
                pending = asyncio.all_tasks(loop)
                for task in pending:
                    task.cancel()
                if pending:
                    results = loop.run_until_complete(
                        asyncio.gather(*pending, return_exceptions=True),
                    )
                    for result in results:
                        if isinstance(result, Exception):
                            loop.call_exception_handler({
                                'message': 'Unexpected error during UDF server shutdown',
                                'exception': result,
                            })
                loop.run_until_complete(loop.shutdown_asyncgens())
        finally:
            asyncio.set_event_loop(None)
            loop.close()
