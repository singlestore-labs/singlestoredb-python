"""
Server lifecycle: accept loop, thread pool, shutdown.

Mirrors the Rust wasm-udf-server architecture with a ThreadPoolExecutor
for concurrent request handling and a SharedRegistry with generation-
counter caching for thread-safe live reload.
"""
import importlib
import logging
import multiprocessing
import os
import select
import signal
import socket
import sys
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from .connection import handle_connection
from .registry import FunctionRegistry

logger = logging.getLogger('collocated.server')


class SharedRegistry:
    """Thread-safe wrapper around FunctionRegistry with generation caching.

    Each worker thread caches a (generation, FunctionRegistry) pair in
    thread-local storage. When @@register bumps the generation, workers
    create a fresh registry and replay all code blocks on next call.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._generation: int = 0
        self._code_blocks: List[Tuple[str, str, bool]] = []
        self._base_registry: Optional[FunctionRegistry] = None
        self._local = threading.local()

    def set_base_registry(self, registry: FunctionRegistry) -> None:
        """Set the base registry (after initial module import + init)."""
        with self._lock:
            self._base_registry = registry

    @property
    def generation(self) -> int:
        return self._generation

    def create_function(
        self,
        signature_json: str,
        code: str,
        replace: bool,
    ) -> List[str]:
        """Register a new function and bump the generation counter.

        Thread-safe: acquires the lock, validates via a temporary
        registry, stores the code block, and increments generation.
        """
        with self._lock:
            # Validate on a temporary registry first
            test_registry = self._build_fresh_registry()
            new_names = test_registry.create_function(
                signature_json, code, replace,
            )
            # Success: store the code block and bump generation
            self._code_blocks.append((signature_json, code, replace))
            self._generation += 1
            logger.info(
                f'SharedRegistry: generation={self._generation}, '
                f'code_blocks={len(self._code_blocks)}',
            )
            return new_names

    def get_thread_local_registry(self) -> FunctionRegistry:
        """Get or refresh the thread-local cached registry.

        Cheap int comparison on the hot path; only rebuilds on
        generation mismatch.
        """
        cached = getattr(self._local, 'cached', None)
        if cached is not None:
            cached_gen, cached_reg = cached
            if cached_gen == self._generation:
                return cached_reg

        # Rebuild from base + code blocks
        with self._lock:
            registry = self._build_fresh_registry()
            gen = self._generation

        self._local.cached = (gen, registry)
        return registry

    def _build_fresh_registry(self) -> FunctionRegistry:
        """Build a fresh registry with base functions + all code blocks.

        Must be called with self._lock held.
        """
        registry = FunctionRegistry()
        # Copy base functions
        if self._base_registry is not None:
            registry.functions = dict(self._base_registry.functions)
        # Replay code blocks
        for sig_json, code, replace in self._code_blocks:
            registry.create_function(sig_json, code, replace)
        return registry


class Server:
    """Collocated UDF server with Unix socket + thread pool."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self.shared_registry = SharedRegistry()
        self.shutdown_event = threading.Event()

    def run(self) -> None:
        """Run the server: import modules, bind socket, accept loop."""
        # 1. Import user modules & initialize registry
        registry = self._initialize_registry()
        self.shared_registry.set_base_registry(registry)

        # 2. Create & bind Unix socket
        server_sock = self._bind_socket()

        # 3. Determine worker count and process mode
        n_workers = self.config.get('n_workers', 0)
        if n_workers <= 0:
            n_workers = os.cpu_count() or 4

        process_mode = self.config.get('process_mode', 'process')

        # 4. Signal handling (main process)
        def _signal_handler(signum: int, frame: Any) -> None:
            logger.info(f'Received signal {signum}, shutting down...')
            self.shutdown_event.set()

        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)

        # 5. Dispatch to mode-specific loop
        sock_path = self.config['socket']
        try:
            if process_mode == 'process':
                self._run_process_mode(server_sock, n_workers)
            else:
                self._run_thread_mode(server_sock, n_workers)
        finally:
            server_sock.close()
            try:
                os.unlink(sock_path)
            except OSError:
                pass
            logger.info('Server stopped.')

    def _bind_socket(self) -> socket.socket:
        """Create, bind, and listen on the Unix domain socket."""
        sock_path = self.config['socket']
        if os.path.exists(sock_path):
            os.unlink(sock_path)

        server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server_sock.bind(sock_path)
        os.chmod(sock_path, 0o600)

        backlog = self.config.get('max_connections', 32)
        server_sock.listen(backlog)
        logger.info(f'Listening on {sock_path} (backlog={backlog})')
        return server_sock

    def _run_thread_mode(
        self,
        server_sock: socket.socket,
        n_workers: int,
    ) -> None:
        """Accept loop using a ThreadPoolExecutor."""
        pool = ThreadPoolExecutor(max_workers=n_workers)
        logger.info(f'Thread pool: {n_workers} workers')

        try:
            while not self.shutdown_event.is_set():
                readable, _, _ = select.select(
                    [server_sock], [], [], 0.1,
                )
                if not readable:
                    continue

                conn, _ = server_sock.accept()
                pool.submit(
                    handle_connection,
                    conn,
                    self.shared_registry,
                    self.shutdown_event,
                )
        finally:
            logger.info('Shutting down thread pool...')
            pool.shutdown(wait=True)

    def _run_process_mode(
        self,
        server_sock: socket.socket,
        n_workers: int,
    ) -> None:
        """Pre-fork worker pool for true CPU parallelism."""
        ctx = multiprocessing.get_context('fork')
        workers: Dict[int, multiprocessing.process.BaseProcess] = {}

        def _spawn_worker(
            worker_id: int,
        ) -> multiprocessing.process.BaseProcess:
            p = ctx.Process(
                target=self._worker_process_main,
                args=(server_sock, worker_id),
                daemon=True,
            )
            p.start()
            logger.info(
                f'Started worker {worker_id} (pid={p.pid})',
            )
            return p

        # Fork initial workers
        logger.info(
            f'Process pool: spawning {n_workers} workers',
        )
        for i in range(n_workers):
            workers[i] = _spawn_worker(i)

        # Monitor loop: restart dead workers
        try:
            while not self.shutdown_event.is_set():
                self.shutdown_event.wait(timeout=0.5)
                for wid, proc in list(workers.items()):
                    if not proc.is_alive():
                        exitcode = proc.exitcode
                        if not self.shutdown_event.is_set():
                            logger.warning(
                                f'Worker {wid} (pid={proc.pid}) '
                                f'exited with code {exitcode}, '
                                f'restarting...',
                            )
                            workers[wid] = _spawn_worker(wid)
        finally:
            logger.info('Shutting down worker processes...')
            # Signal all workers to stop
            for wid, proc in workers.items():
                if proc.is_alive():
                    assert proc.pid is not None
                    os.kill(proc.pid, signal.SIGTERM)

            # Wait for graceful exit
            for wid, proc in workers.items():
                proc.join(timeout=5.0)
                if proc.is_alive():
                    logger.warning(
                        f'Worker {wid} (pid={proc.pid}) '
                        f'did not exit, terminating...',
                    )
                    proc.terminate()
                    proc.join(timeout=2.0)

    def _worker_process_main(
        self,
        server_sock: socket.socket,
        worker_id: int,
    ) -> None:
        """Entry point for each forked worker process."""
        try:
            # Each worker gets its own registry and shutdown event
            local_shared = SharedRegistry()
            local_registry = FunctionRegistry()
            local_registry.initialize()
            local_shared.set_base_registry(local_registry)

            local_shutdown = threading.Event()

            def _worker_signal_handler(
                signum: int,
                frame: Any,
            ) -> None:
                local_shutdown.set()

            signal.signal(signal.SIGTERM, _worker_signal_handler)
            signal.signal(signal.SIGINT, signal.SIG_IGN)

            # Set non-blocking so accept() raises BlockingIOError
            # instead of blocking when another worker wins the race.
            # O_NONBLOCK is on the open file description (shared across
            # forked processes), but that's fine — all workers want
            # non-blocking accept and the parent doesn't call accept.
            server_sock.setblocking(False)

            logger.info(
                f'Worker {worker_id} (pid={os.getpid()}) ready, '
                f'{len(local_registry.functions)} function(s)',
            )

            # Accept loop
            while not local_shutdown.is_set():
                readable, _, _ = select.select(
                    [server_sock], [], [], 0.1,
                )
                if not readable:
                    continue

                try:
                    conn, _ = server_sock.accept()
                except BlockingIOError:
                    # Another worker won the accept race
                    continue
                except OSError:
                    if local_shutdown.is_set():
                        break
                    raise

                handle_connection(
                    conn,
                    local_shared,
                    local_shutdown,
                )
        except Exception:
            logger.error(
                f'Worker {worker_id} crashed:\n'
                f'{traceback.format_exc()}',
            )
            raise

    def _initialize_registry(self) -> FunctionRegistry:
        """Import the extension module and discover @udf functions."""
        extension = self.config['extension']
        extension_path = self.config.get('extension_path', '')

        # Prepend extension path directories to sys.path
        if extension_path:
            for p in reversed(extension_path.split(':')):
                p = p.strip()
                if p and p not in sys.path:
                    sys.path.insert(0, p)
                    logger.info(f'Added to sys.path: {p}')

        # Import the extension module
        logger.info(f'Importing extension module: {extension}')
        importlib.import_module(extension)

        # Initialize registry (discovers @udf functions from sys.modules)
        registry = FunctionRegistry()
        registry.initialize()

        func_count = len(registry.functions)
        if func_count == 0:
            raise RuntimeError(
                f'No @udf functions found after importing {extension!r}',
            )
        logger.info(f'Discovered {func_count} function(s)')
        for name in sorted(registry.functions):
            logger.info(f'  function: {name}')

        return registry
