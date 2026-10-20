"""
Server lifecycle: accept loop, thread pool, shutdown.

Mirrors the Rust wasm-udf-server architecture with a ThreadPoolExecutor
for concurrent request handling and a SharedRegistry with generation-
counter caching for thread-safe live reload.
"""
import importlib
import json
import logging
import multiprocessing
import os
import select
import signal
import socket
import stat
import struct
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple

from . import overload as _overload
from .connection import _close_quietly
from .connection import _MAX_V3_BODY_BYTES
from .connection import _MAX_V3_ENVELOPE_BYTES
from .connection import _write_all_fd
from .connection import handle_connection
from .registry import FunctionRegistry

logger = logging.getLogger('plugin.server')


# Pipe frame caps. The header is JSON built from a v3 envelope, and JSON
# escaping can grow a control character sixfold, so it gets headroom over
# the envelope cap. The blob is a raw function body, so its cap is the
# handshake's own: any body the handshake accepts can be propagated.
_MAX_PIPE_HEADER = 8 * _MAX_V3_ENVELOPE_BYTES
_MAX_PIPE_BLOB = _MAX_V3_BODY_BYTES
_PIPE_PREFIX = struct.Struct('<IQ')

# How long a retired worker may spend finishing its connection before it
# is terminated.
_DRAIN_TIMEOUT = 60.0


class PipeFrameError(Exception):
    """A pipe frame that can't be read. The stream can't be resynced."""


class _RetiringWorker:
    """A worker told to stop accepting, left to finish its connection."""

    def __init__(
        self,
        wid: int,
        proc: multiprocessing.process.BaseProcess,
        pipe_r: Optional[int],
        deadline: float,
    ) -> None:
        self.wid = wid
        self.proc = proc
        self.pipe_r = pipe_r
        self.deadline = deadline
        self.terminated = False

    def close_pipe(self) -> None:
        if self.pipe_r is not None:
            _close_quietly(self.pipe_r)
            self.pipe_r = None


def _read_exact_fd(fd: int, n: int) -> Optional[bytes]:
    """Read exactly ``n`` bytes from ``fd``; None on EOF first."""
    buf = bytearray(n)
    pos = 0
    while pos < n:
        chunk = os.read(fd, n - pos)
        if not chunk:
            return None
        buf[pos:pos + len(chunk)] = chunk
        pos += len(chunk)
    return bytes(buf)


def _read_pipe_message(fd: int) -> Optional[Tuple[bytes, bytes]]:
    """Read one ``(header, blob)`` frame from a pipe fd.

    Wire format: [u32 LE header_len][u64 LE blob_len][header][blob].
    Returns None on EOF or short read. Raises PipeFrameError if a length
    exceeds its cap: the payload is left unread, so the pipe is unusable.
    """
    try:
        prefix = _read_exact_fd(fd, _PIPE_PREFIX.size)
        if prefix is None:
            return None
        header_len, blob_len = _PIPE_PREFIX.unpack(prefix)
        if header_len > _MAX_PIPE_HEADER or blob_len > _MAX_PIPE_BLOB:
            raise PipeFrameError(
                f'pipe frame too large: header {header_len} bytes '
                f'(max {_MAX_PIPE_HEADER}), blob {blob_len} bytes '
                f'(max {_MAX_PIPE_BLOB})',
            )
        header = _read_exact_fd(fd, header_len)
        if header is None:
            return None
        blob = _read_exact_fd(fd, blob_len)
        if blob is None:
            return None
        return header, blob
    except OSError:
        return None


def _write_pipe_message(fd: int, header: bytes, blob: bytes = b'') -> None:
    """Write one ``(header, blob)`` frame to a pipe fd.

    Wire format: [u32 LE header_len][u64 LE blob_len][header][blob].
    """
    _write_all_fd(fd, _PIPE_PREFIX.pack(len(header), len(blob)) + header)
    if blob:
        _write_all_fd(fd, blob)


def _signature_key(signature_json: str) -> Optional[Tuple[str, str]]:
    """Return ``(name, normalized param types key)`` for a signature JSON.

    Returns None if the signature is malformed; the registry's own
    validation reports that.
    """
    try:
        sig = json.loads(signature_json)
        return sig['name'], _overload.signature_key([
            _overload.normalize_sql_type(a['sql']) for a in sig['args']
        ])
    except (ValueError, KeyError, TypeError, AttributeError):
        return None


class SharedRegistry:
    """Thread-safe wrapper around FunctionRegistry with generation caching.

    Each worker thread caches a (generation, FunctionRegistry) pair in
    thread-local storage. When a v3 handshake envelope installs a new
    definition (or @@delete removes one), the generation is bumped and
    workers create a fresh registry and replay all code blocks on next
    call. That covers threads within one process; in process mode each
    worker has its own copy, so main applies the change to its copy and
    replaces the workers (see :class:`_ProcessPool`).

    Code blocks are keyed by an opaque content-addressed id supplied in
    the v3 handshake envelope. Deletion is also keyed by id so callers
    never need to recover a function name from a stored signature.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._generation: int = 0
        # id -> (signature_json, code, replace)
        self._code_blocks: Dict[str, Tuple[str, str, bool]] = {}
        self._base_registry: Optional[FunctionRegistry] = None
        self._local = threading.local()

    def set_base_registry(self, registry: FunctionRegistry) -> None:
        """Set the base registry (after initial module import + init)."""
        with self._lock:
            self._base_registry = registry

    @property
    def generation(self) -> int:
        return self._generation

    def has_definition(self, id: str) -> bool:
        """Return True if a definition with this id is already cached."""
        with self._lock:
            return id in self._code_blocks

    def create_function(
        self,
        id: str,
        signature_json: str,
        code: str,
        replace: bool,
    ) -> Optional[List[str]]:
        """Register a new function and bump the generation counter.

        Thread-safe: acquires the lock, validates via a temporary
        registry, stores the code block keyed by id, and increments
        generation.

        An id is a content key, so an id that is already cached is the
        same definition: that is a no-op returning None, with no
        generation bump. Checking under the lock makes this atomic, so
        two handshakes that both missed ``has_definition`` can't collide.

        SingleStoreDB's ``CREATE OR REPLACE`` ships a fresh id without an
        ``@@delete`` for the old one, so any other cached id whose
        signature has the same ``(name, normalized param types)`` is
        superseded: left out of the validation replay, installed over with
        ``replace`` forced on, and evicted only once validation succeeds.
        """
        with self._lock:
            if id in self._code_blocks:
                return None
            incoming_key = _signature_key(signature_json)
            superseded = [
                other_id
                for other_id, (sig_json, _, _) in self._code_blocks.items()
                if other_id != id and incoming_key is not None
                and _signature_key(sig_json) == incoming_key
            ]
            # Validate on a temporary registry first
            test_registry = self._build_fresh_registry(exclude=set(superseded))
            new_names = test_registry.create_function(
                id, signature_json, code, replace or bool(superseded),
            )
            # Success: store the code block and bump generation
            for other_id in superseded:
                del self._code_blocks[other_id]
            if superseded:
                logger.info(
                    f'SharedRegistry: id={id!r} superseded '
                    f'ids={superseded!r}',
                )
            self._code_blocks[id] = (signature_json, code, replace)
            self._generation += 1
            logger.info(
                f'SharedRegistry: generation={self._generation}, '
                f'code_blocks={len(self._code_blocks)}',
            )
            return new_names

    def delete_function(self, id: str) -> None:
        """Delete a dynamically registered function by its id.

        Raises ValueError if the id is unknown.
        """
        with self._lock:
            if id not in self._code_blocks:
                raise ValueError(
                    f"No registered function with id '{id}'",
                )
            del self._code_blocks[id]
            self._generation += 1
            logger.info(
                f'SharedRegistry: deleted function id={id!r}, '
                f'generation={self._generation}, '
                f'code_blocks={len(self._code_blocks)}',
            )

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

    def _build_fresh_registry(
        self,
        exclude: Optional[Set[str]] = None,
    ) -> FunctionRegistry:
        """Build a fresh registry with base functions + all code blocks.

        Code blocks whose id is in ``exclude`` are not replayed.
        Must be called with self._lock held.
        """
        registry = FunctionRegistry()
        # Copy base functions. Each value is a list of variants — copy the
        # list so worker-local mutations (e.g. dynamic create_function) don't
        # bleed into the shared base registry.
        if self._base_registry is not None:
            registry.functions = {
                k: list(v)
                for k, v in self._base_registry.functions.items()
            }
            registry._base_function_names = (
                set(self._base_registry._base_function_names)
                | set(self._base_registry.functions.keys())
            )
        # Replay code blocks
        for id, (sig_json, code, replace) in self._code_blocks.items():
            if exclude and id in exclude:
                continue
            registry.create_function(id, sig_json, code, replace)
        return registry


class _ProcessPool:
    """The main process's view of a pre-forked worker pool.

    Each worker gets a pipe back to the main process. When a worker
    installs a v3 envelope definition (or handles @@delete), it writes the
    registration/deletion to its pipe. The main process applies it to its
    own registry and, if that changed anything, forks a fresh set of
    workers from the updated state.

    The old workers are retired, not killed: SIGUSR1 stops their accept
    loop, so the connection each one is serving -- including the one that
    shipped the definition -- runs to completion before the worker exits.
    Main keeps reading a retiring worker's pipe until then, since an
    in-flight connection can still register. A worker still draining
    after ``_DRAIN_TIMEOUT`` is terminated.
    """

    def __init__(
        self,
        server: 'Server',
        ctx: Any,
        server_sock: socket.socket,
        n_workers: int,
    ) -> None:
        self.server = server
        self.ctx = ctx
        self.server_sock = server_sock
        self.n_workers = n_workers
        # workers[wid] = (process, pipe_read_fd)
        self.workers: Dict[
            int,
            Tuple[multiprocessing.process.BaseProcess, int],
        ] = {}
        # retiring[pid]; a retiring worker's pipe is None once closed
        self.retiring: Dict[int, _RetiringWorker] = {}

    def run(self) -> None:
        logger.info(
            f'Process pool: spawning {self.n_workers} workers',
        )
        for i in range(self.n_workers):
            self.workers[i] = self._spawn(i)
        try:
            while not self.server.shutdown_event.is_set():
                if self._poll_once():
                    self._retire_and_replace_all()
                self._reap_retiring()
                self._respawn_dead()
        finally:
            logger.info('Shutting down worker processes...')
            self._kill_all()

    def _open_pipe_fds(self) -> List[int]:
        fds = [pipe_r for _, pipe_r in self.workers.values()]
        fds.extend(
            r.pipe_r for r in self.retiring.values() if r.pipe_r is not None
        )
        return fds

    def _spawn(self, worker_id: int) -> Tuple[
        multiprocessing.process.BaseProcess, int,
    ]:
        pipe_r, pipe_w = os.pipe()
        p = self.ctx.Process(
            target=self.server._worker_process_main,
            args=(
                self.server_sock, worker_id, pipe_w,
                self._open_pipe_fds() + [pipe_r],
            ),
            daemon=True,
        )
        p.start()
        # Close the write end in the parent — only the child writes
        os.close(pipe_w)
        logger.info(
            f'Started worker {worker_id} (pid={p.pid})',
        )
        return p, pipe_r

    def _retire(
        self,
        wid: int,
        proc: multiprocessing.process.BaseProcess,
        pipe_r: Optional[int],
    ) -> None:
        """Stop a worker accepting and leave it to drain."""
        assert proc.pid is not None
        if proc.is_alive():
            # A worker forked so recently that it hasn't installed its
            # handler yet dies of SIGUSR1's default action instead. It
            # can't have accepted a connection, so that's harmless.
            try:
                os.kill(proc.pid, signal.SIGUSR1)
            except ProcessLookupError:
                pass
        self.retiring[proc.pid] = _RetiringWorker(
            wid, proc, pipe_r, time.monotonic() + _DRAIN_TIMEOUT,
        )
        logger.info(f'Retiring worker {wid} (pid={proc.pid})')

    def _retire_and_replace_all(self) -> None:
        """Retire every worker and fork replacements from main's state."""
        old = list(self.workers.items())
        self.workers.clear()
        for wid, (proc, pipe_r) in old:
            self._retire(wid, proc, pipe_r)
        for i in range(self.n_workers):
            self.workers[i] = self._spawn(i)

    def _poll_once(self) -> bool:
        """Read pending worker messages; return True if the registry changed."""
        poller = select.poll()
        # fd -> ('active', wid) or ('retiring', pid)
        fd_owner: Dict[int, Tuple[str, int]] = {}
        for wid, (_, pipe_r) in self.workers.items():
            poller.register(pipe_r, select.POLLIN | select.POLLHUP)
            fd_owner[pipe_r] = ('active', wid)
        for pid, r in self.retiring.items():
            if r.pipe_r is not None:
                poller.register(r.pipe_r, select.POLLIN | select.POLLHUP)
                fd_owner[r.pipe_r] = ('retiring', pid)

        registry_changed = False
        for fd, event in poller.poll(500):  # 500ms timeout
            if fd not in fd_owner:
                continue
            kind, key = fd_owner[fd]
            if kind == 'active':
                source = f'worker {key}'
            else:
                source = f'retiring worker {self.retiring[key].wid}'

            if not event & select.POLLIN:
                # POLLHUP: the worker exited. Active ones are respawned by
                # _respawn_dead; a retiring one has nothing left to say.
                if kind == 'retiring':
                    self.retiring[key].close_pipe()
                continue

            try:
                msg = _read_pipe_message(fd)
            except PipeFrameError as exc:
                logger.error(
                    f'Main process: unreadable pipe frame from {source}: '
                    f'{exc}; retiring it',
                )
                if kind == 'active':
                    proc, pipe_r = self.workers.pop(key)
                    _close_quietly(pipe_r)
                    self._retire(key, proc, None)
                    self.workers[key] = self._spawn(key)
                else:
                    self.retiring[key].close_pipe()
                continue

            if msg is not None:
                if self._apply_message(*msg, source):
                    registry_changed = True
            elif kind == 'retiring':
                self.retiring[key].close_pipe()
        return registry_changed

    def _apply_message(self, header: bytes, blob: bytes, source: str) -> bool:
        """Apply one worker message; return True if the registry changed."""
        shared_registry = self.server.shared_registry
        try:
            body = json.loads(header)
            action = body.get('action', 'register')
            if action == 'register':
                if shared_registry.create_function(
                    body['id'],
                    body['signature_json'],
                    blob.decode('utf8'),
                    body['replace'],
                ) is None:
                    logger.info(
                        f'Main process: id={body["id"]!r} from {source} '
                        'is already registered',
                    )
                    return False
                logger.info(
                    'Main process: applied v3 envelope registration '
                    f'from {source}, will replace all workers',
                )
                return True
            if action == 'delete':
                shared_registry.delete_function(body['id'])
                logger.info(
                    f'Main process: applied @@delete from {source}, '
                    'will replace all workers',
                )
                return True
            logger.error(f'Main process: unknown pipe action: {action}')
        except Exception:
            logger.error(
                'Main process: failed to apply pipe message:\n'
                f'{traceback.format_exc()}',
            )
        return False

    def _reap_retiring(self) -> None:
        """Collect drained workers; escalate on ones past their deadline."""
        now = time.monotonic()
        for pid, r in list(self.retiring.items()):
            if not r.proc.is_alive():
                r.proc.join(timeout=0)
                r.close_pipe()
                del self.retiring[pid]
                logger.info(
                    f'Retired worker {r.wid} (pid={pid}) exited '
                    f'with code {r.proc.exitcode}',
                )
            elif now < r.deadline:
                continue
            elif not r.terminated:
                logger.warning(
                    f'Retired worker {r.wid} (pid={pid}) still draining '
                    f'after {_DRAIN_TIMEOUT}s, terminating...',
                )
                r.terminated = True
                r.deadline = now + 5.0
                try:
                    os.kill(pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
            else:
                logger.warning(
                    f'Retired worker {r.wid} (pid={pid}) ignored SIGTERM, '
                    'killing...',
                )
                r.proc.kill()
                r.deadline = now + 5.0

    def _respawn_dead(self) -> None:
        """Respawn active workers that crashed; retiring ones are reaped."""
        for wid, (proc, pipe_r) in list(self.workers.items()):
            if proc.is_alive() or self.server.shutdown_event.is_set():
                continue
            logger.warning(
                f'Worker {wid} (pid={proc.pid}) exited with code '
                f'{proc.exitcode}, restarting...',
            )
            _close_quietly(pipe_r)
            self.workers[wid] = self._spawn(wid)

    def _kill_all(self) -> None:
        """SIGTERM active and retiring workers, wait, then SIGKILL."""
        procs: List[
            Tuple[int, multiprocessing.process.BaseProcess, Optional[int]]
        ] = [
            (wid, proc, pipe_r)
            for wid, (proc, pipe_r) in self.workers.items()
        ]
        procs.extend((r.wid, r.proc, r.pipe_r) for r in self.retiring.values())
        for _, proc, _ in procs:
            if proc.is_alive():
                assert proc.pid is not None
                try:
                    os.kill(proc.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
        for wid, proc, _ in procs:
            proc.join(timeout=5.0)
            if proc.is_alive():
                logger.warning(
                    f'Worker {wid} (pid={proc.pid}) did not exit, killing...',
                )
                proc.kill()
                proc.join(timeout=2.0)
        for _, _, pipe_r in procs:
            if pipe_r is not None:
                _close_quietly(pipe_r)
        self.workers.clear()
        self.retiring.clear()


class Server:
    """Plugin UDF server with Unix socket + thread pool."""

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
            mode = os.stat(sock_path).st_mode
            if not stat.S_ISSOCK(mode):
                raise RuntimeError(
                    f'Path exists but is not a socket: {sock_path}',
                )
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
        """Pre-fork worker pool for true CPU parallelism.

        See :class:`_ProcessPool` for how registry changes reach the
        workers without interrupting their connections.
        """
        try:
            ctx = multiprocessing.get_context('fork')
        except ValueError:
            raise RuntimeError(
                "Process mode requires 'fork' multiprocessing context, "
                'which is not available on this platform. '
                "Use process_mode='thread' instead.",
            )
        _ProcessPool(self, ctx, server_sock, n_workers).run()

    def _worker_process_main(
        self,
        server_sock: socket.socket,
        worker_id: int,
        pipe_w: int,
        inherited_fds: List[int],
    ) -> None:
        """Entry point for each forked worker process.

        Uses ``self.shared_registry`` inherited via fork (contains the
        main process's current state). ``pipe_w`` is used to notify the
        main process when a v3 envelope definition is installed (or
        @@delete is handled) so it can replace the workers.

        ``inherited_fds`` are pipe read ends -- its own and the other
        workers' -- that fork copied in. They are closed so that main closing one is
        enough to make its writer see EPIPE rather than fill the pipe.

        SIGTERM shuts the worker down, abandoning its connection. SIGUSR1
        retires it: the accept loop stops, the current connection runs to
        completion, and then the worker exits.
        """
        try:
            for fd in inherited_fds:
                _close_quietly(fd)

            local_shutdown = threading.Event()
            stop_accepting = threading.Event()

            def _worker_signal_handler(
                signum: int,
                frame: Any,
            ) -> None:
                local_shutdown.set()

            def _retire_signal_handler(
                signum: int,
                frame: Any,
            ) -> None:
                stop_accepting.set()

            signal.signal(signal.SIGTERM, _worker_signal_handler)
            signal.signal(signal.SIGUSR1, _retire_signal_handler)
            signal.signal(signal.SIGINT, signal.SIG_IGN)

            # WARNING: setblocking(False) sets O_NONBLOCK on the open
            # file description, which is shared across all forked
            # processes. This is intentional here — all workers need
            # non-blocking accept() to handle the thundering-herd race,
            # and the parent process never calls accept() on this
            # socket. Do NOT add blocking operations on this socket
            # in the parent process after workers are forked.
            server_sock.setblocking(False)

            registry = self.shared_registry.get_thread_local_registry()
            logger.info(
                f'Worker {worker_id} (pid={os.getpid()}) ready, '
                f'{len(registry.functions)} function(s)',
            )

            # Accept loop
            while not (local_shutdown.is_set() or stop_accepting.is_set()):
                readable, _, _ = select.select(
                    [server_sock], [], [], 0.1,
                )
                if not readable or stop_accepting.is_set():
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

                # Only SIGTERM interrupts a connection; SIGUSR1 lets it
                # finish and is acted on once it returns.
                handle_connection(
                    conn,
                    self.shared_registry,
                    local_shutdown,
                    pipe_write_fd=pipe_w,
                )
        except Exception:
            logger.error(
                f'Worker {worker_id} crashed:\n'
                f'{traceback.format_exc()}',
            )
            raise
        finally:
            try:
                os.close(pipe_w)
            except OSError:
                pass

    def _initialize_registry(self) -> FunctionRegistry:
        """Import the plugin module and discover @udf functions."""
        plugin_name = self.config['plugin_name']
        search_path = self.config.get('search_path', '')

        # Prepend search path directories to sys.path
        if search_path:
            for p in reversed(search_path.split(':')):
                p = p.strip()
                if p and p not in sys.path:
                    sys.path.insert(0, p)
                    logger.info(f'Added to sys.path: {p}')

        # Import the plugin module
        logger.info(f'Importing plugin module: {plugin_name}')
        mod = importlib.import_module(plugin_name)

        # Initialize registry (discovers @udf functions from the plugin module)
        registry = FunctionRegistry()
        registry.initialize(plugin_module=mod)

        func_count = len(registry.functions)
        if func_count == 0:
            raise RuntimeError(
                f'No @udf functions found after importing {plugin_name!r}',
            )
        logger.info(f'Discovered {func_count} function(s)')
        for name in sorted(registry.functions):
            logger.info(f'  function: {name}')

        return registry
