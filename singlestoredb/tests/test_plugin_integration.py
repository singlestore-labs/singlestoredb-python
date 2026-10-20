#!/usr/bin/env python
# type: ignore
"""Integration tests for the plugin UDF server.

Covers: protocol handshake, UDF request loop, control signal integration,
server lifecycle (thread and process modes), rowdat_1 roundtrip with mixed
types, call_function dispatch matrix, and columnar/vector dispatch.

These tests do not require a database connection.
"""
import array
import datetime
import decimal
import json
import os
import signal
import socket
import struct
import tempfile
import threading
import time
import unittest.mock

from singlestoredb.functions.ext import rowdat_1
from singlestoredb.functions.ext.plugin.connection import _handle_connection_inner
from singlestoredb.functions.ext.plugin.connection import _install_v3_definition
from singlestoredb.functions.ext.plugin.connection import _MAX_V3_BODY_BYTES
from singlestoredb.functions.ext.plugin.connection import handle_connection
from singlestoredb.functions.ext.plugin.connection import STATUS_ERROR
from singlestoredb.functions.ext.plugin.connection import STATUS_OK
from singlestoredb.functions.ext.plugin.connection import SUPPORTED_VERSIONS
from singlestoredb.functions.ext.plugin.control import dispatch_control_signal
from singlestoredb.functions.ext.plugin.registry import call_function
from singlestoredb.functions.ext.plugin.registry import FunctionRegistry
from singlestoredb.functions.ext.plugin.server import _MAX_PIPE_HEADER
from singlestoredb.functions.ext.plugin.server import _PIPE_PREFIX
from singlestoredb.functions.ext.plugin.server import _ProcessPool
from singlestoredb.functions.ext.plugin.server import _read_pipe_message
from singlestoredb.functions.ext.plugin.server import _write_pipe_message
from singlestoredb.functions.ext.plugin.server import PipeFrameError
from singlestoredb.functions.ext.plugin.server import Server
from singlestoredb.functions.ext.plugin.server import SharedRegistry
from singlestoredb.mysql.constants import FIELD_TYPE as ft


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_shared_registry_with_func():
    """Create a SharedRegistry with a simple 'triple' UDF registered."""
    shared = SharedRegistry()
    base_reg = FunctionRegistry()
    shared.set_base_registry(base_reg)
    sig = json.dumps({
        'name': 'triple',
        'args': [{'name': 'x', 'dtype': 'int64', 'sql': 'BIGINT'}],
        'returns': [{'name': '', 'dtype': 'int64', 'sql': 'BIGINT'}],
    })
    shared.create_function('id-triple', sig, 'return x * 3', False)
    return shared


def _send_handshake(sock, function_name, input_fd, output_fd):
    """Send the binary handshake from client side."""
    name_bytes = function_name.encode('utf8')
    header = struct.pack('<QQ', SUPPORTED_VERSIONS[0], len(name_bytes))
    sock.sendall(header)

    # Send function name + 2 FDs via SCM_RIGHTS
    fd_array = array.array('i', [input_fd, output_fd])
    sock.sendmsg(
        [name_bytes],
        [(socket.SOL_SOCKET, socket.SCM_RIGHTS, fd_array)],
    )


def _send_handshake_v3(sock, function_name, input_fd, output_fd, envelope, body):
    """Send a v3 handshake: header, name, and 4 FDs (envelope, body)."""
    name_bytes = function_name.encode('utf8')
    sock.sendall(struct.pack('<QQ', 3, len(name_bytes)))
    meta_fd, body_fd = _create_temp_fds()
    try:
        os.write(meta_fd, json.dumps(envelope).encode('utf8'))
        os.write(body_fd, body)
        fd_array = array.array('i', [input_fd, output_fd, meta_fd, body_fd])
        sock.sendmsg(
            [name_bytes],
            [(socket.SOL_SOCKET, socket.SCM_RIGHTS, fd_array)],
        )
    finally:
        os.close(meta_fd)
        os.close(body_fd)


def _wait_for(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return predicate()


def _send_udf_request(sock, input_fd, input_data):
    """Write data to input mmap fd, then send length to trigger processing."""
    os.ftruncate(input_fd, max(128 * 1024, len(input_data)))
    os.lseek(input_fd, 0, os.SEEK_SET)
    os.write(input_fd, input_data)
    sock.sendall(struct.pack('<Q', len(input_data)))


def _recv_udf_response(sock, output_fd):
    """Receive the status+size response and read data from output mmap."""
    resp = b''
    while len(resp) < 16:
        chunk = sock.recv(16 - len(resp))
        if not chunk:
            raise RuntimeError('EOF during response')
        resp += chunk
    status, size = struct.unpack('<QQ', resp)
    if status == STATUS_OK:
        os.lseek(output_fd, 0, os.SEEK_SET)
        data = os.read(output_fd, size)
        return status, data
    else:
        # Error: message follows inline after the 16-byte header
        err_data = b''
        while len(err_data) < size:
            chunk = sock.recv(size - len(err_data))
            if not chunk:
                break
            err_data += chunk
        return status, err_data


def _create_temp_fds():
    """Create a pair of temp file FDs for input/output mmap simulation."""
    input_fd = os.open(
        tempfile.mktemp(), os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o600,
    )
    output_fd = os.open(
        tempfile.mktemp(), os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o600,
    )
    return input_fd, output_fd


# ---------------------------------------------------------------------------
# P0: Protocol Integration Tests
# ---------------------------------------------------------------------------

class TestProtocolIntegration(unittest.TestCase):
    """Full handshake -> UDF call -> response cycle."""

    def setUp(self):
        # Force Python fallback path to avoid C accel build issues
        import singlestoredb.functions.ext.plugin.registry as _reg_mod
        import singlestoredb.functions.ext.plugin.connection as _conn_mod
        self._orig_has_accel = _reg_mod._has_accel
        _reg_mod._has_accel = False
        _conn_mod._has_accel = False

    def tearDown(self):
        import singlestoredb.functions.ext.plugin.registry as _reg_mod
        import singlestoredb.functions.ext.plugin.connection as _conn_mod
        _reg_mod._has_accel = self._orig_has_accel
        _conn_mod._has_accel = self._orig_has_accel

    def test_handshake_and_udf_call(self):
        """Complete end-to-end: handshake, send request, get response."""
        shared = _make_shared_registry_with_func()
        shutdown_event = threading.Event()

        client_sock, server_sock = socket.socketpair()
        input_fd, output_fd = _create_temp_fds()
        server_input_fd = os.dup(input_fd)
        server_output_fd = os.dup(output_fd)

        try:
            # Start server first - it blocks on recv waiting for handshake
            def run_server():
                _handle_connection_inner(
                    server_sock, shared, shutdown_event,
                )

            t = threading.Thread(target=run_server)
            t.start()

            _send_handshake(client_sock, 'triple', server_input_fd, server_output_fd)

            input_data = bytes(rowdat_1._dump([ft.LONGLONG], [1], [[7]]))
            _send_udf_request(client_sock, input_fd, input_data)

            status, data = _recv_udf_response(client_sock, output_fd)
            assert status == STATUS_OK, f'Expected 200, got {status}'

            ids, rows = rowdat_1._load([('r', ft.LONGLONG)], data)
            assert ids == [1]
            assert rows[0][0] == 21  # 7 * 3

            client_sock.sendall(struct.pack('<Q', 0))
            t.join(timeout=5)
            assert not t.is_alive()
        finally:
            client_sock.close()
            os.close(input_fd)
            os.close(output_fd)

    def test_multiple_requests_in_loop(self):
        """Multiple UDF invocations on the same connection."""
        shared = _make_shared_registry_with_func()
        shutdown_event = threading.Event()

        client_sock, server_sock = socket.socketpair()
        input_fd, output_fd = _create_temp_fds()
        server_input_fd = os.dup(input_fd)
        server_output_fd = os.dup(output_fd)

        try:
            def run_server():
                _handle_connection_inner(
                    server_sock, shared, shutdown_event,
                )

            t = threading.Thread(target=run_server)
            t.start()

            _send_handshake(client_sock, 'triple', server_input_fd, server_output_fd)

            for val in [1, 5, 100, -3]:
                input_data = bytes(rowdat_1._dump([ft.LONGLONG], [1], [[val]]))
                _send_udf_request(client_sock, input_fd, input_data)
                status, data = _recv_udf_response(client_sock, output_fd)
                assert status == STATUS_OK
                _, rows = rowdat_1._load([('r', ft.LONGLONG)], data)
                assert rows[0][0] == val * 3, f'{val} * 3 != {rows[0][0]}'

            client_sock.sendall(struct.pack('<Q', 0))
            t.join(timeout=5)
        finally:
            client_sock.close()
            os.close(input_fd)
            os.close(output_fd)

    def test_unknown_function_returns_error(self):
        """Calling a non-existent function returns STATUS_ERROR."""
        shared = _make_shared_registry_with_func()
        shutdown_event = threading.Event()

        client_sock, server_sock = socket.socketpair()
        input_fd, output_fd = _create_temp_fds()
        server_input_fd = os.dup(input_fd)
        server_output_fd = os.dup(output_fd)

        try:
            _send_handshake(
                client_sock, 'no_such_func', server_input_fd, server_output_fd,
            )

            input_data = bytes(rowdat_1._dump([ft.LONGLONG], [1], [[42]]))
            _send_udf_request(client_sock, input_fd, input_data)

            def run_server():
                _handle_connection_inner(
                    server_sock, shared, shutdown_event,
                )

            t = threading.Thread(target=run_server)
            t.start()

            status, data = _recv_udf_response(client_sock, output_fd)
            assert status == STATUS_ERROR
            assert b'unknown function' in data

            t.join(timeout=5)
        finally:
            client_sock.close()
            os.close(input_fd)
            os.close(output_fd)

    def test_control_signal_via_protocol(self):
        """@@health control signal through the full protocol path."""
        shared = _make_shared_registry_with_func()
        shutdown_event = threading.Event()

        client_sock, server_sock = socket.socketpair()
        input_fd, output_fd = _create_temp_fds()
        server_input_fd = os.dup(input_fd)
        server_output_fd = os.dup(output_fd)

        try:
            _send_handshake(
                client_sock, '@@health', server_input_fd, server_output_fd,
            )

            # Control signals send an 8-byte length then read from mmap
            # For @@health, no input data needed (length=0)
            client_sock.sendall(struct.pack('<Q', 0))

            def run_server():
                _handle_connection_inner(
                    server_sock, shared, shutdown_event,
                )

            t = threading.Thread(target=run_server)
            t.start()

            status, data = _recv_udf_response(client_sock, output_fd)
            assert status == STATUS_OK
            result = json.loads(data)
            assert result['status'] == 'ok'

            t.join(timeout=5)
        finally:
            client_sock.close()
            os.close(input_fd)
            os.close(output_fd)

    def test_control_signal_functions_lists_registered(self):
        """@@functions returns the registered function list."""
        shared = _make_shared_registry_with_func()
        shutdown_event = threading.Event()

        client_sock, server_sock = socket.socketpair()
        input_fd, output_fd = _create_temp_fds()
        server_input_fd = os.dup(input_fd)
        server_output_fd = os.dup(output_fd)

        try:
            _send_handshake(
                client_sock, '@@functions', server_input_fd, server_output_fd,
            )
            client_sock.sendall(struct.pack('<Q', 0))

            def run_server():
                _handle_connection_inner(
                    server_sock, shared, shutdown_event,
                )

            t = threading.Thread(target=run_server)
            t.start()

            status, data = _recv_udf_response(client_sock, output_fd)
            assert status == STATUS_OK
            result = json.loads(data)
            assert 'functions' in result
            names = [f['name'] for f in result['functions']]
            assert 'triple' in names

            t.join(timeout=5)
        finally:
            client_sock.close()
            os.close(input_fd)
            os.close(output_fd)


# ---------------------------------------------------------------------------
# P0: @@delete via dispatch_control_signal
# ---------------------------------------------------------------------------

class TestRegisterDeleteIntegration(unittest.TestCase):
    """Test @@delete through dispatch_control_signal with a real
    SharedRegistry (not mocked).  @@register no longer exists as a
    control signal; new function definitions arrive only via the
    in-band v3 envelope.  Functions are seeded directly through
    ``SharedRegistry.create_function``."""

    def _make_shared(self):
        shared = SharedRegistry()
        base_reg = FunctionRegistry()
        shared.set_base_registry(base_reg)
        return shared

    def _seed(self, shared, name, body, replace=False):
        sig = json.dumps({
            'name': name,
            'args': [{'name': 'x', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'returns': [{'name': '', 'dtype': 'int64', 'sql': 'BIGINT'}],
        })
        shared.create_function(f'id-{name}', sig, body, replace)

    def test_delete_then_gone(self):
        """@@delete removes a function so it's no longer callable."""
        shared = self._make_shared()
        self._seed(shared, 'temp_fn', 'return x')

        del_payload = json.dumps({'id': 'id-temp_fn'}).encode()
        result = dispatch_control_signal('@@delete', del_payload, shared)
        assert result.ok

        reg = shared.get_thread_local_registry()
        assert 'temp_fn' not in reg.functions

    def test_delete_failures_carry_codes(self):
        """Each @@delete failure maps to its control-signal error code."""
        shared = self._make_shared()
        self._seed(shared, 'keep_fn', 'return x')

        for payload, code in (
            (b'', 'DELETE_MISSING_PAYLOAD'),
            (b'{', 'DELETE_INVALID_PAYLOAD'),
            (b'\xff', 'DELETE_INVALID_PAYLOAD'),
            (b'{"id": 5}', 'DELETE_INVALID_PAYLOAD'),
            (b'{"name": "keep_fn"}', 'DELETE_INVALID_PAYLOAD'),
            (b'[]', 'DELETE_INVALID_PAYLOAD'),
            (b'{"id": "ghost"}', 'DELETE_FUNC_NOT_REGISTERED'),
            # An empty string is still a string id, as in the Rust server.
            (b'{"id": ""}', 'DELETE_FUNC_NOT_REGISTERED'),
        ):
            result = dispatch_control_signal('@@delete', payload, shared)
            assert not result.ok, payload
            assert result.code == code, (payload, result)

        reg = shared.get_thread_local_registry()
        assert 'keep_fn' in reg.functions

    def test_delete_then_recreate(self):
        """Delete a function and re-create it with new behavior."""
        shared = self._make_shared()
        self._seed(shared, 'morph', 'return x + 1')

        dispatch_control_signal(
            '@@delete',
            json.dumps({'id': 'id-morph'}).encode(),
            shared,
        )

        self._seed(shared, 'morph', 'return x + 999')

        reg = shared.get_thread_local_registry()
        input_data = bytes(rowdat_1._dump([ft.LONGLONG], [1], [[1]]))
        output = call_function(reg, 'morph', input_data)
        _, rows = rowdat_1._load([('r', ft.LONGLONG)], output)
        assert rows[0][0] == 1000

    def test_piped_supersession_leaves_one_code_block(self):
        """The main process applies a worker's register messages through
        the same create_function, so a fresh id with the same signature
        evicts the old block there too."""
        worker = self._make_shared()
        main = self._make_shared()
        signature = {
            'name': 'cor_fn',
            'args': [{'name': 'x', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'returns': [{'name': '', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'replace': False,
        }

        r_fd, w_fd = os.pipe()
        try:
            for id, body in (('id-old', b'return x'), ('id-new', b'return -x')):
                with tempfile.TemporaryFile() as f:
                    f.write(body)
                    f.flush()
                    _install_v3_definition(
                        {'id': id, 'signature': signature},
                        f.fileno(), worker, w_fd,
                    )
                header, blob = _read_pipe_message(r_fd)
                msg = json.loads(header)
                assert msg['action'] == 'register'
                assert blob == body
                main.create_function(
                    msg['id'], msg['signature_json'], blob.decode('utf8'),
                    msg['replace'],
                )
        finally:
            os.close(r_fd)
            os.close(w_fd)

        for shared in (worker, main):
            assert list(shared._code_blocks) == ['id-new']
            reg = shared.get_thread_local_registry()
            input_data = bytes(rowdat_1._dump([ft.LONGLONG], [1], [[5]]))
            output = call_function(reg, 'cor_fn', input_data, 'BIGINT')
            _, rows = rowdat_1._load([('r', ft.LONGLONG)], output)
            assert rows[0][0] == -5

    def test_delete_writes_to_pipe(self):
        """@@delete with pipe_write_fd sends message to parent."""
        shared = self._make_shared()
        self._seed(shared, 'pipe_del', 'return x')

        r_fd, w_fd = os.pipe()
        try:
            del_payload = json.dumps({'id': 'id-pipe_del'}).encode()
            result = dispatch_control_signal(
                '@@delete', del_payload, shared, pipe_write_fd=w_fd,
            )
            assert result.ok

            msg = _read_pipe_message(r_fd)
            assert msg is not None
            body = json.loads(msg[0])
            assert body['action'] == 'delete'
            assert msg[1] == b''
            assert body['id'] == 'id-pipe_del'
        finally:
            os.close(r_fd)
            os.close(w_fd)


# ---------------------------------------------------------------------------
# P1: Server Lifecycle (Thread Mode)
# ---------------------------------------------------------------------------

class TestServerThreadMode(unittest.TestCase):
    """Test the Server class in thread mode."""

    def setUp(self):
        import singlestoredb.functions.ext.plugin.registry as _reg_mod
        import singlestoredb.functions.ext.plugin.connection as _conn_mod
        self._orig_has_accel = _reg_mod._has_accel
        _reg_mod._has_accel = False
        _conn_mod._has_accel = False

    def tearDown(self):
        import singlestoredb.functions.ext.plugin.registry as _reg_mod
        import singlestoredb.functions.ext.plugin.connection as _conn_mod
        _reg_mod._has_accel = self._orig_has_accel
        _conn_mod._has_accel = self._orig_has_accel

    def test_thread_mode_accepts_and_handles_request(self):
        """Start server in thread mode, connect, call a UDF."""
        from concurrent.futures import ThreadPoolExecutor
        import select as _select

        shared = _make_shared_registry_with_func()

        with tempfile.TemporaryDirectory() as tmpdir:
            sock_path = os.path.join(tmpdir, 'test.sock')
            server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server_sock.bind(sock_path)
            server_sock.listen(4)

            shutdown_event = threading.Event()
            pool = ThreadPoolExecutor(max_workers=2)

            def accept_loop():
                while not shutdown_event.is_set():
                    readable, _, _ = _select.select(
                        [server_sock], [], [], 0.1,
                    )
                    if not readable:
                        continue
                    conn, _ = server_sock.accept()
                    pool.submit(
                        handle_connection, conn, shared, shutdown_event,
                    )

            server_thread = threading.Thread(target=accept_loop, daemon=True)
            server_thread.start()

            # Connect as a client
            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client.connect(sock_path)

            input_fd, output_fd = _create_temp_fds()
            server_input_fd = os.dup(input_fd)
            server_output_fd = os.dup(output_fd)

            try:
                _send_handshake(
                    client, 'triple', server_input_fd, server_output_fd,
                )

                input_data = bytes(rowdat_1._dump([ft.LONGLONG], [1], [[14]]))
                _send_udf_request(client, input_fd, input_data)

                status, data = _recv_udf_response(client, output_fd)
                assert status == STATUS_OK
                _, rows = rowdat_1._load([('r', ft.LONGLONG)], data)
                assert rows[0][0] == 42  # 14 * 3

                client.sendall(struct.pack('<Q', 0))
            finally:
                client.close()
                os.close(input_fd)
                os.close(output_fd)

            shutdown_event.set()
            server_thread.join(timeout=5)
            pool.shutdown(wait=True)
            server_sock.close()
            assert not server_thread.is_alive()


# ---------------------------------------------------------------------------
# P1: Process Mode Lifecycle
# ---------------------------------------------------------------------------

class TestServerProcessMode(unittest.TestCase):
    """Test the Server class in process mode (pre-fork).

    Uses forked worker processes that inherit the shared registry from
    the parent. We test this by directly managing the fork ourselves
    rather than going through Server.run() (which requires a real plugin module).
    """

    def setUp(self):
        import singlestoredb.functions.ext.plugin.registry as _reg_mod
        import singlestoredb.functions.ext.plugin.connection as _conn_mod
        self._orig_has_accel = _reg_mod._has_accel
        _reg_mod._has_accel = False
        _conn_mod._has_accel = False

    def tearDown(self):
        import singlestoredb.functions.ext.plugin.registry as _reg_mod
        import singlestoredb.functions.ext.plugin.connection as _conn_mod
        _reg_mod._has_accel = self._orig_has_accel
        _conn_mod._has_accel = self._orig_has_accel

    def test_process_mode_worker_handles_request(self):
        """Fork a worker, send a request through it, verify response."""
        import multiprocessing
        import select as _select

        shared = _make_shared_registry_with_func()

        with tempfile.TemporaryDirectory() as tmpdir:
            sock_path = os.path.join(tmpdir, 'test.sock')
            server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server_sock.bind(sock_path)
            server_sock.listen(4)

            def worker_main():
                """Simulated worker process (runs in a forked child)."""
                server_sock.setblocking(False)
                local_shutdown = threading.Event()

                def sigterm_handler(signum, frame):
                    local_shutdown.set()

                signal.signal(signal.SIGTERM, sigterm_handler)

                while not local_shutdown.is_set():
                    readable, _, _ = _select.select(
                        [server_sock], [], [], 0.1,
                    )
                    if not readable:
                        continue
                    try:
                        conn, _ = server_sock.accept()
                    except BlockingIOError:
                        continue
                    handle_connection(conn, shared, local_shutdown)

            try:
                ctx = multiprocessing.get_context('fork')
            except ValueError:
                self.skipTest('fork not available on this platform')

            worker = ctx.Process(target=worker_main, daemon=True)
            worker.start()

            # Give worker time to start accepting
            time.sleep(0.2)

            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client.connect(sock_path)

            input_fd, output_fd = _create_temp_fds()
            server_input_fd = os.dup(input_fd)
            server_output_fd = os.dup(output_fd)

            try:
                _send_handshake(
                    client, 'triple', server_input_fd, server_output_fd,
                )

                input_data = bytes(rowdat_1._dump([ft.LONGLONG], [1], [[7]]))
                _send_udf_request(client, input_fd, input_data)

                status, data = _recv_udf_response(client, output_fd)
                assert status == STATUS_OK
                _, rows = rowdat_1._load([('r', ft.LONGLONG)], data)
                assert rows[0][0] == 21  # 7 * 3

                client.sendall(struct.pack('<Q', 0))
            finally:
                client.close()
                os.close(input_fd)
                os.close(output_fd)

            # Shutdown worker
            os.kill(worker.pid, signal.SIGTERM)
            worker.join(timeout=5)
            server_sock.close()
            assert not worker.is_alive()


class TestProcessPoolRetire(unittest.TestCase):
    """A registry change replaces the workers without cutting off the
    connections the old ones are serving."""

    _ADD1_ENVELOPE = {
        'id': 'id-add1',
        'param_types': 'BIGINT',
        'signature': {
            'name': 'add1',
            'args': [{'name': 'x', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'returns': [{'name': '', 'dtype': 'int64', 'sql': 'BIGINT'}],
        },
    }

    def setUp(self):
        import multiprocessing
        import singlestoredb.functions.ext.plugin.registry as _reg_mod
        import singlestoredb.functions.ext.plugin.connection as _conn_mod
        try:
            self.ctx = multiprocessing.get_context('fork')
        except ValueError:
            self.skipTest('fork not available on this platform')
        self._orig_has_accel = _reg_mod._has_accel
        _reg_mod._has_accel = False
        _conn_mod._has_accel = False
        self.tmpdir = tempfile.TemporaryDirectory()
        self.sock_path = os.path.join(self.tmpdir.name, 'test.sock')
        self.server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.server_sock.bind(self.sock_path)
        self.server_sock.listen(4)
        self.server = Server({})
        self.server.shared_registry = _make_shared_registry_with_func()

    def tearDown(self):
        import singlestoredb.functions.ext.plugin.registry as _reg_mod
        import singlestoredb.functions.ext.plugin.connection as _conn_mod
        _reg_mod._has_accel = self._orig_has_accel
        _conn_mod._has_accel = self._orig_has_accel
        self.server_sock.close()
        self.tmpdir.cleanup()

    def _connect(self):
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.settimeout(10)
        client.connect(self.sock_path)
        input_fd, output_fd = _create_temp_fds()
        self.addCleanup(os.close, input_fd)
        self.addCleanup(os.close, output_fd)
        self.addCleanup(client.close)
        return client, input_fd, output_fd

    @staticmethod
    def _call(client, input_fd, output_fd, value):
        input_data = bytes(rowdat_1._dump([ft.LONGLONG], [1], [[value]]))
        _send_udf_request(client, input_fd, input_data)
        status, data = _recv_udf_response(client, output_fd)
        assert status == STATUS_OK, data
        _, rows = rowdat_1._load([('r', ft.LONGLONG)], data)
        return rows[0][0]

    def _start_pool(self, n_workers=1):
        pool = _ProcessPool(self.server, self.ctx, self.server_sock, n_workers)
        thread = threading.Thread(target=pool.run, daemon=True)
        thread.start()

        def stop():
            self.server.shutdown_event.set()
            thread.join(timeout=20)
            assert not thread.is_alive()
        self.addCleanup(stop)
        assert _wait_for(lambda: len(pool.workers) == n_workers)
        return pool

    def test_installing_connection_survives_worker_replacement(self):
        pool = self._start_pool()
        (old_proc, _), = pool.workers.values()

        client, input_fd, output_fd = self._connect()
        _send_handshake_v3(
            client, 'add1', os.dup(input_fd), os.dup(output_fd),
            self._ADD1_ENVELOPE, b'return x + 1',
        )
        assert self._call(client, input_fd, output_fd, 1) == 2

        # Main has applied the register and retired the serving worker.
        assert _wait_for(lambda: old_proc.pid in pool.retiring)
        assert self.server.shared_registry.has_definition('id-add1')
        assert old_proc.is_alive()

        # The same connection keeps working on the retired worker.
        assert self._call(client, input_fd, output_fd, 41) == 42
        client.sendall(struct.pack('<Q', 0))

        # Once its connection ends, the retired worker exits and is reaped.
        assert _wait_for(lambda: old_proc.pid not in pool.retiring)
        assert old_proc.exitcode == 0

        # Its replacement was forked with the new definition.
        client2, input_fd2, output_fd2 = self._connect()
        _send_handshake(
            client2, 'add1', os.dup(input_fd2), os.dup(output_fd2),
        )
        assert self._call(client2, input_fd2, output_fd2, 9) == 10
        client2.sendall(struct.pack('<Q', 0))

    def test_cached_register_does_not_retire(self):
        pool = self._start_pool()
        (old_proc, _), = pool.workers.values()
        shared = self.server.shared_registry
        before = shared.generation

        header = json.dumps({
            'action': 'register',
            'id': 'id-triple',
            'signature_json': json.dumps(
                {'name': 'triple', 'args': [], 'returns': []},
            ),
            'replace': False,
        }).encode()
        assert not pool._apply_message(header, b'return 0', 'test')
        assert shared.generation == before

        client, input_fd, output_fd = self._connect()
        _send_handshake(client, 'triple', os.dup(input_fd), os.dup(output_fd))
        assert self._call(client, input_fd, output_fd, 2) == 6
        client.sendall(struct.pack('<Q', 0))
        assert list(pool.workers.values())[0][0] is old_proc
        assert not pool.retiring

    def test_sigusr1_drains_connection_then_exits(self):
        r_fd, w_fd = os.pipe()
        self.addCleanup(os.close, r_fd)
        worker = self.ctx.Process(
            target=self.server._worker_process_main,
            args=(self.server_sock, 0, w_fd, [r_fd]),
            daemon=True,
        )
        worker.start()
        os.close(w_fd)
        self.addCleanup(worker.kill)

        client, input_fd, output_fd = self._connect()
        _send_handshake(client, 'triple', os.dup(input_fd), os.dup(output_fd))
        assert self._call(client, input_fd, output_fd, 1) == 3

        os.kill(worker.pid, signal.SIGUSR1)
        # A connection queued after SIGUSR1 must never be accepted.
        late, _, _ = self._connect()
        time.sleep(0.3)

        assert worker.is_alive()
        assert self._call(client, input_fd, output_fd, 5) == 15
        client.sendall(struct.pack('<Q', 0))

        worker.join(timeout=10)
        assert worker.exitcode == 0
        # The queued connection is still in the backlog, unaccepted.
        conn = self.server_sock.accept()[0]
        conn.close()

    def test_unreadable_frame_retires_active_worker(self):
        pool = _ProcessPool(self.server, self.ctx, self.server_sock, 1)
        r_fd, w_fd = os.pipe()
        self.addCleanup(os.close, w_fd)
        proc = unittest.mock.MagicMock(pid=-1)
        proc.is_alive.return_value = False
        pool.workers[0] = (proc, r_fd)
        replacement = (unittest.mock.MagicMock(), -1)
        os.write(w_fd, _PIPE_PREFIX.pack(_MAX_PIPE_HEADER + 1, 0))
        with unittest.mock.patch.object(
            pool, '_spawn', return_value=replacement,
        ) as spawn:
            assert not pool._poll_once()
        spawn.assert_called_once_with(0)
        assert pool.workers[0] is replacement
        assert pool.retiring[-1].proc is proc
        assert pool.retiring[-1].pipe_r is None
        # main closed its read end
        with self.assertRaises(OSError):
            os.fstat(r_fd)

    @staticmethod
    def _dead_proc():
        proc = unittest.mock.MagicMock(pid=-1)
        proc.is_alive.return_value = False
        return proc

    def _register_frame(self, envelope, body):
        header = json.dumps({
            'action': 'register',
            'id': envelope['id'],
            'signature_json': json.dumps(envelope['signature']),
            'replace': False,
        }).encode()
        return header, body

    def _drain(self, pool, retiring):
        """Poll until ``retiring``'s pipe is closed; return change count."""
        changes = 0
        for _ in range(10):
            if retiring.pipe_r is None:
                return changes
            changes += pool._poll_once()
        self.fail('pipe was never drained to EOF')

    def test_dead_retiring_worker_pipe_drained_before_reap(self):
        pool = _ProcessPool(self.server, self.ctx, self.server_sock, 1)
        r_fd, w_fd = os.pipe()
        _write_pipe_message(
            w_fd, *self._register_frame(self._ADD1_ENVELOPE, b'return x + 1'),
        )
        _write_pipe_message(
            w_fd, json.dumps({'action': 'delete', 'id': 'id-triple'}).encode(),
        )
        os.close(w_fd)
        pool._retire(0, self._dead_proc(), r_fd)
        retiring = pool.retiring[-1]

        # Exited, but two frames are still queued: not reaped yet.
        pool._reap_retiring()
        assert pool.retiring.get(-1) is retiring
        assert retiring.pipe_r == r_fd

        assert self._drain(pool, retiring) == 2
        reg = self.server.shared_registry.get_thread_local_registry()
        assert 'add1' in reg.functions
        assert 'triple' not in reg.functions

        pool._reap_retiring()
        assert -1 not in pool.retiring

    def test_dead_pipe_closed_after_deadline(self):
        import singlestoredb.functions.ext.plugin.server as _server_mod
        pool = _ProcessPool(self.server, self.ctx, self.server_sock, 1)
        r_fd, w_fd = os.pipe()
        # A stray write end keeps EOF from ever arriving.
        self.addCleanup(os.close, w_fd)
        pool._retire(0, self._dead_proc(), r_fd)
        with unittest.mock.patch.object(_server_mod, '_EXITED_PIPE_GRACE', 0):
            pool._reap_retiring()
        assert -1 not in pool.retiring
        with self.assertRaises(OSError):
            os.fstat(r_fd)

    def test_crashed_active_worker_pipe_drained(self):
        pool = _ProcessPool(self.server, self.ctx, self.server_sock, 1)
        r_fd, w_fd = os.pipe()
        _write_pipe_message(
            w_fd, *self._register_frame(self._ADD1_ENVELOPE, b'return x + 1'),
        )
        os.close(w_fd)
        proc = self._dead_proc()
        pool.workers[0] = (proc, r_fd)
        # The replacement's pipe stays empty and open, so it never fires.
        rep_r, rep_w = os.pipe()
        self.addCleanup(os.close, rep_r)
        self.addCleanup(os.close, rep_w)
        replacement = (unittest.mock.MagicMock(), rep_r)
        with unittest.mock.patch.object(
            pool, '_spawn', return_value=replacement,
        ) as spawn:
            pool._respawn_dead()
        spawn.assert_called_once_with(0)
        assert pool.workers[0] is replacement
        retiring = pool.retiring[-1]
        assert retiring.proc is proc
        assert retiring.pipe_r == r_fd

        assert self._drain(pool, retiring) == 1
        reg = self.server.shared_registry.get_thread_local_registry()
        assert 'add1' in reg.functions


# ---------------------------------------------------------------------------
# P1: rowdat_1 roundtrip with mixed types including NULL, datetime, decimal
# ---------------------------------------------------------------------------

class TestRowdat1MixedRoundtrip(unittest.TestCase):
    """Roundtrip encoding/decoding of multi-column rows with diverse types."""

    def test_multi_column_with_nulls(self):
        """Encode/decode rows with INT, STRING, DOUBLE, some NULLs."""
        colspec = [
            ('id', ft.LONGLONG),
            ('name', ft.STRING),
            ('score', ft.DOUBLE),
        ]
        col_types = [c[1] for c in colspec]
        rows = [
            [1, 'alice', 99.5],
            [2, None, 88.0],
            [3, 'charlie', None],
            [4, None, None],
        ]
        row_ids = [10, 20, 30, 40]

        encoded = bytes(rowdat_1._dump(col_types, row_ids, rows))
        dec_ids, dec_rows = rowdat_1._load(colspec, encoded)

        assert dec_ids == row_ids
        assert dec_rows[0] == [1, 'alice', 99.5]
        assert dec_rows[1] == [2, None, 88.0]
        assert dec_rows[2] == [3, 'charlie', None]
        assert dec_rows[3] == [4, None, None]

    def test_datetime_date_time_decimal(self):
        """Encode/decode datetime, date, time, decimal types."""
        colspec = [
            ('dt', ft.DATETIME),
            ('d', ft.DATE),
            ('t', ft.TIME),
            ('dec', ft.NEWDECIMAL),
        ]
        col_types = [c[1] for c in colspec]
        dt = datetime.datetime(2024, 6, 15, 12, 30, 45, 123456)
        d = datetime.date(2024, 6, 15)
        td = datetime.timedelta(hours=3, minutes=30, seconds=15)
        dec_val = decimal.Decimal('123.456')

        rows = [
            [dt, d, td, str(dec_val)],
            [None, None, None, None],
        ]
        row_ids = [1, 2]

        encoded = bytes(rowdat_1._dump(col_types, row_ids, rows))
        dec_ids, dec_rows = rowdat_1._load(colspec, encoded)

        assert dec_ids == [1, 2]
        assert dec_rows[0][0] == dt
        assert dec_rows[0][1] == d
        assert dec_rows[0][2] == td
        assert dec_rows[0][3] == dec_val
        assert dec_rows[1] == [None, None, None, None]

    def test_binary_data(self):
        """Roundtrip with binary (negative string type) data."""
        colspec = [('data', -ft.STRING)]
        col_types = [colspec[0][1]]
        rows = [[b'\x00\x01\x02\xff'], [None], [b'']]
        row_ids = [1, 2, 3]

        encoded = bytes(rowdat_1._dump(col_types, row_ids, rows))
        dec_ids, dec_rows = rowdat_1._load(colspec, encoded)

        assert dec_rows[0][0] == b'\x00\x01\x02\xff'
        assert dec_rows[1][0] is None
        assert dec_rows[2][0] == b''

    def test_all_null_row(self):
        """Row where every column is NULL."""
        colspec = [
            ('a', ft.LONGLONG),
            ('b', ft.STRING),
            ('c', ft.DOUBLE),
            ('d', ft.DATETIME),
        ]
        col_types = [c[1] for c in colspec]
        rows = [[None, None, None, None]]
        row_ids = [99]

        encoded = bytes(rowdat_1._dump(col_types, row_ids, rows))
        dec_ids, dec_rows = rowdat_1._load(colspec, encoded)
        assert dec_rows[0] == [None, None, None, None]

    def test_many_columns_null_bitmap(self):
        """10 columns to exercise multi-byte null handling."""
        colspec = [(f'c{i}', ft.LONGLONG) for i in range(10)]
        col_types = [ft.LONGLONG] * 10
        row = [None if i % 2 == 0 else i for i in range(10)]
        rows = [row]
        row_ids = [1]

        encoded = bytes(rowdat_1._dump(col_types, row_ids, rows))
        dec_ids, dec_rows = rowdat_1._load(colspec, encoded)
        assert dec_rows[0] == row


# ---------------------------------------------------------------------------
# P1: call_function dispatch matrix (scalar path)
# ---------------------------------------------------------------------------

class TestCallFunctionDispatch(unittest.TestCase):
    """Test call_function with various type combinations."""

    def _make_registry_with(self, name, arg_dtypes, ret_dtypes, body):
        """Helper to create a registry with a dynamically registered func."""
        shared = SharedRegistry()
        base_reg = FunctionRegistry()
        shared.set_base_registry(base_reg)
        sig = json.dumps({
            'name': name,
            'args': [
                {'name': f'a{i}', 'dtype': d, 'sql': 'X'}
                for i, d in enumerate(arg_dtypes)
            ],
            'returns': [
                {'name': f'r{i}', 'dtype': d, 'sql': 'X'}
                for i, d in enumerate(ret_dtypes)
            ],
        })
        shared.create_function(f'id-{name}', sig, body, False)
        return shared.get_thread_local_registry()

    def test_int64_passthrough(self):
        reg = self._make_registry_with(
            'f', ['int64'], ['int64'], 'return a0',
        )
        input_data = bytes(rowdat_1._dump([ft.LONGLONG], [1], [[42]]))
        output = call_function(reg, 'f', input_data)
        _, rows = rowdat_1._load([('r', ft.LONGLONG)], output)
        assert rows[0][0] == 42

    def test_float64_computation(self):
        reg = self._make_registry_with(
            'f', ['float64'], ['float64'], 'return a0 * 2.5',
        )
        input_data = bytes(rowdat_1._dump([ft.DOUBLE], [1], [[4.0]]))
        output = call_function(reg, 'f', input_data)
        _, rows = rowdat_1._load([('r', ft.DOUBLE)], output)
        assert abs(rows[0][0] - 10.0) < 1e-9

    def test_string_passthrough(self):
        reg = self._make_registry_with(
            'f', ['str'], ['str'], 'return a0.upper()',
        )
        input_data = bytes(rowdat_1._dump([ft.STRING], [1], [['hello']]))
        output = call_function(reg, 'f', input_data)
        _, rows = rowdat_1._load([('r', ft.STRING)], output)
        assert rows[0][0] == 'HELLO'

    def test_null_handling(self):
        reg = self._make_registry_with(
            'f', ['int64?'], ['int64?'], 'return None if a0 is None else a0 + 1',
        )
        input_data = bytes(rowdat_1._dump([ft.LONGLONG], [1, 2], [[5], [None]]))
        output = call_function(reg, 'f', input_data)
        _, rows = rowdat_1._load([('r', ft.LONGLONG)], output)
        assert rows[0][0] == 6
        assert rows[1][0] is None

    def test_multi_arg(self):
        reg = self._make_registry_with(
            'f', ['int64', 'int64'], ['int64'],
            'return a0 + a1',
        )
        input_data = bytes(
            rowdat_1._dump(
                [ft.LONGLONG, ft.LONGLONG], [1], [[3, 7]],
            ),
        )
        output = call_function(reg, 'f', input_data)
        _, rows = rowdat_1._load([('r', ft.LONGLONG)], output)
        assert rows[0][0] == 10

    def test_multiple_rows(self):
        reg = self._make_registry_with(
            'f', ['int64'], ['int64'], 'return a0 * a0',
        )
        input_data = bytes(
            rowdat_1._dump(
                [ft.LONGLONG], [1, 2, 3], [[2], [3], [4]],
            ),
        )
        output = call_function(reg, 'f', input_data)
        _, rows = rowdat_1._load([('r', ft.LONGLONG)], output)
        assert [r[0] for r in rows] == [4, 9, 16]

    def test_unknown_function_raises(self):
        reg = self._make_registry_with(
            'f', ['int64'], ['int64'], 'return a0',
        )
        with self.assertRaises(ValueError) as ctx:
            call_function(reg, 'nonexistent', b'')
        assert 'unknown function' in str(ctx.exception)


# ---------------------------------------------------------------------------
# P1: Columnar/Vector dispatch
# ---------------------------------------------------------------------------

class TestVectorDispatch(unittest.TestCase):
    """Test call_function with vectorized/columnar UDF path."""

    def _make_vector_registry(self, name, body):
        """Create registry with a numpy-annotated vector-mode UDF."""
        import types
        import sys

        full_code = (
            'import numpy as np\n'
            'import numpy.typing as npt\n'
            'from singlestoredb.functions import udf\n'
            '\n'
            '@udf\n'
            f'def {name}(x: npt.NDArray[np.int64]) -> npt.NDArray[np.int64]:\n'
            f'    {body}\n'
        )
        mod_name = f'_test_vector_{name}'
        compiled = compile(full_code, f'<{mod_name}>', 'exec')
        module = types.ModuleType(mod_name)
        module.__file__ = f'<{mod_name}>'
        sys.modules[mod_name] = module
        exec(compiled, module.__dict__)

        reg = FunctionRegistry()
        reg.initialize(plugin_module=module)

        del sys.modules[mod_name]
        return reg

    def test_numpy_vector_dispatch(self):
        """Vectorized UDF receives numpy arrays, returns numpy array."""
        reg = self._make_vector_registry('vec_double', 'return x * 2')

        if 'vec_double' not in reg.functions:
            self.skipTest('numpy NDArray annotation not recognized by get_signature')

        input_data = bytes(
            rowdat_1._dump(
                [ft.LONGLONG], [1, 2, 3, 4], [[10], [20], [30], [40]],
            ),
        )
        output = call_function(reg, 'vec_double', input_data)
        _, rows = rowdat_1._load([('r', ft.LONGLONG)], output)
        assert [r[0] for r in rows] == [20, 40, 60, 80]


# ---------------------------------------------------------------------------
# P1: Pipe message protocol
# ---------------------------------------------------------------------------

class TestPipeMessages(unittest.TestCase):
    """Test the pipe message wire format for process mode communication."""

    @staticmethod
    def _roundtrip(header, blob=b''):
        r_fd, w_fd = os.pipe()
        try:
            # Write in a thread to avoid pipe buffer deadlock
            t = threading.Thread(
                target=lambda: _write_pipe_message(w_fd, header, blob),
            )
            t.start()
            result = _read_pipe_message(r_fd)
            t.join()
            return result
        finally:
            os.close(r_fd)
            os.close(w_fd)

    def test_roundtrip(self):
        assert self._roundtrip(b'hello', b'world') == (b'hello', b'world')

    def test_header_only(self):
        assert self._roundtrip(b'hello world') == (b'hello world', b'')

    def test_large_payload(self):
        payload = b'X' * 100_000
        assert self._roundtrip(payload, payload) == (payload, payload)

    def test_blob_over_16_mib(self):
        # The old single-buffer frame capped everything at 16 MiB.
        blob = b'Y' * (17 * 1024 * 1024)
        assert self._roundtrip(b'{}', blob) == (b'{}', blob)

    def test_blob_at_body_cap(self):
        blob = b'Z' * _MAX_V3_BODY_BYTES
        assert self._roundtrip(b'{}', blob) == (b'{}', blob)

    def test_eof_returns_none(self):
        r_fd, w_fd = os.pipe()
        os.close(w_fd)
        result = _read_pipe_message(r_fd)
        os.close(r_fd)
        assert result is None

    def test_truncated_frame_returns_none(self):
        r_fd, w_fd = os.pipe()
        try:
            os.write(w_fd, _PIPE_PREFIX.pack(4, 4) + b'abcd' + b'ef')
            os.close(w_fd)
            w_fd = -1
            assert _read_pipe_message(r_fd) is None
        finally:
            os.close(r_fd)
            if w_fd >= 0:
                os.close(w_fd)

    def _assert_frame_rejected(self, header_len, blob_len):
        r_fd, w_fd = os.pipe()
        try:
            os.write(w_fd, _PIPE_PREFIX.pack(header_len, blob_len))
            with self.assertRaises(PipeFrameError):
                _read_pipe_message(r_fd)
        finally:
            os.close(r_fd)
            os.close(w_fd)

    def test_oversized_header_is_an_error_not_a_short_read(self):
        self._assert_frame_rejected(_MAX_PIPE_HEADER + 1, 0)

    def test_oversized_blob_is_an_error_not_a_short_read(self):
        self._assert_frame_rejected(2, _MAX_V3_BODY_BYTES + 1)

    def test_multiple_messages(self):
        r_fd, w_fd = os.pipe()
        try:
            msgs = [(b'msg1', b''), (b'msg2', b'body2'), (b'msg3', b'b3')]
            for m in msgs:
                _write_pipe_message(w_fd, *m)
            for m in msgs:
                assert _read_pipe_message(r_fd) == m
        finally:
            os.close(r_fd)
            os.close(w_fd)

    def test_escape_heavy_body_travels_raw(self):
        """A body JSON would escape sixfold still fits: it isn't JSON."""
        shared = SharedRegistry()
        shared.set_base_registry(FunctionRegistry())
        signature = {
            'name': 'esc_fn',
            'args': [{'name': 'x', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'returns': [{'name': '', 'dtype': 'int64', 'sql': 'BIGINT'}],
        }
        # A comment of control characters: 3 MiB raw, 18 MiB as JSON.
        body = b'return x  # ' + b'\x01' * (3 * 1024 * 1024)
        r_fd, w_fd = os.pipe()
        result = []
        reader = threading.Thread(
            target=lambda: result.append(_read_pipe_message(r_fd)),
        )
        reader.start()
        try:
            with tempfile.TemporaryFile() as f:
                f.write(body)
                f.flush()
                _install_v3_definition(
                    {'id': 'id-esc', 'signature': signature},
                    f.fileno(), shared, w_fd,
                )
            reader.join(timeout=10)
        finally:
            os.close(w_fd)
            reader.join(timeout=5)
            os.close(r_fd)
        header, blob = result[0]
        assert blob == body
        assert 'code' not in json.loads(header)
        assert len(header) < 1024


# ---------------------------------------------------------------------------
# P1: SharedRegistry generation and thread-local caching
# ---------------------------------------------------------------------------

class TestSharedRegistryGeneration(unittest.TestCase):
    """Test generation-based caching in SharedRegistry."""

    def test_generation_increments_on_create(self):
        shared = SharedRegistry()
        shared.set_base_registry(FunctionRegistry())
        assert shared.generation == 0

        sig = json.dumps({
            'name': 'f1',
            'args': [{'name': 'x', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'returns': [{'name': '', 'dtype': 'int64', 'sql': 'BIGINT'}],
        })
        shared.create_function('id-f1', sig, 'return x', False)
        assert shared.generation == 1

    def test_generation_increments_on_delete(self):
        shared = SharedRegistry()
        shared.set_base_registry(FunctionRegistry())
        sig = json.dumps({
            'name': 'f1',
            'args': [{'name': 'x', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'returns': [{'name': '', 'dtype': 'int64', 'sql': 'BIGINT'}],
        })
        shared.create_function('id-f1', sig, 'return x', False)
        assert shared.generation == 1

        shared.delete_function('id-f1')
        assert shared.generation == 2

    def test_thread_local_cache_refreshes(self):
        shared = SharedRegistry()
        shared.set_base_registry(FunctionRegistry())

        sig = json.dumps({
            'name': 'f1',
            'args': [{'name': 'x', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'returns': [{'name': '', 'dtype': 'int64', 'sql': 'BIGINT'}],
        })
        shared.create_function('id-f1', sig, 'return x', False)
        reg1 = shared.get_thread_local_registry()
        assert 'f1' in reg1.functions

        shared.delete_function('id-f1')
        reg2 = shared.get_thread_local_registry()
        assert 'f1' not in reg2.functions

    def test_cached_registry_reused_when_unchanged(self):
        shared = SharedRegistry()
        shared.set_base_registry(FunctionRegistry())
        sig = json.dumps({
            'name': 'f1',
            'args': [{'name': 'x', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'returns': [{'name': '', 'dtype': 'int64', 'sql': 'BIGINT'}],
        })
        shared.create_function('id-f1', sig, 'return x', False)

        reg1 = shared.get_thread_local_registry()
        reg2 = shared.get_thread_local_registry()
        assert reg1 is reg2


if __name__ == '__main__':
    unittest.main()
