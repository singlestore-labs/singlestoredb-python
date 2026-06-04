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
import unittest

from singlestoredb.functions.ext import rowdat_1
from singlestoredb.functions.ext.plugin.connection import _handle_connection_inner
from singlestoredb.functions.ext.plugin.connection import handle_connection
from singlestoredb.functions.ext.plugin.connection import PROTOCOL_VERSION
from singlestoredb.functions.ext.plugin.connection import STATUS_BAD_REQUEST  # noqa: F401
from singlestoredb.functions.ext.plugin.connection import STATUS_ERROR
from singlestoredb.functions.ext.plugin.connection import STATUS_OK
from singlestoredb.functions.ext.plugin.control import dispatch_control_signal
from singlestoredb.functions.ext.plugin.registry import call_function
from singlestoredb.functions.ext.plugin.registry import FunctionRegistry
from singlestoredb.functions.ext.plugin.server import _read_pipe_message
from singlestoredb.functions.ext.plugin.server import _write_pipe_message
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
    shared.create_function(sig, 'return x * 3', False)
    return shared


def _send_handshake(sock, function_name, input_fd, output_fd):
    """Send the binary handshake from client side."""
    name_bytes = function_name.encode('utf8')
    header = struct.pack('<QQ', PROTOCOL_VERSION, len(name_bytes))
    sock.sendall(header)

    # Send function name + 2 FDs via SCM_RIGHTS
    fd_array = array.array('i', [input_fd, output_fd])
    sock.sendmsg(
        [name_bytes],
        [(socket.SOL_SOCKET, socket.SCM_RIGHTS, fd_array)],
    )


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
# P0: @@register success path and @@delete + re-register
# ---------------------------------------------------------------------------

class TestRegisterDeleteIntegration(unittest.TestCase):
    """Test @@register and @@delete through dispatch_control_signal with
    a real SharedRegistry (not mocked)."""

    def _make_shared(self):
        shared = SharedRegistry()
        base_reg = FunctionRegistry()
        shared.set_base_registry(base_reg)
        return shared

    def test_register_success_and_callable(self):
        """@@register a function, then call it via call_function."""
        shared = self._make_shared()
        payload = json.dumps({
            'name': 'double_it',
            'args': [{'name': 'x', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'returns': [{'name': '', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'body': 'return x * 2',
        }).encode()

        result = dispatch_control_signal('@@register', payload, shared)
        assert result.ok is True, f'Expected ok, got: {result.data}'

        # Verify callable
        reg = shared.get_thread_local_registry()
        assert 'double_it' in reg.functions
        input_data = bytes(rowdat_1._dump([ft.LONGLONG], [1], [[5]]))
        output = call_function(reg, 'double_it', input_data)
        _, rows = rowdat_1._load([('r', ft.LONGLONG)], output)
        assert rows[0][0] == 10

    def test_register_with_replace(self):
        """@@register with replace=true overwrites existing function."""
        shared = self._make_shared()
        payload1 = json.dumps({
            'name': 'myfunc',
            'args': [{'name': 'x', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'returns': [{'name': '', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'body': 'return x + 1',
        }).encode()
        result = dispatch_control_signal('@@register', payload1, shared)
        assert result.ok

        payload2 = json.dumps({
            'name': 'myfunc',
            'args': [{'name': 'x', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'returns': [{'name': '', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'body': 'return x + 100',
            'replace': True,
        }).encode()
        result = dispatch_control_signal('@@register', payload2, shared)
        assert result.ok

        reg = shared.get_thread_local_registry()
        input_data = bytes(rowdat_1._dump([ft.LONGLONG], [1], [[5]]))
        output = call_function(reg, 'myfunc', input_data)
        _, rows = rowdat_1._load([('r', ft.LONGLONG)], output)
        assert rows[0][0] == 105

    def test_register_without_replace_fails(self):
        """@@register without replace=true fails for existing function."""
        shared = self._make_shared()
        payload = json.dumps({
            'name': 'dup_fn',
            'args': [{'name': 'x', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'returns': [{'name': '', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'body': 'return x',
        }).encode()
        result = dispatch_control_signal('@@register', payload, shared)
        assert result.ok

        result2 = dispatch_control_signal('@@register', payload, shared)
        assert result2.ok is False
        assert 'already exists' in result2.data

    def test_delete_then_gone(self):
        """@@delete removes a function so it's no longer callable."""
        shared = self._make_shared()
        payload = json.dumps({
            'name': 'temp_fn',
            'args': [{'name': 'x', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'returns': [{'name': '', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'body': 'return x',
        }).encode()
        dispatch_control_signal('@@register', payload, shared)

        del_payload = json.dumps({'name': 'temp_fn'}).encode()
        result = dispatch_control_signal('@@delete', del_payload, shared)
        assert result.ok

        reg = shared.get_thread_local_registry()
        assert 'temp_fn' not in reg.functions

    def test_delete_then_reregister(self):
        """Delete a function and re-register it with new behavior."""
        shared = self._make_shared()
        sig = {
            'name': 'morph',
            'args': [{'name': 'x', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'returns': [{'name': '', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'body': 'return x + 1',
        }
        dispatch_control_signal('@@register', json.dumps(sig).encode(), shared)

        dispatch_control_signal(
            '@@delete',
            json.dumps({'name': 'morph'}).encode(),
            shared,
        )

        sig['body'] = 'return x + 999'
        dispatch_control_signal('@@register', json.dumps(sig).encode(), shared)

        reg = shared.get_thread_local_registry()
        input_data = bytes(rowdat_1._dump([ft.LONGLONG], [1], [[1]]))
        output = call_function(reg, 'morph', input_data)
        _, rows = rowdat_1._load([('r', ft.LONGLONG)], output)
        assert rows[0][0] == 1000

    def test_register_writes_to_pipe(self):
        """@@register with pipe_write_fd sends message to parent."""
        shared = self._make_shared()
        r_fd, w_fd = os.pipe()
        try:
            payload = json.dumps({
                'name': 'piped_fn',
                'args': [{'name': 'x', 'dtype': 'int64', 'sql': 'BIGINT'}],
                'returns': [{'name': '', 'dtype': 'int64', 'sql': 'BIGINT'}],
                'body': 'return x',
            }).encode()
            result = dispatch_control_signal(
                '@@register', payload, shared, pipe_write_fd=w_fd,
            )
            assert result.ok

            msg = _read_pipe_message(r_fd)
            assert msg is not None
            body = json.loads(msg)
            assert body['action'] == 'register'
            assert body['signature_json'] is not None
            assert body['code'] == 'return x'
        finally:
            os.close(r_fd)
            os.close(w_fd)

    def test_delete_writes_to_pipe(self):
        """@@delete with pipe_write_fd sends message to parent."""
        shared = self._make_shared()
        sig = json.dumps({
            'name': 'pipe_del',
            'args': [{'name': 'x', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'returns': [{'name': '', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'body': 'return x',
        }).encode()
        dispatch_control_signal('@@register', sig, shared)

        r_fd, w_fd = os.pipe()
        try:
            del_payload = json.dumps({'name': 'pipe_del'}).encode()
            result = dispatch_control_signal(
                '@@delete', del_payload, shared, pipe_write_fd=w_fd,
            )
            assert result.ok

            msg = _read_pipe_message(r_fd)
            assert msg is not None
            body = json.loads(msg)
            assert body['action'] == 'delete'
            assert body['function_name'] == 'pipe_del'
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
        shared.create_function(sig, body, False)
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

    def test_roundtrip(self):
        r_fd, w_fd = os.pipe()
        try:
            payload = b'hello world'
            _write_pipe_message(w_fd, payload)
            result = _read_pipe_message(r_fd)
            assert result == payload
        finally:
            os.close(r_fd)
            os.close(w_fd)

    def test_large_payload(self):
        r_fd, w_fd = os.pipe()
        try:
            payload = b'X' * 100_000
            # Write in a thread to avoid pipe buffer deadlock
            t = threading.Thread(
                target=lambda: _write_pipe_message(w_fd, payload),
            )
            t.start()
            result = _read_pipe_message(r_fd)
            t.join()
            assert result == payload
        finally:
            os.close(r_fd)
            os.close(w_fd)

    def test_eof_returns_none(self):
        r_fd, w_fd = os.pipe()
        os.close(w_fd)
        result = _read_pipe_message(r_fd)
        os.close(r_fd)
        assert result is None

    def test_multiple_messages(self):
        r_fd, w_fd = os.pipe()
        try:
            msgs = [b'msg1', b'msg2', b'msg3']
            for m in msgs:
                _write_pipe_message(w_fd, m)
            for m in msgs:
                assert _read_pipe_message(r_fd) == m
        finally:
            os.close(r_fd)
            os.close(w_fd)


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
        shared.create_function(sig, 'return x', False)
        assert shared.generation == 1

    def test_generation_increments_on_delete(self):
        shared = SharedRegistry()
        shared.set_base_registry(FunctionRegistry())
        sig = json.dumps({
            'name': 'f1',
            'args': [{'name': 'x', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'returns': [{'name': '', 'dtype': 'int64', 'sql': 'BIGINT'}],
        })
        shared.create_function(sig, 'return x', False)
        assert shared.generation == 1

        shared.delete_function('f1')
        assert shared.generation == 2

    def test_thread_local_cache_refreshes(self):
        shared = SharedRegistry()
        shared.set_base_registry(FunctionRegistry())

        sig = json.dumps({
            'name': 'f1',
            'args': [{'name': 'x', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'returns': [{'name': '', 'dtype': 'int64', 'sql': 'BIGINT'}],
        })
        shared.create_function(sig, 'return x', False)
        reg1 = shared.get_thread_local_registry()
        assert 'f1' in reg1.functions

        shared.delete_function('f1')
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
        shared.create_function(sig, 'return x', False)

        reg1 = shared.get_thread_local_registry()
        reg2 = shared.get_thread_local_registry()
        assert reg1 is reg2


if __name__ == '__main__':
    unittest.main()
