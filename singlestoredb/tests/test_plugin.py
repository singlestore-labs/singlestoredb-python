#!/usr/bin/env python
# type: ignore
"""Tests for the plugin UDF server components.

Covers: _recv_exact_py, control signal dispatch, and lazy imports.
These are unit tests that do not require a database connection.
"""
import json
import socket
import threading
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

from singlestoredb.functions.ext.plugin.connection import _recv_exact_py
from singlestoredb.functions.ext.plugin.control import dispatch_control_signal
from singlestoredb.utils._lazy_import import get_numpy
from singlestoredb.utils._lazy_import import get_pandas
from singlestoredb.utils._lazy_import import get_polars
from singlestoredb.utils._lazy_import import get_pyarrow


class TestRecvExactPy(unittest.TestCase):

    def test_exact_read(self):
        a, b = socket.socketpair()
        try:
            b.sendall(b'hello')
            result = _recv_exact_py(a, 5)
            assert result == b'hello'
        finally:
            a.close()
            b.close()

    def test_eof_returns_none(self):
        a, b = socket.socketpair()
        try:
            b.close()
            result = _recv_exact_py(a, 5)
            assert result is None
        finally:
            a.close()

    def test_fragmented_read(self):
        a, b = socket.socketpair()
        try:
            data = b'A' * 10000
            t = threading.Thread(target=lambda: (b.sendall(data), b.close()))
            t.start()
            result = _recv_exact_py(a, len(data))
            t.join()
            assert result == data
        finally:
            a.close()

    def test_timeout_at_start_raises(self):
        a, b = socket.socketpair()
        try:
            a.settimeout(0.01)
            with self.assertRaises(TimeoutError):
                _recv_exact_py(a, 5)
        finally:
            a.close()
            b.close()

    def test_timeout_restores_after_success(self):
        a, b = socket.socketpair()
        try:
            a.settimeout(5.0)
            b.sendall(b'test')
            result = _recv_exact_py(a, 4)
            assert result == b'test'
            assert a.gettimeout() == 5.0
        finally:
            a.close()
            b.close()

    def test_timeout_restores_after_eof(self):
        a, b = socket.socketpair()
        try:
            a.settimeout(5.0)
            b.close()
            result = _recv_exact_py(a, 5)
            assert result is None
            assert a.gettimeout() == 5.0
        finally:
            a.close()

    def test_zero_length_returns_empty(self):
        a, b = socket.socketpair()
        try:
            result = _recv_exact_py(a, 0)
            assert result == b''
        finally:
            a.close()
            b.close()


class TestControlSignalDispatch(unittest.TestCase):

    def _make_shared_registry(self):
        mock_reg = MagicMock()
        mock_reg.functions = {}
        mock_shared = MagicMock()
        mock_shared.get_thread_local_registry.return_value = mock_reg
        return mock_shared

    def test_health(self):
        shared = self._make_shared_registry()
        result = dispatch_control_signal('@@health', b'', shared)
        assert result.ok is True
        data = json.loads(result.data)
        assert data['status'] == 'ok'

    def test_functions_empty(self):
        shared = self._make_shared_registry()
        with patch(
            'singlestoredb.functions.ext.plugin.control'
            '.describe_functions_json',
            return_value='[]',
        ):
            result = dispatch_control_signal('@@functions', b'', shared)
        assert result.ok is True
        data = json.loads(result.data)
        assert data['functions'] == []

    def test_unknown_signal(self):
        shared = self._make_shared_registry()
        result = dispatch_control_signal('@@unknown', b'', shared)
        assert result.ok is False
        assert 'Unknown control signal' in result.data

    def test_register_missing_payload(self):
        shared = self._make_shared_registry()
        result = dispatch_control_signal('@@register', b'', shared)
        assert result.ok is False
        assert 'Missing registration payload' in result.data

    def test_register_invalid_json(self):
        shared = self._make_shared_registry()
        result = dispatch_control_signal('@@register', b'not json', shared)
        assert result.ok is False
        assert 'Invalid JSON' in result.data

    def test_register_missing_function_name(self):
        shared = self._make_shared_registry()
        payload = json.dumps({'args': [], 'returns': [], 'body': 'x'}).encode()
        result = dispatch_control_signal('@@register', payload, shared)
        assert result.ok is False
        assert 'function_name' in result.data

    def test_register_missing_args(self):
        shared = self._make_shared_registry()
        payload = json.dumps({
            'function_name': 'f', 'returns': [], 'body': 'x',
        }).encode()
        result = dispatch_control_signal('@@register', payload, shared)
        assert result.ok is False
        assert 'args' in result.data

    def test_register_missing_returns(self):
        shared = self._make_shared_registry()
        payload = json.dumps({
            'function_name': 'f', 'args': [], 'body': 'x',
        }).encode()
        result = dispatch_control_signal('@@register', payload, shared)
        assert result.ok is False
        assert 'returns' in result.data

    def test_register_missing_body(self):
        shared = self._make_shared_registry()
        payload = json.dumps({
            'function_name': 'f', 'args': [], 'returns': [],
        }).encode()
        result = dispatch_control_signal('@@register', payload, shared)
        assert result.ok is False
        assert 'body' in result.data

    def test_register_args_not_list(self):
        shared = self._make_shared_registry()
        payload = json.dumps({
            'function_name': 'f', 'args': 'notalist',
            'returns': [], 'body': 'x',
        }).encode()
        result = dispatch_control_signal('@@register', payload, shared)
        assert result.ok is False
        assert 'args' in result.data


class TestLazyImport(unittest.TestCase):

    def test_get_numpy_returns_module(self):
        np = get_numpy()
        if np is not None:
            assert hasattr(np, 'ndarray')

    def test_get_pandas_returns_module(self):
        pd = get_pandas()
        if pd is not None:
            assert hasattr(pd, 'DataFrame')

    def test_get_polars_returns_module_or_none(self):
        pl = get_polars()
        if pl is not None:
            assert hasattr(pl, 'DataFrame')

    def test_get_pyarrow_returns_module_or_none(self):
        pa = get_pyarrow()
        if pa is not None:
            assert hasattr(pa, 'Table')

    def test_caching(self):
        result1 = get_numpy()
        result2 = get_numpy()
        assert result1 is result2


if __name__ == '__main__':
    unittest.main()
