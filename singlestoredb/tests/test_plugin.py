#!/usr/bin/env python
# type: ignore
"""Tests for the plugin UDF server components.

Covers: _recv_exact_py, control signal dispatch, and lazy imports.
These are unit tests that do not require a database connection.
"""
import json
import socket
import struct
import threading
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

from singlestoredb.functions.ext.plugin.connection import _handle_connection_inner
from singlestoredb.functions.ext.plugin.connection import _MAX_FUNCTION_NAME_LEN
from singlestoredb.functions.ext.plugin.connection import _recv_exact_py
from singlestoredb.functions.ext.plugin.connection import PROTOCOL_VERSION
from singlestoredb.functions.ext.plugin.control import dispatch_control_signal
from singlestoredb.functions.ext.plugin.registry import FunctionExistsError
from singlestoredb.functions.ext.plugin.registry import FunctionNotDynamicError
from singlestoredb.functions.ext.plugin.registry import FunctionNotFoundError
from singlestoredb.functions.ext.plugin.registry import FunctionRegistry
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
        body = json.loads(result.data)
        assert body['code'] == 'UNKNOWN_SIGNAL'
        assert 'Unknown control signal' in body['error']

    def test_internal_error_on_handler_failure(self):
        shared = self._make_shared_registry()
        with patch(
            'singlestoredb.functions.ext.plugin.control'
            '.describe_functions_json',
            side_effect=RuntimeError('boom'),
        ):
            result = dispatch_control_signal('@@functions', b'', shared)
        assert result.ok is False
        body = json.loads(result.data)
        assert body['code'] == 'INTERNAL_ERROR'
        assert 'boom' in body['error']

    def test_register_missing_payload(self):
        shared = self._make_shared_registry()
        result = dispatch_control_signal('@@register', b'', shared)
        assert result.ok is False
        body = json.loads(result.data)
        assert body['code'] == 'REGISTER_MISSING_PAYLOAD'
        assert 'Missing registration payload' in body['error']

    def test_register_invalid_json(self):
        shared = self._make_shared_registry()
        result = dispatch_control_signal('@@register', b'not json', shared)
        assert result.ok is False
        body = json.loads(result.data)
        assert body['code'] == 'REGISTER_INVALID_PAYLOAD'
        assert 'Invalid JSON' in body['error']

    def test_register_missing_function_name(self):
        shared = self._make_shared_registry()
        payload = json.dumps({'args': [], 'returns': [], 'body': 'x'}).encode()
        result = dispatch_control_signal('@@register', payload, shared)
        assert result.ok is False
        body = json.loads(result.data)
        assert body['code'] == 'REGISTER_INVALID_PAYLOAD'
        assert 'name' in body['error']

    def test_register_missing_args(self):
        shared = self._make_shared_registry()
        payload = json.dumps({
            'name': 'f', 'returns': [], 'body': 'x',
        }).encode()
        result = dispatch_control_signal('@@register', payload, shared)
        assert result.ok is False
        body = json.loads(result.data)
        assert body['code'] == 'REGISTER_INVALID_PAYLOAD'
        assert 'args' in body['error']

    def test_register_missing_returns(self):
        shared = self._make_shared_registry()
        payload = json.dumps({
            'name': 'f', 'args': [], 'body': 'x',
        }).encode()
        result = dispatch_control_signal('@@register', payload, shared)
        assert result.ok is False
        body = json.loads(result.data)
        assert body['code'] == 'REGISTER_INVALID_PAYLOAD'
        assert 'returns' in body['error']

    def test_register_missing_body(self):
        shared = self._make_shared_registry()
        payload = json.dumps({
            'name': 'f', 'args': [], 'returns': [],
        }).encode()
        result = dispatch_control_signal('@@register', payload, shared)
        assert result.ok is False
        body = json.loads(result.data)
        assert body['code'] == 'REGISTER_INVALID_PAYLOAD'
        assert 'body' in body['error']

    def test_register_args_not_list(self):
        shared = self._make_shared_registry()
        payload = json.dumps({
            'name': 'f', 'args': 'notalist',
            'returns': [], 'body': 'x',
        }).encode()
        result = dispatch_control_signal('@@register', payload, shared)
        assert result.ok is False
        body = json.loads(result.data)
        assert body['code'] == 'REGISTER_INVALID_PAYLOAD'
        assert 'args' in body['error']

    def test_register_name_not_string(self):
        shared = self._make_shared_registry()
        payload = json.dumps({
            'name': 123, 'args': [], 'returns': [], 'body': 'return 1',
        }).encode()
        result = dispatch_control_signal('@@register', payload, shared)
        assert result.ok is False
        body = json.loads(result.data)
        assert body['code'] == 'REGISTER_INVALID_PAYLOAD'
        assert 'name' in body['error']

    def test_register_body_not_string(self):
        shared = self._make_shared_registry()
        payload = json.dumps({
            'name': 'f', 'args': [], 'returns': [], 'body': 12345,
        }).encode()
        result = dispatch_control_signal('@@register', payload, shared)
        assert result.ok is False
        body = json.loads(result.data)
        assert body['code'] == 'REGISTER_INVALID_PAYLOAD'
        assert 'body' in body['error']

    def test_register_arg_missing_dtype(self):
        """An arg dict missing 'dtype' is a client-shape error and must
        surface as REGISTER_INVALID_PAYLOAD, not INTERNAL_ERROR."""
        shared = self._make_shared_registry()
        shared.create_function.side_effect = ValueError(
            '"args[0].dtype" must be a string',
        )
        payload = json.dumps({
            'name': 'f', 'args': [{'name': 'x'}], 'returns': [],
            'body': 'return 1',
        }).encode()
        result = dispatch_control_signal('@@register', payload, shared)
        assert result.ok is False
        body = json.loads(result.data)
        assert body['code'] == 'REGISTER_INVALID_PAYLOAD'
        assert 'dtype' in body['error']

    def test_register_return_missing_dtype(self):
        """A returns dict missing 'dtype' is a client-shape error and must
        surface as REGISTER_INVALID_PAYLOAD, not INTERNAL_ERROR."""
        shared = self._make_shared_registry()
        shared.create_function.side_effect = ValueError(
            '"returns[0].dtype" must be a string',
        )
        payload = json.dumps({
            'name': 'f', 'args': [], 'returns': [{'name': ''}],
            'body': 'return 1',
        }).encode()
        result = dispatch_control_signal('@@register', payload, shared)
        assert result.ok is False
        body = json.loads(result.data)
        assert body['code'] == 'REGISTER_INVALID_PAYLOAD'
        assert 'dtype' in body['error']

    def test_register_func_exists(self):
        shared = self._make_shared_registry()
        shared.create_function.side_effect = FunctionExistsError(
            'Function "f" already exists (use replace=true to overwrite)',
        )
        payload = json.dumps({
            'name': 'f', 'args': [], 'returns': [], 'body': 'x',
        }).encode()
        result = dispatch_control_signal('@@register', payload, shared)
        assert result.ok is False
        body = json.loads(result.data)
        assert body['code'] == 'REGISTER_FUNC_EXISTS'
        assert 'already exists' in body['error']

    def test_register_replace_base_function(self):
        shared = self._make_shared_registry()
        shared.create_function.side_effect = FunctionNotDynamicError(
            "Cannot replace 'base_fn': not a dynamically registered function",
        )
        payload = json.dumps({
            'name': 'base_fn', 'args': [], 'returns': [],
            'body': 'x', 'replace': True,
        }).encode()
        result = dispatch_control_signal('@@register', payload, shared)
        assert result.ok is False
        body = json.loads(result.data)
        assert body['code'] == 'REGISTER_FUNC_NOT_DYNAMIC'
        assert 'not a dynamically registered function' in body['error']

    def test_register_unexpected_internal_error(self):
        shared = self._make_shared_registry()
        shared.create_function.side_effect = RuntimeError(
            'simulated infrastructure failure',
        )
        payload = json.dumps({
            'name': 'f', 'args': [], 'returns': [], 'body': 'x',
        }).encode()
        result = dispatch_control_signal('@@register', payload, shared)
        assert result.ok is False
        body = json.loads(result.data)
        assert body['code'] == 'INTERNAL_ERROR'
        assert 'simulated infrastructure failure' in body['error']

    def test_delete_missing_payload(self):
        shared = self._make_shared_registry()
        result = dispatch_control_signal('@@delete', b'', shared)
        assert result.ok is False
        body = json.loads(result.data)
        assert body['code'] == 'DELETE_MISSING_PAYLOAD'
        assert 'Missing deletion payload' in body['error']

    def test_delete_invalid_json(self):
        shared = self._make_shared_registry()
        result = dispatch_control_signal('@@delete', b'not json', shared)
        assert result.ok is False
        body = json.loads(result.data)
        assert body['code'] == 'DELETE_INVALID_PAYLOAD'
        assert 'Invalid JSON' in body['error']

    def test_delete_missing_function_name(self):
        shared = self._make_shared_registry()
        payload = json.dumps({}).encode()
        result = dispatch_control_signal('@@delete', payload, shared)
        assert result.ok is False
        body = json.loads(result.data)
        assert body['code'] == 'DELETE_INVALID_PAYLOAD'
        assert 'name' in body['error']

    def test_delete_name_not_string(self):
        """A non-string name (e.g. dict) must surface as
        DELETE_INVALID_PAYLOAD, not INTERNAL_ERROR."""
        shared = self._make_shared_registry()
        payload = json.dumps({'name': {'x': 1}}).encode()
        result = dispatch_control_signal('@@delete', payload, shared)
        assert result.ok is False
        body = json.loads(result.data)
        assert body['code'] == 'DELETE_INVALID_PAYLOAD'
        assert 'name' in body['error']

    def test_delete_nonexistent_function(self):
        shared = self._make_shared_registry()
        shared.delete_function.side_effect = FunctionNotFoundError(
            "Function 'no_such' not found",
        )
        payload = json.dumps({'name': 'no_such'}).encode()
        result = dispatch_control_signal('@@delete', payload, shared)
        assert result.ok is False
        body = json.loads(result.data)
        assert body['code'] == 'DELETE_FUNC_NOT_FOUND'
        assert 'not found' in body['error']

    def test_delete_base_function(self):
        shared = self._make_shared_registry()
        shared.delete_function.side_effect = FunctionNotDynamicError(
            "Cannot delete 'base_fn': not a dynamically registered function",
        )
        payload = json.dumps({'name': 'base_fn'}).encode()
        result = dispatch_control_signal('@@delete', payload, shared)
        assert result.ok is False
        body = json.loads(result.data)
        assert body['code'] == 'DELETE_FUNC_NOT_REGISTERED'
        assert 'not a dynamically registered function' in body['error']

    def test_delete_success(self):
        shared = self._make_shared_registry()
        shared.delete_function.return_value = None
        payload = json.dumps({'name': 'my_func'}).encode()
        result = dispatch_control_signal('@@delete', payload, shared)
        assert result.ok is True
        data = json.loads(result.data)
        assert data['status'] == 'ok'
        shared.delete_function.assert_called_once_with('my_func')


class TestFunctionRegistryDeleteGuard(unittest.TestCase):
    """Unit tests for FunctionRegistry.delete_function base-function guard."""

    def _make_registry_with_base(self):
        reg = FunctionRegistry()
        reg.functions = {'base_fn': {'signature': {}, 'func': lambda: None}}
        reg._base_function_names = {'base_fn'}
        return reg

    def test_delete_base_function_rejected(self):
        reg = self._make_registry_with_base()
        with self.assertRaises(ValueError) as ctx:
            reg.delete_function(json.dumps({'name': 'base_fn'}))
        assert 'not a dynamically registered function' in str(ctx.exception)

    def test_delete_dynamic_function_allowed(self):
        reg = self._make_registry_with_base()
        reg.functions['dyn_fn'] = {'signature': {}, 'func': lambda: None}
        reg.delete_function(json.dumps({'name': 'dyn_fn'}))
        assert 'dyn_fn' not in reg.functions

    def test_delete_nonexistent_raises(self):
        reg = self._make_registry_with_base()
        with self.assertRaises(ValueError) as ctx:
            reg.delete_function(json.dumps({'name': 'ghost'}))
        assert 'not found' in str(ctx.exception)

    def test_replace_base_function_rejected(self):
        reg = self._make_registry_with_base()
        sig = json.dumps({
            'name': 'base_fn',
            'args': [{'name': 'x', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'returns': [{'name': '', 'dtype': 'int64', 'sql': 'BIGINT'}],
        })
        with self.assertRaises(FunctionNotDynamicError) as ctx:
            reg.create_function(sig, 'return x + 1', replace=True)
        assert 'reserved by a built-in' in str(ctx.exception)

    def test_create_function_arg_missing_dtype_raises_value_error(self):
        reg = FunctionRegistry()
        sig = json.dumps({
            'name': 'f', 'args': [{'name': 'x'}], 'returns': [],
        })
        with self.assertRaises(ValueError) as ctx:
            reg.create_function(sig, 'return 1', replace=False)
        assert 'dtype' in str(ctx.exception)

    def test_create_function_return_missing_dtype_raises_value_error(self):
        reg = FunctionRegistry()
        sig = json.dumps({
            'name': 'f', 'args': [], 'returns': [{'name': ''}],
        })
        with self.assertRaises(ValueError) as ctx:
            reg.create_function(sig, 'return 1', replace=False)
        assert 'dtype' in str(ctx.exception)

    def test_create_function_arg_item_not_dict_raises_value_error(self):
        reg = FunctionRegistry()
        sig = json.dumps({
            'name': 'f', 'args': [1, 2], 'returns': [],
        })
        with self.assertRaises(ValueError) as ctx:
            reg.create_function(sig, 'return 1', replace=False)
        assert 'args[0]' in str(ctx.exception)

    def test_register_base_name_without_replace_returns_not_dynamic(self):
        """A @@register colliding with a base (non-dynamic) function name
        must raise FunctionNotDynamicError regardless of ``replace``, so
        the dispatcher emits REGISTER_FUNC_NOT_DYNAMIC. Suggesting
        ``replace=true`` would be misleading since base names cannot be
        overwritten by either path.
        """
        reg = self._make_registry_with_base()
        sig = json.dumps({
            'name': 'base_fn',
            'args': [{'name': 'x', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'returns': [{'name': '', 'dtype': 'int64', 'sql': 'BIGINT'}],
        })
        with self.assertRaises(FunctionNotDynamicError) as ctx:
            reg.create_function(sig, 'return x + 1', replace=False)
        assert 'reserved by a built-in' in str(ctx.exception)


class TestDeleteFunctionIntegration(unittest.TestCase):
    """Integration tests for @@delete using a real SharedRegistry."""

    def _make_real_shared_registry(self):
        from singlestoredb.functions.ext.plugin.server import SharedRegistry
        shared = SharedRegistry()
        base_reg = FunctionRegistry()
        base_reg.functions = {'base_fn': {'signature': {}, 'func': lambda: None}}
        base_reg._base_function_names = {'base_fn'}
        shared.set_base_registry(base_reg)
        return shared

    def test_register_then_delete(self):
        shared = self._make_real_shared_registry()
        sig = json.dumps({
            'name': 'dyn_fn',
            'args': [{'name': 'x', 'dtype': 'int', 'sql': 'INT'}],
            'returns': [{'name': '', 'dtype': 'int', 'sql': 'INT'}],
        })
        shared.create_function(sig, 'return x + 1', False)
        reg = shared.get_thread_local_registry()
        assert 'dyn_fn' in reg.functions

        shared.delete_function('dyn_fn')
        reg = shared.get_thread_local_registry()
        assert 'dyn_fn' not in reg.functions

    def test_delete_base_function_errors(self):
        shared = self._make_real_shared_registry()
        with self.assertRaises(FunctionNotDynamicError) as ctx:
            shared.delete_function('base_fn')
        assert 'not a dynamically registered function' in str(ctx.exception)

    def test_delete_nonexistent_errors(self):
        shared = self._make_real_shared_registry()
        with self.assertRaises(FunctionNotFoundError) as ctx:
            shared.delete_function('ghost')
        assert 'not found' in str(ctx.exception)

    def test_dispatch_delete_nonexistent_returns_func_not_found(self):
        """End-to-end @@delete on a real registry returns the typed code.

        Regression test: prior to the typed-exception fix in
        SharedRegistry.delete_function, this scenario fell through to
        the generic ValueError branch and returned DELETE_INVALID_PAYLOAD.
        """
        shared = self._make_real_shared_registry()
        payload = json.dumps({'name': 'ghost'}).encode()
        result = dispatch_control_signal('@@delete', payload, shared)
        assert result.ok is False
        body = json.loads(result.data)
        assert body['code'] == 'DELETE_FUNC_NOT_FOUND'
        assert 'not found' in body['error']

    def test_dispatch_delete_base_function_returns_not_registered(self):
        """End-to-end @@delete on a base function returns the typed code.

        Regression test: prior to the typed-exception fix in
        SharedRegistry.delete_function, this scenario fell through to
        the generic ValueError branch and returned DELETE_INVALID_PAYLOAD.
        """
        shared = self._make_real_shared_registry()
        payload = json.dumps({'name': 'base_fn'}).encode()
        result = dispatch_control_signal('@@delete', payload, shared)
        assert result.ok is False
        body = json.loads(result.data)
        assert body['code'] == 'DELETE_FUNC_NOT_REGISTERED'
        assert 'not a dynamically registered function' in body['error']

    def test_replace_base_via_shared_rejected(self):
        shared = self._make_real_shared_registry()
        sig = json.dumps({
            'name': 'base_fn',
            'args': [{'name': 'x', 'dtype': 'int', 'sql': 'INT'}],
            'returns': [{'name': '', 'dtype': 'int', 'sql': 'INT'}],
        })
        with self.assertRaises(FunctionNotDynamicError) as ctx:
            shared.create_function(sig, 'return x + 1', True)
        assert 'reserved by a built-in' in str(ctx.exception)

    def test_register_delete_reregister(self):
        shared = self._make_real_shared_registry()
        sig = json.dumps({
            'name': 'dyn_fn',
            'args': [{'name': 'x', 'dtype': 'int', 'sql': 'INT'}],
            'returns': [{'name': '', 'dtype': 'int', 'sql': 'INT'}],
        })
        shared.create_function(sig, 'return x + 1', False)
        shared.delete_function('dyn_fn')
        shared.create_function(sig, 'return x + 2', False)
        reg = shared.get_thread_local_registry()
        assert 'dyn_fn' in reg.functions

    def test_deleted_function_does_not_reappear(self):
        shared = self._make_real_shared_registry()
        sig_a = json.dumps({
            'name': 'fn_a',
            'args': [{'name': 'x', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'returns': [{'name': '', 'dtype': 'int64', 'sql': 'BIGINT'}],
        })
        shared.create_function(sig_a, 'return x + 1', False)
        shared.delete_function('fn_a')

        sig_b = json.dumps({
            'name': 'fn_b',
            'args': [{'name': 'x', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'returns': [{'name': '', 'dtype': 'int64', 'sql': 'BIGINT'}],
        })
        shared.create_function(sig_b, 'return x + 2', False)

        reg = shared.get_thread_local_registry()
        assert 'fn_a' not in reg.functions
        assert 'fn_b' in reg.functions


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


class TestHandshakeProtocol(unittest.TestCase):
    """Tests for the binary handshake protocol in _handle_connection_inner."""

    def _make_shared_registry(self):
        mock_reg = MagicMock()
        mock_reg.functions = {}
        mock_shared = MagicMock()
        mock_shared.get_thread_local_registry.return_value = mock_reg
        return mock_shared

    def test_eof_on_header(self):
        a, b = socket.socketpair()
        try:
            b.close()
            _handle_connection_inner(
                a, self._make_shared_registry(), threading.Event(),
            )
        finally:
            a.close()

    def test_bad_protocol_version(self):
        a, b = socket.socketpair()
        try:
            header = struct.pack('<QQ', 999, 5)
            b.sendall(header)
            b.close()
            _handle_connection_inner(
                a, self._make_shared_registry(), threading.Event(),
            )
        finally:
            a.close()

    def test_namelen_too_large(self):
        a, b = socket.socketpair()
        try:
            header = struct.pack('<QQ', PROTOCOL_VERSION, _MAX_FUNCTION_NAME_LEN + 1)
            b.sendall(header)
            b.close()
            _handle_connection_inner(
                a, self._make_shared_registry(), threading.Event(),
            )
        finally:
            a.close()

    def test_namelen_at_limit_accepted(self):
        a, b = socket.socketpair()
        try:
            header = struct.pack('<QQ', PROTOCOL_VERSION, _MAX_FUNCTION_NAME_LEN)
            b.sendall(header)
            b.close()
            # Will fail at recvmsg (no ancdata) but header was accepted
            try:
                _handle_connection_inner(
                    a, self._make_shared_registry(), threading.Event(),
                )
            except (OSError, ValueError):
                pass
        finally:
            a.close()

    def test_short_header(self):
        a, b = socket.socketpair()
        try:
            b.sendall(b'\x00' * 8)
            b.close()
            _handle_connection_inner(
                a, self._make_shared_registry(), threading.Event(),
            )
        finally:
            a.close()


if __name__ == '__main__':
    unittest.main()
