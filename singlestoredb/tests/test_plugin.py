#!/usr/bin/env python
# type: ignore
"""Tests for the plugin UDF server components.

Covers: _recv_exact_py, control signal dispatch, and lazy imports.
These are unit tests that do not require a database connection.
"""
import array
import json
import os
import socket
import struct
import tempfile
import threading
import unittest
from unittest.mock import ANY
from unittest.mock import MagicMock
from unittest.mock import patch

from singlestoredb.functions.ext import rowdat_1
from singlestoredb.functions.ext.plugin.connection import _handle_connection_inner
from singlestoredb.functions.ext.plugin.connection import _install_v3_definition
from singlestoredb.functions.ext.plugin.connection import _MAX_FUNCTION_NAME_LEN
from singlestoredb.functions.ext.plugin.connection import _MAX_V3_BODY_BYTES
from singlestoredb.functions.ext.plugin.connection import _recv_exact_py
from singlestoredb.functions.ext.plugin.connection import STATUS_ERROR
from singlestoredb.functions.ext.plugin.connection import STATUS_OK
from singlestoredb.functions.ext.plugin.connection import SUPPORTED_VERSIONS
from singlestoredb.functions.ext.plugin.control import dispatch_control_signal
from singlestoredb.functions.ext.plugin.registry import call_function
from singlestoredb.functions.ext.plugin.registry import FunctionRegistry
from singlestoredb.mysql.constants import FIELD_TYPE as ft
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
        assert result.data == 'Unknown control signal: @@unknown'
        assert result.code == 'UNKNOWN_SIGNAL'

    def test_register_signal_no_longer_supported(self):
        """@@register was replaced by in-band v3 envelope registration."""
        shared = self._make_shared_registry()
        result = dispatch_control_signal('@@register', b'', shared)
        assert result.ok is False
        assert 'Unknown control signal' in result.data
        assert result.code == 'UNKNOWN_SIGNAL'

    def test_delete_missing_payload(self):
        shared = self._make_shared_registry()
        result = dispatch_control_signal('@@delete', b'', shared)
        assert result.ok is False
        assert 'Missing deletion payload' in result.data
        assert result.code == 'DELETE_MISSING_PAYLOAD'

    def test_delete_invalid_json(self):
        shared = self._make_shared_registry()
        result = dispatch_control_signal('@@delete', b'not json', shared)
        assert result.ok is False
        assert 'Invalid JSON' in result.data
        assert result.code == 'DELETE_INVALID_PAYLOAD'

    def test_delete_non_utf8_payload(self):
        # json.loads raises UnicodeDecodeError, not JSONDecodeError, on
        # bytes that are not valid UTF-8.
        shared = self._make_shared_registry()
        result = dispatch_control_signal('@@delete', b'\xff', shared)
        assert result.ok is False
        assert 'Invalid JSON' in result.data
        assert result.code == 'DELETE_INVALID_PAYLOAD'
        shared.delete_function.assert_not_called()

    def test_delete_missing_id(self):
        shared = self._make_shared_registry()
        payload = json.dumps({}).encode()
        result = dispatch_control_signal('@@delete', payload, shared)
        assert result.ok is False
        assert result.data == 'Missing required string field: id'
        assert result.code == 'DELETE_INVALID_PAYLOAD'

    def test_delete_bad_id_payloads(self):
        # A non-string id, a name-keyed payload, and a non-object payload
        # all fail the same id check without reaching the registry.
        for body in ({'id': 5}, {'id': None}, {'name': 'x'}, [], 'x'):
            shared = self._make_shared_registry()
            payload = json.dumps(body).encode()
            result = dispatch_control_signal('@@delete', payload, shared)
            assert result.ok is False, body
            assert result.data == 'Missing required string field: id', body
            assert result.code == 'DELETE_INVALID_PAYLOAD', body
            shared.delete_function.assert_not_called()

    def test_delete_unknown_id(self):
        shared = self._make_shared_registry()
        shared.delete_function.side_effect = ValueError(
            "No registered function with id 'no_such'",
        )
        payload = json.dumps({'id': 'no_such'}).encode()
        result = dispatch_control_signal('@@delete', payload, shared)
        assert result.ok is False
        assert 'No registered function' in result.data
        assert result.code == 'DELETE_FUNC_NOT_REGISTERED'

    def test_delete_pipe_write_failure_is_internal_error(self):
        shared = self._make_shared_registry()
        # A closed read end makes the parent notification raise
        # BrokenPipeError after the delete itself succeeded.
        r_fd, w_fd = os.pipe()
        os.close(r_fd)
        try:
            payload = json.dumps({'id': 'sha-abc'}).encode()
            result = dispatch_control_signal(
                '@@delete', payload, shared, pipe_write_fd=w_fd,
            )
        finally:
            os.close(w_fd)
        assert result.ok is False
        assert result.code == 'INTERNAL_ERROR'

    def test_success_has_no_code(self):
        shared = self._make_shared_registry()
        assert dispatch_control_signal('@@health', b'', shared).code is None

    def test_delete_success(self):
        shared = self._make_shared_registry()
        shared.delete_function.return_value = None
        payload = json.dumps({'id': 'sha-abc'}).encode()
        result = dispatch_control_signal('@@delete', payload, shared)
        assert result.ok is True
        data = json.loads(result.data)
        assert data['status'] == 'ok'
        shared.delete_function.assert_called_once_with('sha-abc')


class TestFunctionRegistryDeleteGuard(unittest.TestCase):
    """Unit tests for FunctionRegistry.delete_function id-keyed behavior."""

    def _make_registry_with_base(self):
        reg = FunctionRegistry()
        reg.functions = {
            'base_fn': [{
                'signature': {}, 'func': lambda: None,
                'param_sql_types': [],
            }],
        }
        reg._base_function_names = {'base_fn'}
        return reg

    def test_delete_unknown_id_raises(self):
        reg = self._make_registry_with_base()
        with self.assertRaises(ValueError) as ctx:
            reg.delete_function('unknown-id')
        assert 'No registered function with id' in str(ctx.exception)

    def test_delete_dynamic_function_by_id(self):
        reg = self._make_registry_with_base()
        reg.functions['dyn_fn'] = [{
            'signature': {}, 'func': lambda: None,
            'param_sql_types': [],
        }]
        reg._id_to_variant['dyn-id'] = ('dyn_fn', '')
        reg.delete_function('dyn-id')
        assert 'dyn_fn' not in reg.functions
        assert 'dyn-id' not in reg._id_to_variant

    def test_delete_base_function_via_id_rejected(self):
        # Manually map an id at a base function name — registry must
        # still refuse to delete base functions.
        reg = self._make_registry_with_base()
        reg._id_to_variant['fake-id'] = ('base_fn', '')
        with self.assertRaises(ValueError) as ctx:
            reg.delete_function('fake-id')
        assert 'not a dynamically registered function' in str(ctx.exception)

    def test_replace_base_function_rejected(self):
        reg = self._make_registry_with_base()
        sig = json.dumps({
            'name': 'base_fn',
            'args': [{'name': 'x', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'returns': [{'name': '', 'dtype': 'int64', 'sql': 'BIGINT'}],
        })
        with self.assertRaises(ValueError) as ctx:
            reg.create_function('some-id', sig, 'return x + 1', replace=True)
        assert 'not a dynamically registered function' in str(ctx.exception)


class TestDeleteFunctionIntegration(unittest.TestCase):
    """Integration tests for @@delete using a real SharedRegistry."""

    def _make_real_shared_registry(self):
        from singlestoredb.functions.ext.plugin.server import SharedRegistry
        shared = SharedRegistry()
        base_reg = FunctionRegistry()
        base_reg.functions = {
            'base_fn': [{
                'signature': {}, 'func': lambda: None,
                'param_sql_types': [],
            }],
        }
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
        shared.create_function('id-1', sig, 'return x + 1', False)
        reg = shared.get_thread_local_registry()
        assert 'dyn_fn' in reg.functions

        shared.delete_function('id-1')
        reg = shared.get_thread_local_registry()
        assert 'dyn_fn' not in reg.functions

    def test_delete_unknown_id_errors(self):
        shared = self._make_real_shared_registry()
        with self.assertRaises(ValueError) as ctx:
            shared.delete_function('ghost-id')
        assert 'No registered function' in str(ctx.exception)

    def test_replace_base_via_shared_rejected(self):
        shared = self._make_real_shared_registry()
        sig = json.dumps({
            'name': 'base_fn',
            'args': [{'name': 'x', 'dtype': 'int', 'sql': 'INT'}],
            'returns': [{'name': '', 'dtype': 'int', 'sql': 'INT'}],
        })
        with self.assertRaises(ValueError) as ctx:
            shared.create_function('id-x', sig, 'return x + 1', True)
        assert 'not a dynamically registered function' in str(ctx.exception)

    def test_register_delete_reregister(self):
        shared = self._make_real_shared_registry()
        sig = json.dumps({
            'name': 'dyn_fn',
            'args': [{'name': 'x', 'dtype': 'int', 'sql': 'INT'}],
            'returns': [{'name': '', 'dtype': 'int', 'sql': 'INT'}],
        })
        shared.create_function('id-1', sig, 'return x + 1', False)
        shared.delete_function('id-1')
        shared.create_function('id-2', sig, 'return x + 2', False)
        reg = shared.get_thread_local_registry()
        assert 'dyn_fn' in reg.functions

    def test_deleted_function_does_not_reappear(self):
        shared = self._make_real_shared_registry()
        sig_a = json.dumps({
            'name': 'fn_a',
            'args': [{'name': 'x', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'returns': [{'name': '', 'dtype': 'int64', 'sql': 'BIGINT'}],
        })
        shared.create_function('id-a', sig_a, 'return x + 1', False)
        shared.delete_function('id-a')

        sig_b = json.dumps({
            'name': 'fn_b',
            'args': [{'name': 'x', 'dtype': 'int64', 'sql': 'BIGINT'}],
            'returns': [{'name': '', 'dtype': 'int64', 'sql': 'BIGINT'}],
        })
        shared.create_function('id-b', sig_b, 'return x + 2', False)

        reg = shared.get_thread_local_registry()
        assert 'fn_a' not in reg.functions
        assert 'fn_b' in reg.functions


def _ovl_sig(name, args, ret_dtype='int64', ret_sql='BIGINT'):
    """Build a describe-functions-style signature JSON string.

    ``args`` is a list of (dtype, sql) pairs; parameter names are generated.
    """
    return json.dumps({
        'name': name,
        'args': [
            {'name': f'a{i}', 'dtype': dtype, 'sql': sql}
            for i, (dtype, sql) in enumerate(args)
        ],
        'returns': [{'name': '', 'dtype': ret_dtype, 'sql': ret_sql}],
    })


class TestSharedRegistrySupersession(unittest.TestCase):
    """CREATE OR REPLACE ships a fresh id for an existing (name, param
    types) and never sends @@delete for the old one."""

    def _make_shared(self, base=None):
        from singlestoredb.functions.ext.plugin.server import SharedRegistry
        shared = SharedRegistry()
        shared.set_base_registry(base or FunctionRegistry())
        return shared

    @staticmethod
    def _call(shared, name, param_types, *values):
        reg = shared.get_thread_local_registry()
        input_data = bytes(
            rowdat_1._dump([ft.LONGLONG] * len(values), [1], [list(values)]),
        )
        output = call_function(reg, name, input_data, param_types)
        _, rows = rowdat_1._load([('r', ft.LONGLONG)], output)
        return rows[0][0]

    def _supersede(self, replace):
        shared = self._make_shared()
        sig = _ovl_sig('f', [('int64', 'BIGINT')])
        shared.create_function('id-A', sig, 'return 100 + a0', False)
        shared.create_function('id-B', sig, 'return 200 + a0', replace)
        return shared

    def test_fresh_id_same_signature_supersedes_without_replace(self):
        shared = self._supersede(replace=False)
        assert self._call(shared, 'f', 'BIGINT', 1) == 201
        assert not shared.has_definition('id-A')
        assert shared.has_definition('id-B')
        assert len(shared.get_thread_local_registry().functions['f']) == 1

    def test_fresh_id_same_signature_supersedes_with_replace(self):
        shared = self._supersede(replace=True)
        assert self._call(shared, 'f', 'BIGINT', 1) == 201
        assert not shared.has_definition('id-A')

    def test_delete_after_supersession_does_not_resurrect(self):
        shared = self._supersede(replace=False)
        shared.delete_function('id-B')
        reg = shared.get_thread_local_registry()
        assert 'f' not in reg.functions
        result = dispatch_control_signal('@@functions', b'', shared)
        assert json.loads(result.data)['functions'] == []

    def test_supersession_uses_normalized_types(self):
        # INT and BIGINT are different groups: both variants coexist.
        shared = self._make_shared()
        shared.create_function(
            'id-int', _ovl_sig('f', [('int64', 'INT')]), 'return 1', False,
        )
        shared.create_function(
            'id-big', _ovl_sig('f', [('int64', 'BIGINT')]), 'return 2', False,
        )
        assert shared.has_definition('id-int')
        assert shared.has_definition('id-big')

        # CHAR(10) and CHAR(20) share CHAR_GROUP: the second supersedes.
        shared.create_function(
            'id-c10', _ovl_sig('g', [('str', 'CHAR(10)')]), 'return 1', False,
        )
        shared.create_function(
            'id-c20', _ovl_sig('g', [('str', 'CHAR(20)')]), 'return 2', False,
        )
        assert not shared.has_definition('id-c10')
        assert shared.has_definition('id-c20')
        assert len(shared.get_thread_local_registry().functions['g']) == 1

    def test_sibling_overload_survives_supersession(self):
        shared = self._make_shared()
        shared.create_function(
            'id-1', _ovl_sig('f', [('int64', 'BIGINT')]),
            'return 100 + a0', False,
        )
        shared.create_function(
            'id-2', _ovl_sig('f', [('int64', 'BIGINT')] * 2),
            'return 200 + a0 + a1', False,
        )
        shared.create_function(
            'id-3', _ovl_sig('f', [('int64', 'BIGINT')]),
            'return 300 + a0', False,
        )
        assert not shared.has_definition('id-1')
        assert shared.has_definition('id-2')
        assert self._call(shared, 'f', 'BIGINT', 1) == 301
        assert self._call(shared, 'f', 'BIGINT;BIGINT', 1, 2) == 203

    def test_failed_supersession_keeps_old_definition(self):
        shared = self._make_shared()
        sig = _ovl_sig('f', [('int64', 'BIGINT')])
        shared.create_function('id-A', sig, 'return 100 + a0', False)
        generation = shared.generation
        with self.assertRaises(SyntaxError):
            shared.create_function('id-B', sig, 'return (', False)
        assert shared.has_definition('id-A')
        assert not shared.has_definition('id-B')
        assert shared.generation == generation
        assert self._call(shared, 'f', 'BIGINT', 1) == 101

    def test_resending_superseded_id_supersedes_its_successor(self):
        # Mirrors the Rust server: an evicted id is just a fresh install.
        shared = self._supersede(replace=False)
        sig = _ovl_sig('f', [('int64', 'BIGINT')])
        shared.create_function('id-A', sig, 'return 100 + a0', False)
        assert shared.has_definition('id-A')
        assert not shared.has_definition('id-B')
        assert self._call(shared, 'f', 'BIGINT', 1) == 101

    def test_builtin_collision_still_rejected(self):
        base = FunctionRegistry()
        base.create_function(
            'base-0', _ovl_sig('f', [('int64', 'BIGINT')]),
            'return 100 + a0', False,
        )
        base._base_function_names = {'f'}
        shared = self._make_shared(base)
        with self.assertRaises(ValueError) as ctx:
            shared.create_function(
                'id-B', _ovl_sig('f', [('int64', 'BIGINT')]),
                'return 200 + a0', False,
            )
        assert 'not a dynamically registered function' in str(ctx.exception)
        assert not shared.has_definition('id-B')


class TestSharedRegistryIdempotentInstall(unittest.TestCase):
    """An id is a content key: installing a cached id again is a no-op."""

    def _make_shared(self):
        from singlestoredb.functions.ext.plugin.server import SharedRegistry
        shared = SharedRegistry()
        shared.set_base_registry(FunctionRegistry())
        return shared

    def test_same_id_twice_is_a_noop(self):
        shared = self._make_shared()
        sig = _ovl_sig('f', [('int64', 'BIGINT')])
        assert shared.create_function('id-A', sig, 'return a0', False)
        generation = shared.generation
        assert shared.create_function('id-A', sig, 'return a0', False) is None
        assert shared.generation == generation
        assert len(shared.get_thread_local_registry().functions['f']) == 1

    def test_both_handshakes_miss_cache(self):
        # Two handshakes for one definition can both pass has_definition
        # before either installs. Neither may fail, and main gets told once.
        shared = self._make_shared()
        envelope = _ovl_envelope('id-A', 'f', [('int64', 'BIGINT')], 'BIGINT')
        from singlestoredb.functions.ext.plugin.server import _read_pipe_message
        r, w = os.pipe()
        try:
            with patch.object(shared, 'has_definition', return_value=False):
                for _ in range(2):
                    with tempfile.TemporaryFile() as f:
                        f.write(b'return a0')
                        f.flush()
                        _install_v3_definition(envelope, f.fileno(), shared, w)
            os.close(w)
            w = -1
            msg = _read_pipe_message(r)
            assert msg is not None
            assert json.loads(msg[0])['action'] == 'register'
            assert msg[1] == b'return a0'
            assert _read_pipe_message(r) is None
        finally:
            os.close(r)
            if w >= 0:
                os.close(w)
        assert len(shared.get_thread_local_registry().functions['f']) == 1


class TestOverloadRegistry(unittest.TestCase):
    """Registry-level behavior when a name carries multiple variants."""

    def _registry_with_three_variants(self):
        """dyn_ovl with 0-arg, 1-arg BIGINT, and 2-arg (BIGINT, BIGINT)."""
        reg = FunctionRegistry()
        reg.create_function(
            'id-0', _ovl_sig('dyn_ovl', []), 'return 0', False,
        )
        reg.create_function(
            'id-1', _ovl_sig('dyn_ovl', [('int64', 'BIGINT')]),
            'return a0 + 1', False,
        )
        reg.create_function(
            'id-2',
            _ovl_sig(
                'dyn_ovl',
                [('int64', 'BIGINT'), ('int64', 'BIGINT')],
            ),
            'return a0 + a1', False,
        )
        return reg

    def test_declared_sql_types_survive_the_recompile(self):
        # The body is recompiled from a Python annotation, and that trip is
        # lossy: a declared INT becomes `int` becomes BIGINT. Dispatch must
        # key off the *declared* types, or INT collides with BIGINT.
        reg = FunctionRegistry()
        reg.create_function(
            'id-int', _ovl_sig('dyn_ints', [('int32', 'INT')]),
            "return 'int'", False,
        )
        reg.create_function(
            'id-bigint', _ovl_sig('dyn_ints', [('int64', 'BIGINT')]),
            "return 'bigint'", False,
        )

        keys = [
            v['param_sql_types'] for v in reg.functions['dyn_ints']
        ]
        # describe_functions is what a host reflects functions from, so it
        # has to report the declared types too, not the re-derived ones.
        from singlestoredb.functions.ext.plugin.registry import (
            describe_functions_json,
        )
        entries = [
            e for e in json.loads(describe_functions_json(reg))
            if e['name'] == 'dyn_ints'
        ]
        assert [e['args'][0]['sql'] for e in entries] == ['INT', 'BIGINT']
        assert keys == [['INT_GROUP'], ['BIGINT']]
        assert set(reg._id_to_variant.values()) == {
            ('dyn_ints', 'INT_GROUP'), ('dyn_ints', 'BIGINT'),
        }
        assert reg.lookup_variant('dyn_ints', 'INT')['func'](1) == 'int'
        assert reg.lookup_variant('dyn_ints', 'BIGINT')['func'](1) == 'bigint'

    def test_describe_emits_one_entry_per_variant_in_order(self):
        from singlestoredb.functions.ext.plugin.registry import (
            describe_functions_json,
        )
        reg = self._registry_with_three_variants()
        entries = json.loads(describe_functions_json(reg))
        assert [e['name'] for e in entries] == ['dyn_ovl'] * 3
        # Registration order is preserved: 0-arg, 1-arg, 2-arg.
        assert [len(e['args']) for e in entries] == [0, 1, 2]

    def test_describe_includes_sibling_names(self):
        from singlestoredb.functions.ext.plugin.registry import (
            describe_functions_json,
        )
        reg = self._registry_with_three_variants()
        reg.create_function(
            'id-other', _ovl_sig('dyn_other', [('int64', 'BIGINT')]),
            'return a0', False,
        )
        entries = json.loads(describe_functions_json(reg))
        assert len(entries) == 4
        assert sum(1 for e in entries if e['name'] == 'dyn_other') == 1

    def test_delete_last_variant_removes_name(self):
        reg = FunctionRegistry()
        reg.create_function(
            'id-1', _ovl_sig('dyn_solo', [('int64', 'BIGINT')]),
            'return a0', False,
        )
        assert 'dyn_solo' in reg.functions
        reg.delete_function('id-1')
        # The name is gone entirely, not left as an empty list.
        assert 'dyn_solo' not in reg.functions
        assert 'id-1' not in reg._id_to_variant

    def test_delete_middle_variant_leaves_siblings_resolvable(self):
        reg = self._registry_with_three_variants()
        reg.delete_function('id-1')

        assert len(reg.functions['dyn_ovl']) == 2
        assert 'id-1' not in reg._id_to_variant
        assert set(reg._id_to_variant) == {'id-0', 'id-2'}

        # Survivors still dispatch, and the deleted arity now fails.
        assert reg.lookup_variant('dyn_ovl', '')['param_sql_types'] == []
        assert reg.lookup_variant(
            'dyn_ovl', 'BIGINT;BIGINT',
        )['param_sql_types'] == ['BIGINT', 'BIGINT']
        with self.assertRaises(ValueError) as ctx:
            reg.lookup_variant('dyn_ovl', 'BIGINT')
        assert 'matches 1 arguments' in str(ctx.exception)

        # And they are still callable.
        zero = reg.lookup_variant('dyn_ovl', '')
        two = reg.lookup_variant('dyn_ovl', 'BIGINT;BIGINT')
        assert zero['func']() == 0
        assert two['func'](3, 4) == 7

    def test_delete_each_variant_in_turn(self):
        reg = self._registry_with_three_variants()
        for id_ in ('id-2', 'id-0', 'id-1'):
            reg.delete_function(id_)
        assert 'dyn_ovl' not in reg.functions
        assert reg._id_to_variant == {}

    def test_replace_matching_signature_retires_prior_id(self):
        # replace=True swaps the variant with the same signature key even
        # though it was registered under a different id: the new id takes
        # over and the old mapping is dropped.
        reg = FunctionRegistry()
        sig = _ovl_sig('dyn_rep', [('int64', 'BIGINT')])
        reg.create_function('id-old', sig, 'return a0 + 1', False)
        reg.create_function('id-new', sig, 'return a0 + 100', True)

        assert 'id-old' not in reg._id_to_variant
        assert 'id-new' in reg._id_to_variant
        assert len(reg.functions['dyn_rep']) == 1
        # The surviving variant runs the *new* body.
        assert reg.functions['dyn_rep'][0]['func'](1) == 101

        # The retired id is no longer deletable.
        with self.assertRaises(ValueError):
            reg.delete_function('id-old')

    def test_replace_leaves_other_variants_untouched(self):
        reg = self._registry_with_three_variants()
        reg.create_function(
            'id-1b', _ovl_sig('dyn_ovl', [('int64', 'BIGINT')]),
            'return a0 + 1000', True,
        )
        assert len(reg.functions['dyn_ovl']) == 3
        assert set(reg._id_to_variant) == {'id-0', 'id-1b', 'id-2'}
        assert reg.lookup_variant('dyn_ovl', 'BIGINT')['func'](1) == 1001
        assert reg.lookup_variant('dyn_ovl', '')['func']() == 0

    def test_duplicate_signature_without_replace_leaves_state_intact(self):
        reg = self._registry_with_three_variants()
        with self.assertRaises(ValueError) as ctx:
            reg.create_function(
                'id-dup', _ovl_sig('dyn_ovl', [('int64', 'BIGINT')]),
                'return a0 + 7', False,
            )
        assert 'already exists' in str(ctx.exception)
        assert len(reg.functions['dyn_ovl']) == 3
        assert 'id-dup' not in reg._id_to_variant
        assert reg.lookup_variant('dyn_ovl', 'BIGINT')['func'](1) == 2

    def test_unsupported_dtype_in_second_variant_leaves_first_callable(self):
        reg = FunctionRegistry()
        reg.create_function(
            'id-good', _ovl_sig('dyn_mixed', [('int64', 'BIGINT')]),
            'return a0 + 1', False,
        )
        with self.assertRaises(ValueError):
            reg.create_function(
                'id-bad', _ovl_sig('dyn_mixed', [('geography', 'GEOGRAPHY')]),
                'return 0', False,
            )
        # First variant intact and callable; failed id left no mapping.
        assert len(reg.functions['dyn_mixed']) == 1
        assert 'id-bad' not in reg._id_to_variant
        assert set(reg._id_to_variant) == {'id-good'}
        assert reg.lookup_variant('dyn_mixed', 'BIGINT')['func'](1) == 2

    def test_overload_of_base_function_rejected(self):
        # A different parameter signature does not buy permission to
        # overload a built-in @udf name.
        reg = FunctionRegistry()
        reg.functions = {
            'base_fn': [{
                'signature': {}, 'func': lambda: None,
                'param_sql_types': [],
            }],
        }
        reg._base_function_names = {'base_fn'}
        for args in (
            [],
            [('int64', 'BIGINT')],
            [('str', 'VARCHAR(10)')],
            [('int64', 'BIGINT'), ('int64', 'BIGINT')],
        ):
            with self.assertRaises(ValueError) as ctx:
                reg.create_function(
                    'id-x', _ovl_sig('base_fn', args), 'return 0', False,
                )
            assert 'not a dynamically registered function' in str(
                ctx.exception,
            )
        assert len(reg.functions['base_fn']) == 1
        assert reg._id_to_variant == {}

    def test_delete_unknown_id_does_not_mutate_state(self):
        reg = self._registry_with_three_variants()
        before_ids = dict(reg._id_to_variant)
        before_variants = list(reg.functions['dyn_ovl'])
        with self.assertRaises(ValueError) as ctx:
            reg.delete_function('ghost-id')
        assert 'No registered function with id' in str(ctx.exception)
        assert reg._id_to_variant == before_ids
        assert reg.functions['dyn_ovl'] == before_variants

    def test_zero_arg_and_one_arg_variants_coexist(self):
        reg = FunctionRegistry()
        reg.create_function(
            'id-zero', _ovl_sig('dyn_z', []), 'return 11', False,
        )
        reg.create_function(
            'id-one', _ovl_sig('dyn_z', [('int64', 'BIGINT')]),
            'return 22', False,
        )
        assert len(reg.functions['dyn_z']) == 2
        # '' on the wire is a zero-arg call, not "no type info".
        assert reg.lookup_variant('dyn_z', '')['func']() == 11
        assert reg.lookup_variant('dyn_z', 'BIGINT')['func'](1) == 22
        # No type list at all (v1) is ambiguous with two variants.
        with self.assertRaises(ValueError) as ctx:
            reg.lookup_variant('dyn_z', None)
        assert 'no parameter type list was supplied' in str(ctx.exception)

    def test_zero_arg_variant_has_distinct_id_key(self):
        reg = self._registry_with_three_variants()
        assert reg._id_to_variant['id-0'] == ('dyn_ovl', '')
        assert reg._id_to_variant['id-1'] == ('dyn_ovl', 'BIGINT')
        assert reg._id_to_variant['id-2'] == ('dyn_ovl', 'BIGINT,BIGINT')


class TestRecursiveOverloads(unittest.TestCase):
    """A variant's own name inside its body must resolve to that variant,
    not to whichever overload happened to be compiled last."""

    _FACT = 'return 1 if a0 <= 1 else a0 * f(a0 - 1)'
    _STRLEN = "return 0 if a0 == '' else 1 + f(a0[1:])"

    def _install(self, target):
        target.create_function(
            'id-fact', _ovl_sig('f', [('int32', 'INT')]), self._FACT, False,
        )

    def _install_other_arity(self, target):
        target.create_function(
            'id-two', _ovl_sig('f', [('int32', 'INT'), ('int32', 'INT')]),
            'return a0 + a1', False,
        )

    def _install_other_type(self, target):
        target.create_function(
            'id-len', _ovl_sig('f', [('str', 'TEXT')]), self._STRLEN, False,
        )

    def _shared(self):
        from singlestoredb.functions.ext.plugin.server import SharedRegistry
        shared = SharedRegistry()
        shared.set_base_registry(FunctionRegistry())
        return shared

    def test_recursion_survives_different_arity_overload(self):
        reg = FunctionRegistry()
        self._install(reg)
        self._install_other_arity(reg)
        assert reg.lookup_variant('f', 'INT')['func'](5) == 120
        assert reg.lookup_variant('f', 'INT;INT')['func'](2, 3) == 5

    def test_same_arity_overloads_each_recurse_into_themselves(self):
        reg = FunctionRegistry()
        self._install(reg)
        self._install_other_type(reg)
        assert reg.lookup_variant('f', 'INT')['func'](5) == 120
        assert reg.lookup_variant('f', 'TEXT')['func']('abcd') == 4

    def test_recursion_survives_shared_registry_replay(self):
        shared = self._shared()
        self._install(shared)
        self._install_other_arity(shared)
        self._install_other_type(shared)
        reg = shared.get_thread_local_registry()
        assert reg.lookup_variant('f', 'INT')['func'](5) == 120
        assert reg.lookup_variant('f', 'INT;INT')['func'](2, 3) == 5
        assert reg.lookup_variant('f', 'TEXT')['func']('abcd') == 4


class TestShadowingNames(unittest.TestCase):
    """A dynamic function may share a name with the decorator, an
    annotation type, or a module the generated code would otherwise use."""

    _FACT = 'return 1 if a0 <= 1 else a0 * {name}(a0 - 1)'

    def _shared(self):
        from singlestoredb.functions.ext.plugin.server import SharedRegistry
        shared = SharedRegistry()
        shared.set_base_registry(FunctionRegistry())
        return shared

    def _install_fact(self, target, name):
        target.create_function(
            f'id-{name}', _ovl_sig(name, [('int64', 'BIGINT')]),
            self._FACT.format(name=name), False,
        )

    def test_function_named_udf(self):
        reg = FunctionRegistry()
        reg.create_function(
            'id-udf', _ovl_sig('udf', [('int64', 'BIGINT')]),
            'return a0 + 1', False,
        )
        assert reg.lookup_variant('udf', 'BIGINT')['func'](4) == 5

    def test_function_named_udf_recurses(self):
        reg = FunctionRegistry()
        self._install_fact(reg, 'udf')
        assert reg.lookup_variant('udf', 'BIGINT')['func'](5) == 120

    def test_function_named_int_recurses(self):
        reg = FunctionRegistry()
        self._install_fact(reg, 'int')
        assert reg.lookup_variant('int', 'BIGINT')['func'](5) == 120

    def test_function_named_datetime_is_not_overwritten(self):
        reg = FunctionRegistry()
        reg.create_function(
            'id-dt', _ovl_sig('datetime', [('datetime', 'DATETIME')]),
            'return a0.year', False,
        )
        # Registering a later function must not rebind `datetime` to the
        # module in the shared globals.
        reg.create_function(
            'id-g', _ovl_sig('g', [('datetime', 'DATETIME')]),
            'return datetime(a0) + 1', False,
        )
        when = __import__('datetime').datetime(2020, 1, 1)
        assert reg.lookup_variant('datetime', 'DATETIME')['func'](when) == 2020
        assert reg.lookup_variant('g', 'DATETIME')['func'](when) == 2021

    def test_deleting_shadowing_function_restores_prelude(self):
        reg = FunctionRegistry()
        reg.create_function(
            'id-dt', _ovl_sig('datetime', [('int64', 'BIGINT')]),
            'return a0', False,
        )
        reg.create_function(
            'id-g', _ovl_sig('g', [('int64', 'BIGINT')]),
            'return datetime.date(2020, 1, a0).day', False,
        )
        reg.delete_function('id-dt')
        assert reg.lookup_variant('g', 'BIGINT')['func'](3) == 3

    def test_shadowing_survives_shared_registry_replay(self):
        shared = self._shared()
        self._install_fact(shared, 'udf')
        self._install_fact(shared, 'int')
        reg = shared.get_thread_local_registry()
        assert reg.lookup_variant('udf', 'BIGINT')['func'](5) == 120
        assert reg.lookup_variant('int', 'BIGINT')['func'](5) == 120


class TestCrossFunctionCalls(unittest.TestCase):
    """A dynamic function's body can call another dynamic function by name,
    resolved when called, while its own name stays bound to itself."""

    def _install_g(self, target, id='id-g'):
        target.create_function(
            id, _ovl_sig('g', [('int32', 'INT')]), 'return a0 * 10', False,
        )

    def _install_f(self, target):
        target.create_function(
            'id-f', _ovl_sig('f', [('int32', 'INT')]), 'return g(a0) + 1',
            False,
        )

    def _shared(self):
        from singlestoredb.functions.ext.plugin.server import SharedRegistry
        shared = SharedRegistry()
        shared.set_base_registry(FunctionRegistry())
        return shared

    def test_callee_installed_first(self):
        reg = FunctionRegistry()
        self._install_g(reg)
        self._install_f(reg)
        assert reg.lookup_variant('f', 'INT')['func'](4) == 41

    def test_callee_installed_after_caller(self):
        reg = FunctionRegistry()
        self._install_f(reg)
        self._install_g(reg)
        assert reg.lookup_variant('f', 'INT')['func'](4) == 41

    def test_cross_call_survives_shared_registry_replay(self):
        for order in (
            (self._install_g, self._install_f),
            (self._install_f, self._install_g),
        ):
            shared = self._shared()
            for install in order:
                install(shared)
            reg = shared.get_thread_local_registry()
            assert reg.lookup_variant('f', 'INT')['func'](4) == 41

    def test_recursion_stays_private_alongside_cross_call(self):
        reg = FunctionRegistry()
        self._install_g(reg)
        reg.create_function(
            'id-f1', _ovl_sig('f', [('int32', 'INT')]),
            'return g(a0) if a0 <= 1 else f(a0 - 1)', False,
        )
        # A later overload of f must not capture the first variant's
        # recursive call.
        reg.create_function(
            'id-f2', _ovl_sig('f', [('int32', 'INT'), ('int32', 'INT')]),
            'return -1', False,
        )
        assert reg.lookup_variant('f', 'INT')['func'](3) == 10

    def test_deleted_callee_is_unbound(self):
        reg = FunctionRegistry()
        self._install_g(reg)
        self._install_f(reg)
        reg.delete_function('id-g')
        with self.assertRaises(NameError):
            reg.lookup_variant('f', 'INT')['func'](4)

    def test_deleting_one_overload_rebinds_to_the_rest(self):
        reg = FunctionRegistry()
        self._install_g(reg)
        reg.create_function(
            'id-g2', _ovl_sig('g', [('str', 'TEXT')]), 'return len(a0)',
            False,
        )
        self._install_f(reg)
        # The most recently registered variant (TEXT) is the bound one;
        # deleting it falls back to the INT variant.
        reg.delete_function('id-g2')
        assert reg.lookup_variant('f', 'INT')['func'](4) == 41

    def test_replace_rebinds_callee(self):
        reg = FunctionRegistry()
        self._install_g(reg)
        self._install_f(reg)
        reg.create_function(
            'id-g-new', _ovl_sig('g', [('int32', 'INT')]), 'return a0 * 100',
            True,
        )
        assert reg.lookup_variant('f', 'INT')['func'](4) == 401

    def test_registries_do_not_share_bindings(self):
        with_g = FunctionRegistry()
        self._install_g(with_g)
        self._install_f(with_g)
        without_g = FunctionRegistry()
        self._install_f(without_g)
        assert with_g.lookup_variant('f', 'INT')['func'](4) == 41
        with self.assertRaises(NameError):
            without_g.lookup_variant('f', 'INT')['func'](4)

    def test_function_named_like_a_factory_survives_install(self):
        # f's factory is __s2_make_f; installing f must not unbind a
        # dynamic function that already has that name.
        reg = FunctionRegistry()
        reg.create_function(
            'id-m', _ovl_sig('__s2_make_f', [('int32', 'INT')]),
            'return a0 * 10', False,
        )
        reg.create_function(
            'id-f', _ovl_sig('f', [('int32', 'INT')]), 'return a0 + 1',
            False,
        )
        reg.create_function(
            'id-h', _ovl_sig('h', [('int32', 'INT')]),
            'return __s2_make_f(a0)', False,
        )
        assert reg.lookup_variant('h', 'INT')['func'](4) == 40
        assert reg.lookup_variant('f', 'INT')['func'](4) == 5


class TestDynamicModule(unittest.TestCase):
    """Each registry's dynamic functions live in a real module, so code that
    resolves ``cls.__module__`` through ``sys.modules`` works."""

    def test_dataclass_with_string_annotation(self):
        reg = FunctionRegistry()
        reg.create_function(
            'id-p', _ovl_sig('p', [('int32', 'INT')]),
            'import dataclasses\n'
            '@dataclasses.dataclass\n'
            'class P:\n'
            "    v: 'int'\n"
            'return P(a0).v + 1',
            False,
        )
        assert reg.lookup_variant('p', 'INT')['func'](4) == 5

    def test_module_is_removed_with_its_registry(self):
        import gc
        import sys
        reg = FunctionRegistry()
        name = reg._dynamic_globals['__name__']
        assert sys.modules[name].__dict__ is reg._dynamic_globals
        del reg
        gc.collect()
        assert name not in sys.modules

    def test_registries_get_distinct_modules(self):
        one = FunctionRegistry()
        two = FunctionRegistry()
        assert one._dynamic_globals['__name__'] != \
            two._dynamic_globals['__name__']
        assert one._dynamic_globals is not two._dynamic_globals


class TestWasmAdapterParamTypes(unittest.TestCase):
    """The WIT adapter must keep ``option<string>`` faithful.

    ``param-types`` is an ``option<string>`` in wit/plugin.wit: ``some("")``
    is a zero-arg call and ``none`` is "no type list at all" (v1). The
    adapter forwards whichever it is received without collapsing the two.
    """

    def _forwarded(self, param_types):
        from singlestoredb.functions.ext.plugin import wasm
        with patch.object(wasm, 'call_function') as mock:
            mock.return_value = b'out'
            assert wasm.Plugin().call_function(
                'f', param_types, b'in',
            ) == b'out'
        return mock.call_args

    def test_none_forwarded_as_none(self):
        args, _ = self._forwarded(None)
        assert args == (ANY, 'f', b'in', None)

    def test_empty_string_forwarded_as_empty_string(self):
        # Not folded into None: '' means the call has zero arguments.
        args, _ = self._forwarded('')
        assert args == (ANY, 'f', b'in', '')

    def test_type_list_forwarded_verbatim(self):
        # Semicolon separator, and a comma inside DECIMAL survives.
        args, _ = self._forwarded('DECIMAL(10,2);BIGINT')
        assert args == (ANY, 'f', b'in', 'DECIMAL(10,2);BIGINT')


def _temp_fd():
    """Create an anonymous read/write fd standing in for an engine mmap."""
    fd, path = tempfile.mkstemp()
    os.unlink(path)
    return fd


def _ovl_envelope(id, name, args, param_types, replace=False):
    """Build a v3 handshake envelope carrying a function ``signature``.

    The body is not part of it; it travels on the body fd, which
    ``_ServerConn.handshake`` takes separately.
    """
    sig = json.loads(_ovl_sig(name, args))
    return {
        'id': id,
        'param_types': param_types,
        'batch_delimited': False,
        'signature': {
            'name': name,
            'args': sig['args'],
            'returns': sig['returns'],
            'replace': replace,
        },
    }


class _ServerConn:
    """Drive one client connection against ``_handle_connection_inner``.

    The server half runs in a thread, exactly as it does under the real
    thread-pool server. FDs are handed over with SCM_RIGHTS, so the server
    gets its own copies and closes them itself.
    """

    def __init__(self, shared_registry):
        self._shared = shared_registry
        self.shutdown_event = threading.Event()
        self.sock, self._server_sock = socket.socketpair()
        # The server abandons a connection without replying on some error
        # paths (a rejected v3 definition, a bad envelope). Time out rather
        # than hang the suite waiting for a response that never comes.
        self.sock.settimeout(10)
        self.input_fd = _temp_fd()
        self.output_fd = _temp_fd()
        self._thread = threading.Thread(
            target=_handle_connection_inner,
            args=(self._server_sock, self._shared, self.shutdown_event),
        )

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc_info):
        # A zero-length frame is the client's "no more requests" signal.
        try:
            self.sock.sendall(struct.pack('<Q', 0))
        except OSError:
            pass
        self._thread.join(timeout=5)
        self.shutdown_event.set()
        self.sock.close()
        self._server_sock.close()
        os.close(self.input_fd)
        os.close(self.output_fd)
        assert not self._thread.is_alive(), 'server thread did not exit'

    def handshake(self, function_name, envelope, body=b'', version=3):
        """Send a v3 handshake: header, name, and 4 FDs.

        The third FD carries the JSON envelope and the fourth the raw
        function body (empty for signature-only); the engine seals both,
        so each one's size alone delimits its payload. Offsets are left at
        EOF, as an engine that just wrote them would.
        """
        name_bytes = function_name.encode('utf8')
        self.sock.sendall(struct.pack('<QQ', version, len(name_bytes)))
        meta_fd = _temp_fd()
        body_fd = _temp_fd()
        try:
            os.write(meta_fd, json.dumps(envelope).encode('utf8'))
            os.write(body_fd, body)
            fds = array.array(
                'i', [self.input_fd, self.output_fd, meta_fd, body_fd],
            )
            self.sock.sendmsg(
                [name_bytes],
                [(socket.SOL_SOCKET, socket.SCM_RIGHTS, fds)],
            )
        finally:
            os.close(meta_fd)
            os.close(body_fd)

    def handshake_v1(self, function_name):
        """Send a v1 handshake: no envelope, no param type segment."""
        name_bytes = function_name.encode('utf8')
        self.sock.sendall(struct.pack('<QQ', 1, len(name_bytes)))
        fds = array.array('i', [self.input_fd, self.output_fd])
        self.sock.sendmsg(
            [name_bytes],
            [(socket.SOL_SOCKET, socket.SCM_RIGHTS, fds)],
        )

    def call(self, input_data):
        """Send one request frame and read the response."""
        os.ftruncate(self.input_fd, max(128 * 1024, len(input_data)))
        os.lseek(self.input_fd, 0, os.SEEK_SET)
        os.write(self.input_fd, input_data)
        self.sock.sendall(struct.pack('<Q', len(input_data)))

        resp = b''
        while len(resp) < 16:
            chunk = self.sock.recv(16 - len(resp))
            if not chunk:
                raise RuntimeError('EOF during response')
            resp += chunk
        status, size = struct.unpack('<QQ', resp)
        if status == STATUS_OK:
            os.lseek(self.output_fd, 0, os.SEEK_SET)
            return status, os.read(self.output_fd, size)
        # Errors carry their message inline after the header.
        err = b''
        while len(err) < size:
            chunk = self.sock.recv(size - len(err))
            if not chunk:
                break
            err += chunk
        return status, err


class TestOverloadServerProtocol(unittest.TestCase):
    """Overload behavior driven through the collocated socket protocol."""

    def setUp(self):
        # Force the pure-Python mmap path: these FDs are temp files, which
        # matches the pattern used in test_plugin_integration.py.
        import singlestoredb.functions.ext.plugin.registry as _reg_mod
        import singlestoredb.functions.ext.plugin.connection as _conn_mod
        self._reg_mod = _reg_mod
        self._conn_mod = _conn_mod
        self._orig_has_accel = _reg_mod._has_accel
        _reg_mod._has_accel = False
        _conn_mod._has_accel = False

    def tearDown(self):
        self._reg_mod._has_accel = self._orig_has_accel
        self._conn_mod._has_accel = self._orig_has_accel

    def _make_shared(self):
        from singlestoredb.functions.ext.plugin.server import SharedRegistry
        shared = SharedRegistry()
        shared.set_base_registry(FunctionRegistry())
        return shared

    def _describe(self, shared):
        """Run an @@functions control frame over the wire."""
        with _ServerConn(shared) as conn:
            conn.handshake('@@functions', {})
            status, data = conn.call(b'')
        assert status == STATUS_OK
        return json.loads(data)['functions']

    @staticmethod
    def _bigints(*values):
        return bytes(
            rowdat_1._dump([ft.LONGLONG] * len(values), [1], [list(values)]),
        )

    @staticmethod
    def _one_bigint_out(data):
        _, rows = rowdat_1._load([('r', ft.LONGLONG)], data)
        return rows[0][0]

    def test_two_v3_definitions_same_name_both_land(self):
        # Each CREATE ships its definition in its own handshake envelope.
        one = _ovl_envelope(
            'id-1', 'srv_ovl', [('int64', 'BIGINT')], 'BIGINT',
        )
        two = _ovl_envelope(
            'id-2', 'srv_ovl',
            [('int64', 'BIGINT'), ('int64', 'BIGINT')], 'BIGINT;BIGINT',
        )
        shared = self._make_shared()

        with _ServerConn(shared) as conn:
            conn.handshake('srv_ovl', one, b'return 100 + a0')
            status, data = conn.call(self._bigints(7))
            assert status == STATUS_OK, data
            assert self._one_bigint_out(data) == 107

        with _ServerConn(shared) as conn:
            conn.handshake('srv_ovl', two, b'return 200 + a0 + a1')
            status, data = conn.call(self._bigints(7, 8))
            assert status == STATUS_OK, data
            assert self._one_bigint_out(data) == 215

        # Both variants are visible to @@functions, in registration order.
        entries = self._describe(shared)
        assert [e['name'] for e in entries] == ['srv_ovl'] * 2
        assert [len(e['args']) for e in entries] == [1, 2]

        # The first variant is still callable after the second registered.
        with _ServerConn(shared) as conn:
            conn.handshake('srv_ovl', {'param_types': 'BIGINT'})
            status, data = conn.call(self._bigints(7))
            assert status == STATUS_OK, data
            assert self._one_bigint_out(data) == 107

    def test_ambiguous_call_frame_returns_clean_error(self):
        shared = self._make_shared()
        shared.create_function(
            'id-1', _ovl_sig('srv_amb', [('int64', 'BIGINT')]),
            'return 100 + a0', False,
        )
        shared.create_function(
            'id-2',
            _ovl_sig(
                'srv_amb', [('int64', 'BIGINT'), ('int64', 'BIGINT')],
            ),
            'return 200 + a0 + a1', False,
        )

        # v3 envelope with no param_types key at all.
        with _ServerConn(shared) as conn:
            conn.handshake('srv_amb', {})
            status, data = conn.call(self._bigints(7))
        assert status == STATUS_ERROR
        assert b'no parameter type list was supplied' in data
        assert b'srv_amb' in data

        # v1 carries no type segment on the wire either.
        with _ServerConn(shared) as conn:
            conn.handshake_v1('srv_amb')
            status, data = conn.call(self._bigints(7))
        assert status == STATUS_ERROR
        assert b'no parameter type list was supplied' in data

        # Neither failed call disturbed the registry.
        entries = self._describe(shared)
        assert [len(e['args']) for e in entries] == [1, 2]

    def test_delete_by_id_frame_removes_only_addressed_variant(self):
        shared = self._make_shared()
        shared.create_function(
            'id-0', _ovl_sig('srv_del', []), 'return 0', False,
        )
        shared.create_function(
            'id-1', _ovl_sig('srv_del', [('int64', 'BIGINT')]),
            'return 100 + a0', False,
        )
        shared.create_function(
            'id-2',
            _ovl_sig(
                'srv_del', [('int64', 'BIGINT'), ('int64', 'BIGINT')],
            ),
            'return 200 + a0 + a1', False,
        )

        with _ServerConn(shared) as conn:
            conn.handshake('@@delete', {})
            status, data = conn.call(json.dumps({'id': 'id-1'}).encode())
        assert status == STATUS_OK, data
        assert json.loads(data)['status'] == 'ok'

        entries = self._describe(shared)
        assert [len(e['args']) for e in entries] == [0, 2]

        # The sibling still dispatches through the server...
        with _ServerConn(shared) as conn:
            conn.handshake('srv_del', {'param_types': 'BIGINT;BIGINT'})
            status, data = conn.call(self._bigints(7, 8))
            assert status == STATUS_OK, data
            assert self._one_bigint_out(data) == 215

        # ...and the deleted arity is gone, not silently redirected.
        with _ServerConn(shared) as conn:
            conn.handshake('srv_del', {'param_types': 'BIGINT'})
            status, data = conn.call(self._bigints(7))
        assert status == STATUS_ERROR
        assert b'matches 1 arguments' in data

    def test_delete_unknown_id_frame_leaves_variants_intact(self):
        shared = self._make_shared()
        shared.create_function(
            'id-1', _ovl_sig('srv_keep', [('int64', 'BIGINT')]),
            'return 100 + a0', False,
        )
        with _ServerConn(shared) as conn:
            conn.handshake('@@delete', {})
            status, data = conn.call(json.dumps({'id': 'ghost'}).encode())
        # Control-signal failures are 200s; the body in the output mmap
        # carries the error and its code.
        assert status == STATUS_OK
        body = json.loads(data)
        assert body['code'] == 'DELETE_FUNC_NOT_REGISTERED'
        assert 'No registered function' in body['error']
        assert [len(e['args']) for e in self._describe(shared)] == [1]

    def test_create_or_replace_fresh_id_frame_supersedes(self):
        # CREATE OR REPLACE: a fresh id, the same signature, replace=false,
        # and no @@delete for the old id.
        shared = self._make_shared()
        for id, body, expected in (
            ('id-1', b'return 100 + a0', 107),
            ('id-2', b'return 200 + a0', 207),
        ):
            envelope = _ovl_envelope(
                id, 'srv_cor', [('int64', 'BIGINT')], 'BIGINT',
            )
            with _ServerConn(shared) as conn:
                conn.handshake('srv_cor', envelope, body)
                status, data = conn.call(self._bigints(7))
                assert status == STATUS_OK, data
                assert self._one_bigint_out(data) == expected

        assert not shared.has_definition('id-1')
        assert [len(e['args']) for e in self._describe(shared)] == [1]

    def test_unknown_control_signal_frame_returns_coded_error(self):
        shared = self._make_shared()
        with _ServerConn(shared) as conn:
            conn.handshake('@@bogus', {})
            status, data = conn.call(b'')
        assert status == STATUS_OK
        assert json.loads(data) == {
            'error': 'Unknown control signal: @@bogus',
            'code': 'UNKNOWN_SIGNAL',
        }

    def test_v3_semicolon_param_types_round_trip(self):
        shared = self._make_shared()
        shared.create_function(
            'id-1', _ovl_sig('srv_sep', [('int64', 'BIGINT')]),
            'return 100 + a0', False,
        )
        shared.create_function(
            'id-2',
            _ovl_sig(
                'srv_sep', [('int64', 'BIGINT'), ('int64', 'BIGINT')],
            ),
            'return 200 + a0 + a1', False,
        )

        for param_types, args, expected in (
            ('BIGINT', (7,), 107),
            ('BIGINT;BIGINT', (7, 8), 215),
        ):
            with _ServerConn(shared) as conn:
                conn.handshake('srv_sep', {'param_types': param_types})
                status, data = conn.call(self._bigints(*args))
                assert status == STATUS_OK, (param_types, data)
                assert self._one_bigint_out(data) == expected

    def test_v3_param_types_comma_inside_type_is_not_a_separator(self):
        # A naive comma split of 'DECIMAL(10,2);BIGINT' sees three
        # parameters and would land on the three-arg decoy below.
        shared = self._make_shared()
        shared.create_function(
            'id-dec',
            _ovl_sig(
                'srv_dec',
                [('decimal', 'DECIMAL(10,2)'), ('int64', 'BIGINT')],
            ),
            'return 200 + int(a0) + a1', False,
        )
        shared.create_function(
            'id-three',
            _ovl_sig('srv_dec', [('int64', 'BIGINT')] * 3),
            'return 300', False,
        )

        with _ServerConn(shared) as conn:
            conn.handshake('srv_dec', {'param_types': 'DECIMAL(10,2);BIGINT'})
            input_data = bytes(
                rowdat_1._dump(
                    [ft.NEWDECIMAL, ft.LONGLONG], [1], [['5', 8]],
                ),
            )
            status, data = conn.call(input_data)
            assert status == STATUS_OK, data
            assert self._one_bigint_out(data) == 213

    def test_zero_arg_variant_callable_through_server(self):
        shared = self._make_shared()
        shared.create_function(
            'id-0', _ovl_sig('srv_zero', []), 'return 7', False,
        )
        shared.create_function(
            'id-1', _ovl_sig('srv_zero', [('int64', 'BIGINT')]),
            'return 100 + a0', False,
        )

        # '' on the wire is a zero-arg call, not "no type info".
        with _ServerConn(shared) as conn:
            conn.handshake('srv_zero', {'param_types': ''})
            status, data = conn.call(bytes(rowdat_1._dump([], [1], [[]])))
            assert status == STATUS_OK, data
            assert self._one_bigint_out(data) == 7

        with _ServerConn(shared) as conn:
            conn.handshake('srv_zero', {'param_types': 'BIGINT'})
            status, data = conn.call(self._bigints(5))
            assert status == STATUS_OK, data
            assert self._one_bigint_out(data) == 105

    def test_bodyless_v3_definition_preserves_base_variants(self):
        # Signature-only CREATE PLUGIN FUNCTION over an overloaded builtin:
        # the body fd is empty, so there is nothing to install and
        # the pre-existing variants must be left exactly as they are.
        base = FunctionRegistry()
        base.create_function(
            'base-0', _ovl_sig('srv_base', [('int64', 'BIGINT')]),
            'return 100 + a0', False,
        )
        base.create_function(
            'base-1',
            _ovl_sig(
                'srv_base', [('int64', 'BIGINT'), ('int64', 'BIGINT')],
            ),
            'return 200 + a0 + a1', False,
        )
        base._base_function_names = {'srv_base'}

        from singlestoredb.functions.ext.plugin.server import SharedRegistry
        shared = SharedRegistry()
        shared.set_base_registry(base)
        gen_before = shared.generation

        envelope = _ovl_envelope(
            'id-nobody', 'srv_base',
            [('int64', 'BIGINT'), ('int64', 'BIGINT')], 'BIGINT;BIGINT',
        )
        with _ServerConn(shared) as conn:
            conn.handshake('srv_base', envelope, b'')
            status, data = conn.call(self._bigints(7, 8))
            assert status == STATUS_OK, data
            assert self._one_bigint_out(data) == 215

        # No registration happened: no new generation, no id mapping, and
        # the base name was not rejected as a duplicate along the way.
        assert shared.generation == gen_before
        assert not shared.has_definition('id-nobody')
        reg = shared.get_thread_local_registry()
        assert len(reg.functions['srv_base']) == 2
        assert reg._id_to_variant == {}
        assert [len(e['args']) for e in self._describe(shared)] == [1, 2]

    def test_concurrent_v3_registration_of_two_arities(self):
        shared = self._make_shared()
        definitions = [
            (
                _ovl_envelope(
                    'id-1', 'srv_race', [('int64', 'BIGINT')], 'BIGINT',
                ),
                b'return 100 + a0',
            ),
            (
                _ovl_envelope(
                    'id-2', 'srv_race',
                    [('int64', 'BIGINT'), ('int64', 'BIGINT')],
                    'BIGINT;BIGINT',
                ),
                b'return 200 + a0 + a1',
            ),
        ]
        calls = [((7,), 107), ((7, 8), 215)]
        barrier = threading.Barrier(len(definitions))
        results = [None] * len(definitions)

        def register_and_call(i):
            envelope, body = definitions[i]
            args, expected = calls[i]
            with _ServerConn(shared) as conn:
                barrier.wait(timeout=5)
                conn.handshake('srv_race', envelope, body)
                status, data = conn.call(self._bigints(*args))
                results[i] = (
                    status,
                    self._one_bigint_out(data)
                    if status == STATUS_OK else data,
                    expected,
                )

        threads = [
            threading.Thread(target=register_and_call, args=(i,))
            for i in range(len(definitions))
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
            assert not t.is_alive()

        for status, got, expected in results:
            assert status == STATUS_OK, got
            assert got == expected

        # Both landed, and the id map agrees with the variant list.
        reg = shared.get_thread_local_registry()
        assert len(reg.functions['srv_race']) == 2
        assert set(reg._id_to_variant) == {'id-1', 'id-2'}
        assert set(reg._id_to_variant.values()) == {
            ('srv_race', 'BIGINT'), ('srv_race', 'BIGINT,BIGINT'),
        }
        assert sorted(
            len(e['args']) for e in self._describe(shared)
        ) == [1, 2]

    def test_failed_install_answers_the_request_with_an_error(self):
        # A definition that won't compile must not take the connection down
        # silently: the engine is already waiting on a response frame and
        # would otherwise report only "connection reset by peer".
        shared = self._make_shared()
        envelope = _ovl_envelope(
            'id-bad', 'srv_bad', [('int64', 'BIGINT')], 'BIGINT',
        )
        with _ServerConn(shared) as conn:
            conn.handshake('srv_bad', envelope, b"return 'x' +")
            status, data = conn.call(self._bigints(7))

        assert status == STATUS_ERROR, (status, data)
        assert b'failed to install function definition' in data
        assert b'invalid syntax' in data
        assert not shared.has_definition('id-bad')

    def test_v3_body_fd_round_trips_byte_exact(self):
        # Characters a JSON-embedded body had to escape, plus non-ASCII
        # UTF-8, must reach the compiler untouched.
        shared = self._make_shared()
        body = (
            "s = 'say \"hi\" \\\\ back\\tcafé \U0001f600'\n"
            'return len(s) + a0'
        ).encode('utf8')
        envelope = _ovl_envelope(
            'id-exact', 'srv_exact', [('int64', 'BIGINT')], 'BIGINT',
        )
        with _ServerConn(shared) as conn:
            conn.handshake('srv_exact', envelope, body)
            status, data = conn.call(self._bigints(100))
        assert status == STATUS_OK, data
        assert self._one_bigint_out(data) == 100 + len(
            'say "hi" \\ back\tcafé \U0001f600',
        )

    def test_v3_cache_hit_does_not_read_body_fd(self):
        # Once the id is registered, a body that would fail UTF-8 decoding
        # is harmless: the body fd is closed unread.
        shared = self._make_shared()
        envelope = _ovl_envelope(
            'id-lazy', 'srv_lazy', [('int64', 'BIGINT')], 'BIGINT',
        )
        with _ServerConn(shared) as conn:
            conn.handshake('srv_lazy', envelope, b'return 3 + a0')
            status, data = conn.call(self._bigints(4))
            assert status == STATUS_OK, data

        with _ServerConn(shared) as conn:
            conn.handshake('srv_lazy', envelope, b'\xff\xfe not utf-8')
            status, data = conn.call(self._bigints(4))
        assert status == STATUS_OK, data
        assert self._one_bigint_out(data) == 7

    def _install_cached(self):
        """Return a SharedRegistry with id-cached already installed."""
        shared = self._make_shared()
        shared.create_function(
            'id-cached', _ovl_sig('srv_cached', [('int64', 'BIGINT')]),
            'return a0', False,
        )
        return shared

    def test_v3_cache_hit_still_requires_signature_object(self):
        shared = self._install_cached()
        for envelope in ({'id': 'id-cached'}, {'id': 'id-cached', 'signature': []}):
            with tempfile.TemporaryFile() as f:
                with self.assertRaises(ValueError) as ctx:
                    _install_v3_definition(envelope, f.fileno(), shared, None)
            assert "required object field 'signature'" in str(ctx.exception)

    def test_v3_envelope_with_id_but_no_signature_is_rejected(self):
        # Without a signature the handshake must still go through the
        # installer, which rejects it, rather than skip it.
        shared = self._install_cached()
        with _ServerConn(shared) as conn:
            conn.handshake('srv_cached', {'id': 'id-cached'})
            status, data = conn.call(self._bigints(4))
        assert status == STATUS_ERROR
        assert b'failed to install function definition' in data
        assert b"required object field 'signature'" in data

    def test_v3_cache_hit_still_enforces_body_limit(self):
        shared = self._install_cached()
        envelope = _ovl_envelope(
            'id-cached', 'srv_cached', [('int64', 'BIGINT')], 'BIGINT',
        )
        with tempfile.TemporaryFile() as f:
            # Sparse: sets the size without writing 64 MiB.
            os.ftruncate(f.fileno(), _MAX_V3_BODY_BYTES + 1)
            with self.assertRaises(ValueError) as ctx:
                _install_v3_definition(envelope, f.fileno(), shared, None)
        assert str(ctx.exception) == (
            f'v3 body size {_MAX_V3_BODY_BYTES + 1} '
            f'exceeds limit {_MAX_V3_BODY_BYTES}'
        )

    def test_v3_cache_hit_with_empty_body_is_a_noop(self):
        shared = self._install_cached()
        generation = shared.generation
        envelope = _ovl_envelope(
            'id-cached', 'srv_cached', [('int64', 'BIGINT')], 'BIGINT',
        )
        with tempfile.TemporaryFile() as f:
            _install_v3_definition(envelope, f.fileno(), shared, None)
        assert shared.generation == generation
        assert shared.has_definition('id-cached')

    def test_v3_invalid_utf8_body_rejected(self):
        shared = self._make_shared()
        envelope = _ovl_envelope(
            'id-utf8', 'srv_utf8', [('int64', 'BIGINT')], 'BIGINT',
        )
        with _ServerConn(shared) as conn:
            conn.handshake('srv_utf8', envelope, b'return a0\xff')
            status, data = conn.call(self._bigints(1))
        assert status == STATUS_ERROR, (status, data)
        assert b'not valid UTF-8' in data
        assert not shared.has_definition('id-utf8')

    def test_v3_wrong_fd_count_rejected_without_leaking(self):
        # v3 requires exactly 4 fds. Pass the write end of a pipe as every
        # fd: once all received copies are closed, a non-blocking read on
        # the read end sees EOF; a leaked copy would make it EAGAIN.
        shared = self._make_shared()
        for n in (3, 5):
            r, w = os.pipe()
            os.set_blocking(r, False)
            try:
                with _ServerConn(shared) as conn:
                    name = b'f'
                    conn.sock.sendall(struct.pack('<QQ', 3, len(name)))
                    conn.sock.sendmsg(
                        [name],
                        [(
                            socket.SOL_SOCKET, socket.SCM_RIGHTS,
                            array.array('i', [w] * n),
                        )],
                    )
                    os.close(w)
                    w = -1
                    # The server abandons the handshake without replying.
                    conn._thread.join(timeout=5)
                    assert not conn._thread.is_alive(), n
                assert os.read(r, 1) == b'', f'{n} fds: a received fd leaked'
            finally:
                os.close(r)
                if w >= 0:
                    os.close(w)


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
            header = struct.pack(
                '<QQ', SUPPORTED_VERSIONS[0], _MAX_FUNCTION_NAME_LEN + 1,
            )
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
            header = struct.pack(
                '<QQ', SUPPORTED_VERSIONS[0], _MAX_FUNCTION_NAME_LEN,
            )
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

    def test_v2_param_types_len_too_large(self):
        import singlestoredb.functions.ext.plugin.connection as conn_mod
        requested = []
        real_recv_exact = conn_mod._recv_exact_py

        def spy(sock, n):
            requested.append(n)
            # Don't let a regression allocate the oversized buffer.
            if n > conn_mod._MAX_V2_PARAM_TYPES_BYTES:
                return None
            return real_recv_exact(sock, n)

        a, b = socket.socketpair()
        input_fd = _temp_fd()
        output_fd = _temp_fd()
        try:
            name = b'f'
            b.sendall(struct.pack('<QQ', 2, len(name)))
            b.sendmsg(
                [name],
                [(
                    socket.SOL_SOCKET, socket.SCM_RIGHTS,
                    array.array('i', [input_fd, output_fd]),
                )],
            )
            b.sendall(
                struct.pack('<Q', conn_mod._MAX_V2_PARAM_TYPES_BYTES + 1),
            )
            b.close()
            with patch.object(conn_mod, '_recv_exact_py', spy):
                _handle_connection_inner(
                    a, self._make_shared_registry(), threading.Event(),
                )
        finally:
            a.close()
            os.close(input_fd)
            os.close(output_fd)
        assert max(requested) <= conn_mod._MAX_V2_PARAM_TYPES_BYTES

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

    def _assert_request_fds_released(self, version, extra_fds):
        """Hand the server two pipe write ends as the request fds, end the
        handshake early, and check the server closed its copies.

        Each pipe's read end only sees EOF once every write end is closed.
        The test closes its own copies after sending, so a non-blocking
        read that raises instead of returning ``b''`` means the server
        leaked one.
        """
        pipes = [os.pipe(), os.pipe()]
        for r, _ in pipes:
            os.set_blocking(r, False)
        a, b = socket.socketpair()
        try:
            name = b'fn'
            b.sendall(struct.pack('<QQ', version, len(name)))
            fds = array.array('i', [w for _, w in pipes] + extra_fds)
            b.sendmsg(
                [name], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, fds)],
            )
            for _, w in pipes:
                os.close(w)
            b.close()
            _handle_connection_inner(
                a, self._make_shared_registry(), threading.Event(),
            )
            for r, _ in pipes:
                assert os.read(r, 1) == b'', 'request fd still open'
        finally:
            a.close()
            b.close()
            for r, _ in pipes:
                os.close(r)

    def test_v3_bad_envelope_releases_request_fds(self):
        meta_fd = _temp_fd()
        body_fd = _temp_fd()
        try:
            os.write(meta_fd, b'{not json')
            self._assert_request_fds_released(
                3, [meta_fd, body_fd],
            )
        finally:
            os.close(meta_fd)
            os.close(body_fd)

    def test_v3_non_utf8_envelope_releases_request_fds(self):
        # Not valid UTF-8, so json.loads raises UnicodeDecodeError. The
        # handshake is dropped like any other bad envelope, not raised.
        # (No leading BOM bytes: json.loads would sniff those as UTF-16.)
        meta_fd = _temp_fd()
        body_fd = _temp_fd()
        try:
            os.write(meta_fd, b'{"name": "\xff"}')
            self._assert_request_fds_released(
                3, [meta_fd, body_fd],
            )
        finally:
            os.close(meta_fd)
            os.close(body_fd)

    def test_v2_disconnect_before_type_list_releases_request_fds(self):
        # The peer hangs up instead of sending the 8-byte type-list length.
        self._assert_request_fds_released(2, [])

    def test_v2_and_v3_accepted_as_versions(self):
        assert 1 in SUPPORTED_VERSIONS
        assert 2 in SUPPORTED_VERSIONS
        assert 3 in SUPPORTED_VERSIONS


if __name__ == '__main__':
    unittest.main()
