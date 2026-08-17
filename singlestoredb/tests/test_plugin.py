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
from singlestoredb.functions.ext.plugin.connection import _MAX_FUNCTION_NAME_LEN
from singlestoredb.functions.ext.plugin.connection import _recv_exact_py
from singlestoredb.functions.ext.plugin.connection import STATUS_ERROR
from singlestoredb.functions.ext.plugin.connection import STATUS_OK
from singlestoredb.functions.ext.plugin.connection import SUPPORTED_VERSIONS
from singlestoredb.functions.ext.plugin.control import dispatch_control_signal
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
        assert 'Unknown control signal' in result.data

    def test_register_signal_no_longer_supported(self):
        """@@register was replaced by in-band v3 envelope registration."""
        shared = self._make_shared_registry()
        result = dispatch_control_signal('@@register', b'', shared)
        assert result.ok is False
        assert 'Unknown control signal' in result.data

    def test_delete_missing_payload(self):
        shared = self._make_shared_registry()
        result = dispatch_control_signal('@@delete', b'', shared)
        assert result.ok is False
        assert 'Missing deletion payload' in result.data

    def test_delete_invalid_json(self):
        shared = self._make_shared_registry()
        result = dispatch_control_signal('@@delete', b'not json', shared)
        assert result.ok is False
        assert 'Invalid JSON' in result.data

    def test_delete_missing_id(self):
        shared = self._make_shared_registry()
        payload = json.dumps({}).encode()
        result = dispatch_control_signal('@@delete', payload, shared)
        assert result.ok is False
        assert 'id' in result.data

    def test_delete_unknown_id(self):
        shared = self._make_shared_registry()
        shared.delete_function.side_effect = ValueError(
            "No registered function with id 'no_such'",
        )
        payload = json.dumps({'id': 'no_such'}).encode()
        result = dispatch_control_signal('@@delete', payload, shared)
        assert result.ok is False
        assert 'No registered function' in result.data

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


def _ovl_definition(id, name, args, body, replace=False):
    """Build a v3 handshake envelope ``definition`` object."""
    sig = json.loads(_ovl_sig(name, args))
    return {
        'id': id,
        'name': name,
        'args': sig['args'],
        'returns': sig['returns'],
        'body': body,
        'replace': replace,
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

    def handshake(self, function_name, envelope, version=3):
        """Send a v3 handshake: header, name, and 3 FDs.

        The third FD is the metadata memfd carrying the JSON envelope; the
        engine seals it, so its size alone delimits the payload.
        """
        name_bytes = function_name.encode('utf8')
        self.sock.sendall(struct.pack('<QQ', version, len(name_bytes)))
        meta_fd = _temp_fd()
        try:
            os.write(meta_fd, json.dumps(envelope).encode('utf8'))
            fds = array.array('i', [self.input_fd, self.output_fd, meta_fd])
            self.sock.sendmsg(
                [name_bytes],
                [(socket.SOL_SOCKET, socket.SCM_RIGHTS, fds)],
            )
        finally:
            os.close(meta_fd)

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
        one = _ovl_definition(
            'id-1', 'srv_ovl', [('int64', 'BIGINT')], 'return 100 + a0',
        )
        two = _ovl_definition(
            'id-2', 'srv_ovl',
            [('int64', 'BIGINT'), ('int64', 'BIGINT')],
            'return 200 + a0 + a1',
        )
        shared = self._make_shared()

        with _ServerConn(shared) as conn:
            conn.handshake(
                'srv_ovl', {'definition': one, 'param_types': 'BIGINT'},
            )
            status, data = conn.call(self._bigints(7))
            assert status == STATUS_OK, data
            assert self._one_bigint_out(data) == 107

        with _ServerConn(shared) as conn:
            conn.handshake(
                'srv_ovl',
                {'definition': two, 'param_types': 'BIGINT;BIGINT'},
            )
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
        assert status != STATUS_OK
        assert b'No registered function' in data
        assert [len(e['args']) for e in self._describe(shared)] == [1]

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
        # the envelope carries no body, so there is nothing to install and
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

        definition = _ovl_definition(
            'id-nobody', 'srv_base',
            [('int64', 'BIGINT'), ('int64', 'BIGINT')], '',
        )
        with _ServerConn(shared) as conn:
            conn.handshake(
                'srv_base',
                {'definition': definition, 'param_types': 'BIGINT;BIGINT'},
            )
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
            _ovl_definition(
                'id-1', 'srv_race', [('int64', 'BIGINT')], 'return 100 + a0',
            ),
            _ovl_definition(
                'id-2', 'srv_race',
                [('int64', 'BIGINT'), ('int64', 'BIGINT')],
                'return 200 + a0 + a1',
            ),
        ]
        calls = [
            ('BIGINT', (7,), 107),
            ('BIGINT;BIGINT', (7, 8), 215),
        ]
        barrier = threading.Barrier(len(definitions))
        results = [None] * len(definitions)

        def register_and_call(i):
            definition = definitions[i]
            param_types, args, expected = calls[i]
            with _ServerConn(shared) as conn:
                barrier.wait(timeout=5)
                conn.handshake(
                    'srv_race',
                    {'definition': definition, 'param_types': param_types},
                )
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
        definition = _ovl_definition(
            'id-bad', 'srv_bad', [('int64', 'BIGINT')], "return 'x' +",
        )
        with _ServerConn(shared) as conn:
            conn.handshake(
                'srv_bad',
                {'definition': definition, 'param_types': 'BIGINT'},
            )
            status, data = conn.call(self._bigints(7))

        assert status == STATUS_ERROR, (status, data)
        assert b'failed to install function definition' in data
        assert b'invalid syntax' in data
        assert not shared.has_definition('id-bad')


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

    def test_v2_and_v3_accepted_as_versions(self):
        assert 1 in SUPPORTED_VERSIONS
        assert 2 in SUPPORTED_VERSIONS
        assert 3 in SUPPORTED_VERSIONS


if __name__ == '__main__':
    unittest.main()
