#!/usr/bin/env python
# type: ignore
"""Tests for UDF overload resolution in the plugin registry.

Covers the pure functions in ``plugin.overload`` and the overload-aware
behavior of ``FunctionRegistry`` (duplicate detection in
``_register_function``, per-variant ``create_function`` / ``delete_function``,
and ``lookup_variant`` wire-input handling).
"""
import json
import unittest

from singlestoredb.functions.ext.plugin import overload
from singlestoredb.functions.ext.plugin.registry import FunctionRegistry


class TestNormalizeSqlType(unittest.TestCase):

    def test_small_int_group_collapses(self):
        for t in ('TINYINT', 'SMALLINT', 'BOOL', 'BOOLEAN'):
            assert overload.normalize_sql_type(t) == 'TINYINT_GROUP', t

    def test_int_group_collapses(self):
        for t in ('MEDIUMINT', 'INT', 'INTEGER'):
            assert overload.normalize_sql_type(t) == 'INT_GROUP', t

    def test_bigint_stands_alone(self):
        assert overload.normalize_sql_type('BIGINT') == 'BIGINT'

    def test_length_stripped(self):
        assert overload.normalize_sql_type('VARCHAR(255)') == 'VARCHAR_GROUP'
        assert overload.normalize_sql_type('DECIMAL(10,2)') == 'DECIMAL'
        assert overload.normalize_sql_type('CHAR(10)') == 'CHAR_GROUP'

    def test_modifiers_stripped(self):
        assert overload.normalize_sql_type('BIGINT UNSIGNED') == 'BIGINT'
        assert overload.normalize_sql_type('bigint not null') == 'BIGINT'
        assert (
            overload.normalize_sql_type('INT UNSIGNED ZEROFILL')
            == 'INT_GROUP'
        )

    def test_blob_text_family(self):
        for t in (
            'TEXT', 'TINYTEXT', 'MEDIUMTEXT', 'LONGTEXT',
            'BLOB', 'TINYBLOB', 'MEDIUMBLOB', 'LONGBLOB',
        ):
            assert overload.normalize_sql_type(t) == 'BLOB_GROUP', t

    def test_unknown_type_passes_through_uppercased(self):
        assert overload.normalize_sql_type('geography') == 'GEOGRAPHY'


class TestParseParamTypes(unittest.TestCase):

    def test_empty_string(self):
        assert overload.parse_param_types('') == []
        assert overload.parse_param_types('   ') == []

    def test_multi_arg(self):
        assert (
            overload.parse_param_types('BIGINT, VARCHAR(10)')
            == ['BIGINT', 'VARCHAR_GROUP']
        )


class TestMatchKind(unittest.TestCase):

    def test_exact(self):
        assert overload.match_kind('BIGINT', 'BIGINT') is overload.MatchKind.EXACT

    def test_numeric_compatible(self):
        assert (
            overload.match_kind('BIGINT', 'INT_GROUP')
            is overload.MatchKind.COMPATIBLE
        )
        assert (
            overload.match_kind('DECIMAL', 'DOUBLE')
            is overload.MatchKind.COMPATIBLE
        )

    def test_string_compatible(self):
        assert (
            overload.match_kind('VARCHAR_GROUP', 'BLOB_GROUP')
            is overload.MatchKind.COMPATIBLE
        )

    def test_none(self):
        assert (
            overload.match_kind('BIGINT', 'VARCHAR_GROUP')
            is overload.MatchKind.NONE
        )


def _variant(param_sql_types, marker):
    """Test-only variant dict; only `param_sql_types` matters for resolve."""
    return {'param_sql_types': list(param_sql_types), '_marker': marker}


class TestResolveDispatch(unittest.TestCase):

    def test_no_variants(self):
        with self.assertRaises(ValueError):
            overload.resolve('f', [], ['BIGINT'])

    def test_none_param_types_single_variant(self):
        v = _variant(['BIGINT'], 'a')
        assert overload.resolve('f', [v], None) is v

    def test_none_param_types_multi_variant_raises(self):
        with self.assertRaises(ValueError):
            overload.resolve(
                'f',
                [_variant(['BIGINT'], 'a'), _variant(['VARCHAR_GROUP'], 'b')],
                None,
            )

    def test_arity_filter(self):
        with self.assertRaises(ValueError) as ctx:
            overload.resolve(
                'f', [_variant(['BIGINT'], 'a')], ['BIGINT', 'BIGINT'],
            )
        assert 'matches 2 arguments' in str(ctx.exception)

    def test_exact_wins_over_compatible(self):
        # Both variants pass compatibility, but the exact BIGINT match wins.
        exact = _variant(['BIGINT'], 'exact')
        compat = _variant(['INT_GROUP'], 'compat')
        chosen = overload.resolve('f', [exact, compat], ['BIGINT'])
        assert chosen['_marker'] == 'exact'

    def test_compatible_only(self):
        v = _variant(['INT_GROUP'], 'compat')
        chosen = overload.resolve('f', [v], ['BIGINT'])
        assert chosen['_marker'] == 'compat'

    def test_no_arg_types_match(self):
        with self.assertRaises(ValueError) as ctx:
            overload.resolve(
                'f', [_variant(['BIGINT'], 'a')], ['VARCHAR_GROUP'],
            )
        assert 'matches argument types' in str(ctx.exception)

    def test_ambiguous_tie(self):
        # Two variants match with the same exact-count (both COMPATIBLE) —
        # neither is preferred, so this must raise.
        a = _variant(['INT_GROUP'], 'a')
        b = _variant(['DECIMAL'], 'b')
        with self.assertRaises(ValueError) as ctx:
            overload.resolve('f', [a, b], ['BIGINT'])
        assert 'ambiguous' in str(ctx.exception)


def _sig(name, arg_sqls, arg_dtype='int64', ret_sql='BIGINT'):
    return {
        'name': name,
        'args': [
            {'name': f'a{i}', 'dtype': arg_dtype, 'sql': s}
            for i, s in enumerate(arg_sqls)
        ],
        'returns': [{'name': '', 'dtype': 'int64', 'sql': ret_sql}],
    }


class TestRegistryDuplicateDetection(unittest.TestCase):

    def test_duplicate_exact_signature_rejected(self):
        reg = FunctionRegistry()
        reg._register_function(lambda x: x, 'f', _sig('f', ['BIGINT']))
        with self.assertRaises(ValueError) as ctx:
            reg._register_function(lambda x: x, 'f', _sig('f', ['BIGINT']))
        assert 'already has a variant' in str(ctx.exception)

    def test_duplicate_group_equivalent_rejected(self):
        # CHAR(10) and CHAR(20) collapse to the same canonical key.
        reg = FunctionRegistry()
        reg._register_function(
            lambda x: x, 'f', _sig('f', ['CHAR(10)'], arg_dtype='str'),
        )
        with self.assertRaises(ValueError):
            reg._register_function(
                lambda x: x, 'f', _sig('f', ['CHAR(20)'], arg_dtype='str'),
            )

    def test_different_groups_register_separately(self):
        reg = FunctionRegistry()
        reg._register_function(lambda x: x, 'f', _sig('f', ['INT']))
        reg._register_function(lambda x: x, 'f', _sig('f', ['BIGINT']))
        assert len(reg.functions['f']) == 2


class TestLookupVariant(unittest.TestCase):

    def _seed_two_variants(self):
        reg = FunctionRegistry()
        reg._register_function(lambda x: x + 1, 'f', _sig('f', ['BIGINT']))
        reg._register_function(
            lambda x: x + 'x', 'f', _sig('f', ['VARCHAR(10)'], arg_dtype='str'),
        )
        return reg

    def test_dispatch_by_wire_type_string(self):
        reg = self._seed_two_variants()
        bigint_variant = reg.lookup_variant('f', 'BIGINT')
        assert bigint_variant['param_sql_types'] == ['BIGINT']
        varchar_variant = reg.lookup_variant('f', 'VARCHAR(255)')
        assert varchar_variant['param_sql_types'] == ['VARCHAR_GROUP']

    def test_empty_string_treated_as_none(self):
        reg = FunctionRegistry()
        reg._register_function(lambda x: x, 'f', _sig('f', ['BIGINT']))
        # Single variant + no type info → returns it.
        assert reg.lookup_variant('f', '') is reg.functions['f'][0]
        assert reg.lookup_variant('f', None) is reg.functions['f'][0]

    def test_none_with_ambiguity_raises(self):
        reg = self._seed_two_variants()
        with self.assertRaises(ValueError):
            reg.lookup_variant('f', None)

    def test_unknown_function_raises(self):
        reg = FunctionRegistry()
        with self.assertRaises(ValueError):
            reg.lookup_variant('nope', 'BIGINT')


class TestCreateFunctionOverload(unittest.TestCase):

    def test_two_variants_coexist_and_delete_one(self):
        reg = FunctionRegistry()
        # Not a "base" function — dynamic registration is allowed.
        sig_int = json.dumps(_sig('dyn_over', ['BIGINT']))
        sig_flt = json.dumps(
            _sig(
                'dyn_over', ['DOUBLE'],
                arg_dtype='float64', ret_sql='DOUBLE',
            ),
        )
        reg.create_function('id-int', sig_int, 'return a0 + 1', False)
        reg.create_function('id-flt', sig_flt, 'return a0 + 2.0', False)

        variants = reg.functions['dyn_over']
        assert len(variants) == 2
        assert ('dyn_over', 'BIGINT') in reg._id_to_variant.values()
        assert ('dyn_over', 'DOUBLE') in reg._id_to_variant.values()

        # Delete the BIGINT variant only; DOUBLE variant survives.
        reg.delete_function('id-int')
        remaining = reg.functions['dyn_over']
        assert len(remaining) == 1
        assert remaining[0]['param_sql_types'] == ['DOUBLE']
        # The BIGINT variant's id-mapping is gone.
        assert 'id-int' not in reg._id_to_variant
        assert 'id-flt' in reg._id_to_variant

    def test_duplicate_signature_without_replace_rejected(self):
        reg = FunctionRegistry()
        sig = json.dumps(_sig('dyn_dup', ['BIGINT']))
        reg.create_function('id-1', sig, 'return a0 + 1', False)
        with self.assertRaises(ValueError) as ctx:
            reg.create_function('id-2', sig, 'return a0 + 2', False)
        assert 'already exists' in str(ctx.exception)

    def test_duplicate_signature_with_replace_swaps_variant(self):
        reg = FunctionRegistry()
        sig = json.dumps(_sig('dyn_rep', ['BIGINT']))
        reg.create_function('id-1', sig, 'return a0 + 1', False)
        reg.create_function('id-2', sig, 'return a0 + 42', True)
        # Only the new id remains; only one variant lives under the name.
        assert 'id-1' not in reg._id_to_variant
        assert 'id-2' in reg._id_to_variant
        assert len(reg.functions['dyn_rep']) == 1


if __name__ == '__main__':
    unittest.main()
