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

    def test_case_and_whitespace_insensitive(self):
        for t in ('varchar', 'VarChar(20)', '  VARCHAR ', 'varchar (20)'):
            assert overload.normalize_sql_type(t) == 'VARCHAR_GROUP', t
        for t in ('int', '  INT  ', 'Integer'):
            assert overload.normalize_sql_type(t) == 'INT_GROUP', t

    def test_decimal_and_numeric_share_key(self):
        assert (
            overload.normalize_sql_type('DECIMAL(10,2)')
            == overload.normalize_sql_type('NUMERIC(10,2)')
            == 'DECIMAL'
        )

    def test_real_maps_to_double(self):
        assert overload.normalize_sql_type('REAL') == 'DOUBLE'
        assert (
            overload.normalize_sql_type('REAL')
            == overload.normalize_sql_type('DOUBLE')
        )

    def test_unsigned_invisible_to_overloading(self):
        # The engine drops the UNSIGNED bit when it compares overloads, so
        # INT and INT UNSIGNED occupy the same overload slot.
        assert (
            overload.normalize_sql_type('INT UNSIGNED')
            == overload.normalize_sql_type('INT')
            == 'INT_GROUP'
        )
        assert (
            overload.normalize_sql_type('BIGINT UNSIGNED NOT NULL')
            == overload.normalize_sql_type('BIGINT')
            == 'BIGINT'
        )
        assert (
            overload.normalize_sql_type('VARCHAR(255) NOT NULL')
            == 'VARCHAR_GROUP'
        )

    def test_temporal_and_json_groups(self):
        assert overload.normalize_sql_type('JSON') == 'JSON'
        assert overload.normalize_sql_type('DATE') == 'DATE'
        assert overload.normalize_sql_type('TIME') == 'TIME'
        # DATETIME and TIMESTAMP are the same overload slot.
        assert (
            overload.normalize_sql_type('DATETIME')
            == overload.normalize_sql_type('TIMESTAMP')
            == 'DATETIME'
        )

    def test_temporal_and_json_are_not_cross_compatible(self):
        keys = [
            overload.normalize_sql_type(t)
            for t in ('JSON', 'DATE', 'TIME', 'DATETIME')
        ]
        for i, a in enumerate(keys):
            for j, b in enumerate(keys):
                kind = overload.match_kind(a, b)
                if i == j:
                    assert kind is overload.MatchKind.EXACT, (a, b)
                else:
                    assert kind is overload.MatchKind.NONE, (a, b)

    def test_datetime_timestamp_match_exactly(self):
        assert (
            overload.match_kind(
                overload.normalize_sql_type('DATETIME'),
                overload.normalize_sql_type('TIMESTAMP'),
            ) is overload.MatchKind.EXACT
        )


class TestParseParamTypes(unittest.TestCase):

    def test_empty_string(self):
        assert overload.parse_param_types('') == []
        assert overload.parse_param_types('   ') == []

    def test_multi_arg(self):
        assert (
            overload.parse_param_types('BIGINT;VARCHAR(10)')
            == ['BIGINT', 'VARCHAR_GROUP']
        )
        assert (
            overload.parse_param_types(' BIGINT ; varchar(10) ')
            == ['BIGINT', 'VARCHAR_GROUP']
        )

    def test_engine_separator_is_semicolon(self):
        # ';' is the canonical separator emitted by the engine.
        assert (
            overload.parse_param_types('BIGINT;TEXT;BIGINT')
            == ['BIGINT', 'BLOB_GROUP', 'BIGINT']
        )

    def test_comma_is_not_a_separator(self):
        # ';' is the only separator. A comma belongs to a type's own syntax,
        # so a comma-separated list is not split — it falls through as a
        # single unrecognized type name rather than being mis-parsed.
        assert (
            overload.parse_param_types('BIGINT, VARCHAR(10)')
            == ['BIGINT, VARCHAR(10)']
        )

    def test_decimal_precision_comma_preserved(self):
        # The comma inside DECIMAL(10,2) must not split the list.
        assert (
            overload.parse_param_types('DECIMAL(10,2);DECIMAL(4,1)')
            == ['DECIMAL', 'DECIMAL']
        )

    def test_zero_arg_round_trip(self):
        # "" on the wire -> [] -> a stable key distinct from any 1-arg key.
        parsed = overload.parse_param_types('')
        assert parsed == []
        assert overload.signature_key(parsed) == ''
        assert (
            overload.signature_key(parsed)
            != overload.signature_key(overload.parse_param_types('BIGINT'))
        )
        # Stable across calls.
        assert (
            overload.signature_key(overload.parse_param_types('  '))
            == overload.signature_key(parsed)
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

    def test_zero_arg_variant_hit_by_zero_arg_call(self):
        v = _variant([], 'zero')
        assert overload.resolve('f', [v], []) is v

    def test_zero_arg_and_one_arg_variants_coexist(self):
        zero = _variant([], 'zero')
        one = _variant(['BIGINT'], 'one')
        assert overload.resolve('f', [zero, one], [])['_marker'] == 'zero'
        assert (
            overload.resolve('f', [zero, one], ['BIGINT'])['_marker'] == 'one'
        )

    def test_zero_arg_call_against_one_arg_only(self):
        with self.assertRaises(ValueError) as ctx:
            overload.resolve('f', [_variant(['BIGINT'], 'one')], [])
        assert 'matches 0 arguments' in str(ctx.exception)

    def test_numeric_compat_survives_when_string_variant_filtered_out(self):
        # Both variants survive the arity filter; only the numeric one
        # survives the type filter, so no ambiguity is reported.
        num = _variant(['INT_GROUP'], 'num')
        string = _variant(['VARCHAR_GROUP'], 'str')
        chosen = overload.resolve('f', [num, string], ['BIGINT'])
        assert chosen['_marker'] == 'num'

    def test_multi_arg_exact_count_breaks_tie(self):
        # (BIGINT, VARCHAR) vs (INT, VARCHAR) called with (BIGINT, BLOB):
        # both compatible, but the first has one more exact match.
        a = _variant(['BIGINT', 'VARCHAR_GROUP'], 'a')
        b = _variant(['INT_GROUP', 'VARCHAR_GROUP'], 'b')
        chosen = overload.resolve('f', [a, b], ['BIGINT', 'BLOB_GROUP'])
        assert chosen['_marker'] == 'a'

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

    def test_char_vs_binary_different_lengths_rejected(self):
        # CHAR and BINARY share a group and length is stripped, so
        # CHAR(10) vs BINARY(20) is a duplicate.
        reg = FunctionRegistry()
        reg._register_function(
            lambda x: x, 'f', _sig('f', ['CHAR(10)'], arg_dtype='str'),
        )
        with self.assertRaises(ValueError) as ctx:
            reg._register_function(
                lambda x: x, 'f', _sig('f', ['BINARY(20)'], arg_dtype='bytes'),
            )
        assert 'already has a variant' in str(ctx.exception)

    def test_multi_arg_group_equivalent_rejected(self):
        # (INT, VARCHAR) vs (MEDIUMINT, VARBINARY) — both args collapse to
        # the same groups, so the whole signature is a duplicate.
        reg = FunctionRegistry()
        reg._register_function(
            lambda x, y: x, 'f',
            _sig('f', ['INT', 'VARCHAR(10)']),
        )
        with self.assertRaises(ValueError) as ctx:
            reg._register_function(
                lambda x, y: x, 'f',
                _sig('f', ['MEDIUMINT', 'VARBINARY(20)']),
            )
        assert 'already has a variant' in str(ctx.exception)
        assert len(reg.functions['f']) == 1

    def test_unsigned_collides_with_signed(self):
        # The engine drops UNSIGNED for overload dispatch, so a uint32
        # variant is a duplicate of an int32 variant. The SQL strings here
        # are exactly what get_signature() emits for those dtypes.
        reg = FunctionRegistry()
        reg._register_function(
            lambda x: x, 'f',
            _sig('f', ['INT NOT NULL'], arg_dtype='int32'),
        )
        with self.assertRaises(ValueError) as ctx:
            reg._register_function(
                lambda x: x, 'f',
                _sig('f', ['INT UNSIGNED NOT NULL'], arg_dtype='uint32'),
            )
        assert 'already has a variant' in str(ctx.exception)
        assert len(reg.functions['f']) == 1

    def test_zero_arg_and_one_arg_variants_coexist(self):
        reg = FunctionRegistry()
        reg._register_function(lambda: 1, 'f', _sig('f', []))
        reg._register_function(lambda x: x, 'f', _sig('f', ['BIGINT']))
        assert len(reg.functions['f']) == 2
        assert (
            reg.lookup_variant('f', 'BIGINT')['param_sql_types'] == ['BIGINT']
        )
        assert reg.lookup_variant('f', '')['param_sql_types'] == []

    def test_zero_arg_overload_reachable_beside_sibling(self):
        # '' must reach the zero-arg variant even when siblings exist;
        # folding '' into None would report a false ambiguity here.
        reg = FunctionRegistry()
        reg._register_function(lambda: 1, 'f', _sig('f', []))
        reg._register_function(lambda x: x, 'f', _sig('f', ['BIGINT']))
        reg._register_function(
            lambda x, y: x, 'f', _sig('f', ['BIGINT', 'BIGINT']),
        )
        assert reg.lookup_variant('f', '')['param_sql_types'] == []
        assert reg.lookup_variant('f', 'BIGINT;BIGINT')['param_sql_types'] == [
            'BIGINT', 'BIGINT',
        ]

    def test_duplicate_zero_arg_rejected(self):
        reg = FunctionRegistry()
        reg._register_function(lambda: 1, 'f', _sig('f', []))
        with self.assertRaises(ValueError):
            reg._register_function(lambda: 2, 'f', _sig('f', []))


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

    def test_none_means_no_type_info(self):
        reg = FunctionRegistry()
        reg._register_function(lambda x: x, 'f', _sig('f', ['BIGINT']))
        # Single variant + no type info (v1 wire) → returns it.
        assert reg.lookup_variant('f', None) is reg.functions['f'][0]

    def test_empty_string_means_zero_args(self):
        # '' is what the engine sends for a zero-arg call, so it must not
        # be read as "no type info": against a one-arg function it is an
        # arity mismatch, not a fallback to the sole variant.
        reg = FunctionRegistry()
        reg._register_function(lambda x: x, 'f', _sig('f', ['BIGINT']))
        with self.assertRaises(ValueError) as ctx:
            reg.lookup_variant('f', '')
        assert 'matches 0 arguments' in str(ctx.exception)

    def test_empty_string_reaches_zero_arg_variant(self):
        reg = FunctionRegistry()
        reg._register_function(lambda: 1, 'f', _sig('f', []))
        assert reg.lookup_variant('f', '') is reg.functions['f'][0]

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
