"""
Overloaded-function resolution for the plugin registry.

Multiple UDFs may share a name when their parameter signatures differ.
This module implements SingleStoreDB's overload resolution rules (see
https://docs.singlestore.com/db/v9.1/.../overloaded-functions-and-stored-procedures/):

1. Filter variants by argument count.
2. Filter by per-arg match (exact OR compatible).
3. Prefer the variant with the most exact matches.
4. A tie on exact-match count is an ambiguity error.

SQL types that SingleStoreDB treats as equivalent for overloading (e.g.
TINYINT/SMALLINT, MEDIUMINT/INT, VARCHAR/VARBINARY, CHAR(N)/BINARY(N),
BLOB/TEXT family) collapse to the same canonical key, so registering two
variants that differ only in those types is rejected as a duplicate.
"""
from __future__ import annotations

import re
from enum import Enum
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple


# Canonical SQL type groups. Two types in the same group are treated as
# the *same* type for both exact-match and duplicate-registration
# purposes. Two types in the same broad category (numeric / string) but
# different groups are merely *compatible*.
_TYPE_GROUPS: Dict[str, str] = {
    # tiny/small integer group
    'TINYINT': 'TINYINT_GROUP',
    'SMALLINT': 'TINYINT_GROUP',
    'BOOL': 'TINYINT_GROUP',
    'BOOLEAN': 'TINYINT_GROUP',
    # medium/int group
    'MEDIUMINT': 'INT_GROUP',
    'INT': 'INT_GROUP',
    'INTEGER': 'INT_GROUP',
    # bigint stands alone
    'BIGINT': 'BIGINT',
    # floats
    'FLOAT': 'FLOAT',
    'DOUBLE': 'DOUBLE',
    'REAL': 'DOUBLE',
    'DECIMAL': 'DECIMAL',
    'NUMERIC': 'DECIMAL',
    # char/binary group (length stripped before lookup)
    'CHAR': 'CHAR_GROUP',
    'BINARY': 'CHAR_GROUP',
    # varchar/varbinary group
    'VARCHAR': 'VARCHAR_GROUP',
    'VARBINARY': 'VARCHAR_GROUP',
    # blob/text family
    'TEXT': 'BLOB_GROUP',
    'TINYTEXT': 'BLOB_GROUP',
    'MEDIUMTEXT': 'BLOB_GROUP',
    'LONGTEXT': 'BLOB_GROUP',
    'BLOB': 'BLOB_GROUP',
    'TINYBLOB': 'BLOB_GROUP',
    'MEDIUMBLOB': 'BLOB_GROUP',
    'LONGBLOB': 'BLOB_GROUP',
    'JSON': 'JSON',
    # temporal
    'DATE': 'DATE',
    'TIME': 'TIME',
    'DATETIME': 'DATETIME',
    'TIMESTAMP': 'DATETIME',
}

_NUMERIC_GROUPS = frozenset({
    'TINYINT_GROUP', 'INT_GROUP', 'BIGINT', 'FLOAT', 'DOUBLE', 'DECIMAL',
})
_STRING_GROUPS = frozenset({
    'CHAR_GROUP', 'VARCHAR_GROUP', 'BLOB_GROUP',
})


class MatchKind(Enum):
    """Match category for a single parameter."""
    EXACT = 2
    COMPATIBLE = 1
    NONE = 0


def normalize_sql_type(raw: str) -> str:
    """Collapse a raw SQL type to its canonical group key.

    Strips length/precision specifiers, `UNSIGNED`, `NOT NULL`, surrounding
    whitespace, then maps known equivalences (TINYINT/SMALLINT/BOOL etc.)
    to a shared key. Unknown types pass through uppercased so they still
    compare equal to themselves.
    """
    s = raw.strip().upper()
    # strip "NOT NULL", "NULL", "UNSIGNED", "ZEROFILL" suffixes
    s = re.sub(r'\bUNSIGNED\b', '', s)
    s = re.sub(r'\bZEROFILL\b', '', s)
    s = re.sub(r'\bNOT\s+NULL\b', '', s)
    s = re.sub(r'\bNULL\b', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    # strip length/precision: VARCHAR(255) -> VARCHAR, DECIMAL(10,2) -> DECIMAL
    m = re.match(r'^([A-Z][A-Z0-9_ ]*?)\s*\(.*\)\s*$', s)
    if m:
        s = m.group(1).strip()
    # collapse interior whitespace remnants
    s = re.sub(r'\s+', ' ', s).strip()
    return _TYPE_GROUPS.get(s, s)


def parse_param_types(s: str) -> List[str]:
    """Parse a comma-separated SQL parameter type list into canonical keys.

    Empty / whitespace-only input → empty list. Each element is run
    through `normalize_sql_type`.
    """
    if not s or not s.strip():
        return []
    return [normalize_sql_type(t) for t in s.split(',')]


def match_kind(declared: str, requested: str) -> MatchKind:
    """Classify a single (declared, requested) type pair."""
    if declared == requested:
        return MatchKind.EXACT
    if declared in _NUMERIC_GROUPS and requested in _NUMERIC_GROUPS:
        return MatchKind.COMPATIBLE
    if declared in _STRING_GROUPS and requested in _STRING_GROUPS:
        return MatchKind.COMPATIBLE
    return MatchKind.NONE


def signature_key(param_types: List[str]) -> str:
    """Canonical key for duplicate-registration detection.

    Two variants whose `param_types` produce the same key are considered
    duplicates per SingleStoreDB's rules (e.g. CHAR(10) vs CHAR(20),
    BOOL vs TINYINT both collapse to the same key).
    """
    return ','.join(param_types)


def resolve(
    name: str,
    variants: List[Dict[str, Any]],
    requested_param_types: Optional[List[str]],
) -> Dict[str, Any]:
    """Pick the best-matching variant for a call.

    Args:
        name: Function name (for error messages only).
        variants: Candidate variants under this name. Each must carry a
            `param_sql_types` field (list of normalized SQL type keys).
        requested_param_types: Normalized SQL types from the caller, or
            None if no type info was supplied (v1 path).

    Returns the chosen variant dict.
    Raises ValueError on no-match or ambiguous-match.
    """
    if not variants:
        raise ValueError(f'unknown function: {name}')

    # No type info: only unambiguous when exactly one variant exists.
    if requested_param_types is None:
        if len(variants) == 1:
            return variants[0]
        raise ValueError(
            f"function '{name}' has {len(variants)} overloads but no "
            f'parameter type list was supplied; cannot dispatch',
        )

    # Step 1: filter by arg count.
    by_count = [
        v for v in variants
        if len(v['param_sql_types']) == len(requested_param_types)
    ]
    if not by_count:
        raise ValueError(
            f"no overload of '{name}' matches "
            f'{len(requested_param_types)} arguments',
        )

    # Step 2: filter by per-arg match (exact OR compatible). Even a single
    # arg-count survivor must still pass type-compatibility — a one-arg
    # BIGINT variant should not accept a VARCHAR call.
    scored: List[Tuple[int, Dict[str, Any]]] = []  # (exact_count, variant)
    for v in by_count:
        kinds = [
            match_kind(d, r)
            for d, r in zip(v['param_sql_types'], requested_param_types)
        ]
        if any(k is MatchKind.NONE for k in kinds):
            continue
        exact_count = sum(1 for k in kinds if k is MatchKind.EXACT)
        scored.append((exact_count, v))

    if not scored:
        raise ValueError(
            f"no overload of '{name}' matches argument types "
            f"({', '.join(requested_param_types)})",
        )
    if len(scored) == 1:
        return scored[0][1]

    # Step 3/4: prefer most exact matches; tie → ambiguous.
    max_exact = max(s[0] for s in scored)
    best = [v for c, v in scored if c == max_exact]
    if len(best) > 1:
        sigs = ', '.join(
            f"({', '.join(v['param_sql_types'])})" for v in best
        )
        raise ValueError(
            f"ambiguous call to '{name}': {len(best)} overloads match "
            f'equally well: {sigs}',
        )
    return best[0]
