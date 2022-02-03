from __future__ import annotations

import ibis.expr.datatypes as dt
import sqlalchemy.dialects.mysql as singlestore
from ibis.backends.base.sql.alchemy import AlchemyCompiler
from ibis.backends.base.sql.alchemy import AlchemyExprTranslator

from .registry import operation_registry


class SingleStoreExprTranslator(AlchemyExprTranslator):
    _registry = operation_registry
    _rewrites = AlchemyExprTranslator._rewrites.copy()
    _type_map = AlchemyExprTranslator._type_map.copy()
    _type_map.update(
        {
            dt.Boolean: singlestore.BOOLEAN,
            dt.Int8: singlestore.TINYINT,
            dt.Int32: singlestore.INTEGER,
            dt.Int64: singlestore.BIGINT,
            dt.Double: singlestore.DOUBLE,
            dt.Float: singlestore.FLOAT,
            dt.String: singlestore.VARCHAR,
        },
    )


rewrites = SingleStoreExprTranslator.rewrites


class SingleStoreCompiler(AlchemyCompiler):
    translator_class = SingleStoreExprTranslator
