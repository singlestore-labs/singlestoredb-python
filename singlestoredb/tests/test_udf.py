#!/usr/bin/env python
# type: ignore
"""SingleStoreDB UDF testing."""
import datetime
import re
import unittest
from typing import List
from typing import Optional
from typing import Tuple
from typing import TypeVar
from typing import Union

import numpy as np

from ..functions import dtypes as dt
from ..functions import signature as sig
from ..functions import udf


A = TypeVar('A', bytearray, bytes, None)
B = TypeVar('B', int, float, np.int64, np.int32, np.uint16)
C = TypeVar('C', B, np.int8)
D = TypeVar('D', bound=str)
E = Optional[List[Optional[Union[float, int]]]]


def to_sql(x):
    out = sig.signature_to_sql(sig.get_signature(x))
    out = re.sub(r'^CREATE EXTERNAL FUNCTION ', r'', out)
    out = re.sub(r' AS REMOTE SERVICE.+$', r'', out)
    return out


class TestUDF(unittest.TestCase):

    def test_return_annotations(self):

        # No annotations
        def foo(): ...
        with self.assertRaises(TypeError):
            to_sql(foo)

        # NULL return value
        def foo() -> None: ...
        assert to_sql(foo) == '`foo`() RETURNS NULL'

        # Simple return value
        def foo() -> int: ...
        assert to_sql(foo) == '`foo`() RETURNS BIGINT NOT NULL'

        # Simple return value
        def foo() -> np.int8: ...
        assert to_sql(foo) == '`foo`() RETURNS TINYINT NOT NULL'

        # Optional return value
        def foo() -> Optional[int]: ...
        assert to_sql(foo) == '`foo`() RETURNS BIGINT NULL'

        # Optional return value
        def foo() -> Union[int, None]: ...
        assert to_sql(foo) == '`foo`() RETURNS BIGINT NULL'

        # Optional return value with multiple types
        def foo() -> Union[int, float, None]: ...
        assert to_sql(foo) == '`foo`() RETURNS DOUBLE NULL'

        # Optional return value with custom type
        def foo() -> Optional[B]: ...
        assert to_sql(foo) == '`foo`() RETURNS DOUBLE NULL'

        # Optional return value with nested custom type
        def foo() -> Optional[C]: ...
        assert to_sql(foo) == '`foo`() RETURNS DOUBLE NULL'

        # Optional return value with collection type
        def foo() -> Optional[List[str]]: ...
        assert to_sql(foo) == '`foo`() RETURNS ARRAY(TEXT NOT NULL) NULL'

        # Optional return value with nested collection type
        def foo() -> Optional[List[List[str]]]: ...
        assert to_sql(foo) == '`foo`() RETURNS ARRAY(ARRAY(TEXT NOT NULL) NOT NULL) NULL'

        # Optional return value with collection type with nulls
        def foo() -> Optional[List[Optional[str]]]: ...
        assert to_sql(foo) == '`foo`() RETURNS ARRAY(TEXT NULL) NULL'

        # Custom type with bound
        def foo() -> D: ...
        assert to_sql(foo) == '`foo`() RETURNS TEXT NOT NULL'

        # Return value with custom collection type with nulls
        def foo() -> E: ...
        assert to_sql(foo) == '`foo`() RETURNS ARRAY(DOUBLE NULL) NULL'

        # Incompatible types
        def foo() -> Union[int, str]: ...
        with self.assertRaises(TypeError):
            to_sql(foo)

        # Tuple
        def foo() -> Tuple[int, float, str]: ...
        assert to_sql(foo) == '`foo`() RETURNS RECORD(a BIGINT NOT NULL, ' \
            'b DOUBLE NOT NULL, ' \
            'c TEXT NOT NULL) NOT NULL'

        # Optional tuple
        def foo() -> Optional[Tuple[int, float, str]]: ...
        assert to_sql(foo) == '`foo`() RETURNS RECORD(a BIGINT NOT NULL, ' \
            'b DOUBLE NOT NULL, ' \
            'c TEXT NOT NULL) NULL'

        # Optional tuple with optional element
        def foo() -> Optional[Tuple[int, float, Optional[str]]]: ...
        assert to_sql(foo) == '`foo`() RETURNS RECORD(a BIGINT NOT NULL, ' \
            'b DOUBLE NOT NULL, ' \
            'c TEXT NULL) NULL'

        # Optional tuple with optional union element
        def foo() -> Optional[Tuple[int, Optional[Union[float, int]], str]]: ...
        assert to_sql(foo) == '`foo`() RETURNS RECORD(a BIGINT NOT NULL, ' \
            'b DOUBLE NULL, ' \
            'c TEXT NOT NULL) NULL'

        # Unknown type
        def foo() -> set: ...
        with self.assertRaises(TypeError) as exc:
            to_sql(foo)
        assert 'unsupported type annotation' in str(exc.exception)

    def test_parameter_annotations(self):

        # No annotations
        def foo(x) -> None: ...
        with self.assertRaises(TypeError):
            to_sql(foo)

        # Simple parameter
        def foo(x: int) -> None: ...
        assert to_sql(foo) == '`foo`(`x` BIGINT NOT NULL) RETURNS NULL'

        # Optional parameter
        def foo(x: Optional[int]) -> None: ...
        assert to_sql(foo) == '`foo`(`x` BIGINT NULL) RETURNS NULL'

        # Optional parameter
        def foo(x: Union[int, None]) -> None: ...
        assert to_sql(foo) == '`foo`(`x` BIGINT NULL) RETURNS NULL'

        # Optional multiple parameter types
        def foo(x: Union[int, float, None]) -> None: ...
        assert to_sql(foo) == '`foo`(`x` DOUBLE NULL) RETURNS NULL'

        # Optional parameter with custom type
        def foo(x: Optional[B]) -> None: ...
        assert to_sql(foo) == '`foo`(`x` DOUBLE NULL) RETURNS NULL'

        # Optional parameter with nested custom type
        def foo(x: Optional[C]) -> None: ...
        assert to_sql(foo) == '`foo`(`x` DOUBLE NULL) RETURNS NULL'

        # Optional parameter with collection type
        def foo(x: Optional[List[str]]) -> None: ...
        assert to_sql(foo) == '`foo`(`x` ARRAY(TEXT NOT NULL) NULL) RETURNS NULL'

        # Optional parameter with nested collection type
        def foo(x: Optional[List[List[str]]]) -> None: ...
        assert to_sql(foo) == '`foo`(`x` ARRAY(ARRAY(TEXT NOT NULL) NOT NULL) NULL) ' \
            'RETURNS NULL'

        # Optional parameter with collection type with nulls
        def foo(x: Optional[List[Optional[str]]]) -> None: ...
        assert to_sql(foo) == '`foo`(`x` ARRAY(TEXT NULL) NULL) RETURNS NULL'

        # Custom type with bound
        def foo(x: D) -> None: ...
        assert to_sql(foo) == '`foo`(`x` TEXT NOT NULL) RETURNS NULL'

        # Incompatible types
        def foo(x: Union[int, str]) -> None: ...
        with self.assertRaises(TypeError):
            to_sql(foo)

        # Tuple
        def foo(x: Tuple[int, float, str]) -> None: ...
        assert to_sql(foo) == '`foo`(`x` RECORD(a BIGINT NOT NULL, ' \
            'b DOUBLE NOT NULL, ' \
            'c TEXT NOT NULL) NOT NULL) RETURNS NULL'

        # Optional tuple with optional element
        def foo(x: Optional[Tuple[int, float, Optional[str]]]) -> None: ...
        assert to_sql(foo) == '`foo`(`x` RECORD(a BIGINT NOT NULL, ' \
            'b DOUBLE NOT NULL, ' \
            'c TEXT NULL) NULL) RETURNS NULL'

        # Optional tuple with optional union element
        def foo(x: Optional[Tuple[int, Optional[Union[float, int]], str]]) -> None: ...
        assert to_sql(foo) == '`foo`(`x` RECORD(a BIGINT NOT NULL, ' \
            'b DOUBLE NULL, ' \
            'c TEXT NOT NULL) NULL) RETURNS NULL'

        # Unknown type
        def foo(x: set) -> None: ...
        with self.assertRaises(TypeError) as exc:
            to_sql(foo)
        assert 'unsupported type annotation' in str(exc.exception)

    def test_datetimes(self):

        # Datetime
        def foo(x: datetime.datetime) -> None: ...
        assert to_sql(foo) == '`foo`(`x` DATETIME NOT NULL) RETURNS NULL'

        # Date
        def foo(x: datetime.date) -> None: ...
        assert to_sql(foo) == '`foo`(`x` DATE NOT NULL) RETURNS NULL'

        # Time
        def foo(x: datetime.timedelta) -> None: ...
        assert to_sql(foo) == '`foo`(`x` TIME NOT NULL) RETURNS NULL'

        # Datetime + Date
        def foo(x: Union[datetime.datetime, datetime.date]) -> None: ...
        with self.assertRaises(TypeError):
            to_sql(foo)

    def test_numerics(self):
        #
        # Ints
        #
        def foo(x: int) -> None: ...
        assert to_sql(foo) == '`foo`(`x` BIGINT NOT NULL) RETURNS NULL'

        def foo(x: np.int8) -> None: ...
        assert to_sql(foo) == '`foo`(`x` TINYINT NOT NULL) RETURNS NULL'

        def foo(x: np.int16) -> None: ...
        assert to_sql(foo) == '`foo`(`x` SMALLINT NOT NULL) RETURNS NULL'

        def foo(x: np.int32) -> None: ...
        assert to_sql(foo) == '`foo`(`x` INT NOT NULL) RETURNS NULL'

        def foo(x: np.int64) -> None: ...
        assert to_sql(foo) == '`foo`(`x` BIGINT NOT NULL) RETURNS NULL'

        #
        # Unsigned ints
        #
        def foo(x: np.uint8) -> None: ...
        assert to_sql(foo) == '`foo`(`x` TINYINT UNSIGNED NOT NULL) RETURNS NULL'

        def foo(x: np.uint16) -> None: ...
        assert to_sql(foo) == '`foo`(`x` SMALLINT UNSIGNED NOT NULL) RETURNS NULL'

        def foo(x: np.uint32) -> None: ...
        assert to_sql(foo) == '`foo`(`x` INT UNSIGNED NOT NULL) RETURNS NULL'

        def foo(x: np.uint64) -> None: ...
        assert to_sql(foo) == '`foo`(`x` BIGINT UNSIGNED NOT NULL) RETURNS NULL'

        #
        # Floats
        #
        def foo(x: float) -> None: ...
        assert to_sql(foo) == '`foo`(`x` DOUBLE NOT NULL) RETURNS NULL'

        def foo(x: np.float32) -> None: ...
        assert to_sql(foo) == '`foo`(`x` FLOAT NOT NULL) RETURNS NULL'

        def foo(x: np.float64) -> None: ...
        assert to_sql(foo) == '`foo`(`x` DOUBLE NOT NULL) RETURNS NULL'

        #
        # Type collapsing
        #
        def foo(x: Union[np.int8, np.int16]) -> None: ...
        assert to_sql(foo) == '`foo`(`x` SMALLINT NOT NULL) RETURNS NULL'

        def foo(x: Union[np.int64, np.double]) -> None: ...
        assert to_sql(foo) == '`foo`(`x` DOUBLE NOT NULL) RETURNS NULL'

        def foo(x: Union[int, float]) -> None: ...
        assert to_sql(foo) == '`foo`(`x` DOUBLE NOT NULL) RETURNS NULL'

    def test_positional_and_keyword_parameters(self):
        # Keyword only
        def foo(x: int = 100) -> None: ...
        assert to_sql(foo) == '`foo`(`x` BIGINT NOT NULL DEFAULT 100) RETURNS NULL'

        # Multiple keywords
        def foo(x: int = 100, y: float = 3.14) -> None: ...
        assert to_sql(foo) == '`foo`(`x` BIGINT NOT NULL DEFAULT 100, ' \
            '`y` DOUBLE NOT NULL DEFAULT 3.14e0) RETURNS NULL'

        # Keywords and positional
        def foo(a: str, b: str, x: int = 100, y: float = 3.14) -> None: ...
        assert to_sql(foo) == '`foo`(`a` TEXT NOT NULL, ' \
            '`b` TEXT NOT NULL, ' \
            '`x` BIGINT NOT NULL DEFAULT 100, ' \
            '`y` DOUBLE NOT NULL DEFAULT 3.14e0) RETURNS NULL'

        # Variable positional
        def foo(*args: int) -> None: ...
        with self.assertRaises(TypeError):
            to_sql(foo)

        # Variable keywords
        def foo(x: int = 100, **kwargs: float) -> None: ...
        with self.assertRaises(TypeError):
            to_sql(foo)

    def test_udf(self):

        # No parameters
        @udf
        def foo(x: int) -> int: ...
        assert to_sql(foo) == '`foo`(`x` BIGINT NOT NULL) RETURNS BIGINT NOT NULL'

        # No parameters
        @udf()
        def foo(x: int) -> int: ...
        assert to_sql(foo) == '`foo`(`x` BIGINT NOT NULL) RETURNS BIGINT NOT NULL'

        # Override return value with callable
        @udf(returns=dt.SMALLINT)
        def foo(x: int) -> int: ...
        assert to_sql(foo) == '`foo`(`x` BIGINT NOT NULL) RETURNS SMALLINT NULL'

        # Override return value with string
        @udf(returns=dt.SMALLINT(nullable=False))
        def foo(x: int) -> int: ...
        assert to_sql(foo) == '`foo`(`x` BIGINT NOT NULL) RETURNS SMALLINT NOT NULL'

        # Override multiple params with one type
        @udf(args=dt.SMALLINT(nullable=False))
        def foo(x: int, y: float, z: np.int8) -> int: ...
        assert to_sql(foo) == '`foo`(`x` SMALLINT NOT NULL, ' \
            '`y` SMALLINT NOT NULL, ' \
            '`z` SMALLINT NOT NULL) RETURNS BIGINT NOT NULL'

        # Override with list
        @udf(args=[dt.SMALLINT, dt.FLOAT, dt.CHAR(30)])
        def foo(x: int, y: float, z: str) -> int: ...
        assert to_sql(foo) == '`foo`(`x` SMALLINT NULL, ' \
            '`y` FLOAT NULL, ' \
            '`z` CHAR(30) NULL) RETURNS BIGINT NOT NULL'

        # Override with too short of a list
        @udf(args=[dt.SMALLINT, dt.FLOAT])
        def foo(x: int, y: float, z: str) -> int: ...
        with self.assertRaises(TypeError):
            to_sql(foo)

        # Override with too long of a list
        @udf(args=[dt.SMALLINT, dt.FLOAT, dt.CHAR(30), dt.TEXT])
        def foo(x: int, y: float, z: str) -> int: ...
        with self.assertRaises(TypeError):
            to_sql(foo)

        # Override with list
        @udf(args=[dt.SMALLINT, dt.FLOAT, dt.CHAR(30)])
        def foo(x: int, y: float, z: str) -> int: ...
        assert to_sql(foo) == '`foo`(`x` SMALLINT NULL, ' \
            '`y` FLOAT NULL, ' \
            '`z` CHAR(30) NULL) RETURNS BIGINT NOT NULL'

        # Override with dict
        @udf(args=dict(x=dt.SMALLINT, z=dt.CHAR(30)))
        def foo(x: int, y: float, z: str) -> int: ...
        assert to_sql(foo) == '`foo`(`x` SMALLINT NULL, ' \
            '`y` DOUBLE NOT NULL, ' \
            '`z` CHAR(30) NULL) RETURNS BIGINT NOT NULL'

        # Override with empty dict
        @udf(args=dict())
        def foo(x: int, y: float, z: str) -> int: ...
        assert to_sql(foo) == '`foo`(`x` BIGINT NOT NULL, ' \
            '`y` DOUBLE NOT NULL, ' \
            '`z` TEXT NOT NULL) RETURNS BIGINT NOT NULL'

        # Override with dict with extra keys
        @udf(args=dict(bar=dt.INT))
        def foo(x: int, y: float, z: str) -> int: ...
        assert to_sql(foo) == '`foo`(`x` BIGINT NOT NULL, ' \
            '`y` DOUBLE NOT NULL, ' \
            '`z` TEXT NOT NULL) RETURNS BIGINT NOT NULL'

        # Override parameters and return value
        @udf(args=dict(x=dt.SMALLINT, z=dt.CHAR(30)), returns=dt.SMALLINT(nullable=False))
        def foo(x: int, y: float, z: str) -> int: ...
        assert to_sql(foo) == '`foo`(`x` SMALLINT NULL, ' \
            '`y` DOUBLE NOT NULL, ' \
            '`z` CHAR(30) NULL) RETURNS SMALLINT NOT NULL'

        # Override parameter with incorrect type
        with self.assertRaises(TypeError):
            @udf(args=dict(x=int))
            def foo(x: int, y: float, z: str) -> int: ...

        # Override return value with incorrect type
        with self.assertRaises(TypeError):
            @udf(returns=int)
            def foo(x: int, y: float, z: str) -> int: ...

        # Change function name
        @udf(name='hello_world')
        def foo(x: int) -> int: ...
        assert to_sql(foo) == '`hello_world`(`x` BIGINT NOT NULL) ' \
                              'RETURNS BIGINT NOT NULL'

        @udf(name='hello`_`world')
        def foo(x: int) -> int: ...
        assert to_sql(foo) == '`hello``_``world`(`x` BIGINT NOT NULL) ' \
                              'RETURNS BIGINT NOT NULL'

        # Add database name
        @udf(database='mydb')
        def foo(x: int) -> int: ...
        assert to_sql(foo) == '`mydb`.`foo`(`x` BIGINT NOT NULL) ' \
                              'RETURNS BIGINT NOT NULL'

        @udf(database='my`db')
        def foo(x: int) -> int: ...
        assert to_sql(foo) == '`my``db`.`foo`(`x` BIGINT NOT NULL) ' \
                              'RETURNS BIGINT NOT NULL'

    def test_dtypes(self):
        assert dt.BOOL() == 'BOOL NULL'
        assert dt.BOOL(nullable=False) == 'BOOL NOT NULL'
        assert dt.BOOL(default=False) == 'BOOL NULL DEFAULT 0'
        assert dt.BOOL(default=True) == 'BOOL NULL DEFAULT 1'
        assert dt.BOOL(default='a') == 'BOOL NULL DEFAULT 1'

        assert dt.BOOLEAN() == 'BOOLEAN NULL'
        assert dt.BOOLEAN(nullable=False) == 'BOOLEAN NOT NULL'
        assert dt.BOOLEAN(default=False) == 'BOOLEAN NULL DEFAULT 0'
        assert dt.BOOLEAN(default=True) == 'BOOLEAN NULL DEFAULT 1'
        assert dt.BOOLEAN(default='a') == 'BOOLEAN NULL DEFAULT 1'

        assert dt.BIT() == 'BIT NULL'
        assert dt.BIT(nullable=False) == 'BIT NOT NULL'
        assert dt.BIT(default=100) == 'BIT NULL DEFAULT 100'

        assert dt.TINYINT() == 'TINYINT NULL'
        assert dt.TINYINT(5) == 'TINYINT(5) NULL'
        assert dt.TINYINT(nullable=False) == 'TINYINT NOT NULL'
        assert dt.TINYINT(default=100) == 'TINYINT NULL DEFAULT 100'
        assert dt.TINYINT(unsigned=True, default=100) == \
            'TINYINT UNSIGNED NULL DEFAULT 100'

        assert dt.TINYINT_UNSIGNED() == 'TINYINT UNSIGNED NULL'
        assert dt.TINYINT_UNSIGNED(5) == 'TINYINT(5) UNSIGNED NULL'
        assert dt.TINYINT_UNSIGNED(nullable=False) == 'TINYINT UNSIGNED NOT NULL'
        assert dt.TINYINT_UNSIGNED(default=100) == 'TINYINT UNSIGNED NULL DEFAULT 100'

        assert dt.SMALLINT() == 'SMALLINT NULL'
        assert dt.SMALLINT(5) == 'SMALLINT(5) NULL'
        assert dt.SMALLINT(nullable=False) == 'SMALLINT NOT NULL'
        assert dt.SMALLINT(default=100) == 'SMALLINT NULL DEFAULT 100'
        assert dt.SMALLINT(unsigned=True, default=100) == \
            'SMALLINT UNSIGNED NULL DEFAULT 100'

        assert dt.SMALLINT_UNSIGNED() == 'SMALLINT UNSIGNED NULL'
        assert dt.SMALLINT_UNSIGNED(5) == 'SMALLINT(5) UNSIGNED NULL'
        assert dt.SMALLINT_UNSIGNED(nullable=False) == 'SMALLINT UNSIGNED NOT NULL'
        assert dt.SMALLINT_UNSIGNED(default=100) == \
            'SMALLINT UNSIGNED NULL DEFAULT 100'

        assert dt.MEDIUMINT() == 'MEDIUMINT NULL'
        assert dt.MEDIUMINT(5) == 'MEDIUMINT(5) NULL'
        assert dt.MEDIUMINT(nullable=False) == 'MEDIUMINT NOT NULL'
        assert dt.MEDIUMINT(default=100) == 'MEDIUMINT NULL DEFAULT 100'
        assert dt.MEDIUMINT(unsigned=True, default=100) == \
            'MEDIUMINT UNSIGNED NULL DEFAULT 100'

        assert dt.MEDIUMINT_UNSIGNED() == 'MEDIUMINT UNSIGNED NULL'
        assert dt.MEDIUMINT_UNSIGNED(5) == 'MEDIUMINT(5) UNSIGNED NULL'
        assert dt.MEDIUMINT_UNSIGNED(nullable=False) == 'MEDIUMINT UNSIGNED NOT NULL'
        assert dt.MEDIUMINT_UNSIGNED(default=100) == \
            'MEDIUMINT UNSIGNED NULL DEFAULT 100'

        assert dt.INT() == 'INT NULL'
        assert dt.INT(5) == 'INT(5) NULL'
        assert dt.INT(nullable=False) == 'INT NOT NULL'
        assert dt.INT(default=100) == 'INT NULL DEFAULT 100'
        assert dt.INT(unsigned=True, default=100) == \
            'INT UNSIGNED NULL DEFAULT 100'

        assert dt.INT_UNSIGNED() == 'INT UNSIGNED NULL'
        assert dt.INT_UNSIGNED(5) == 'INT(5) UNSIGNED NULL'
        assert dt.INT_UNSIGNED(nullable=False) == 'INT UNSIGNED NOT NULL'
        assert dt.INT_UNSIGNED(default=100) == \
            'INT UNSIGNED NULL DEFAULT 100'

        assert dt.INTEGER() == 'INTEGER NULL'
        assert dt.INTEGER(5) == 'INTEGER(5) NULL'
        assert dt.INTEGER(nullable=False) == 'INTEGER NOT NULL'
        assert dt.INTEGER(default=100) == 'INTEGER NULL DEFAULT 100'
        assert dt.INTEGER(unsigned=True, default=100) == \
            'INTEGER UNSIGNED NULL DEFAULT 100'

        assert dt.INTEGER_UNSIGNED() == 'INTEGER UNSIGNED NULL'
        assert dt.INTEGER_UNSIGNED(5) == 'INTEGER(5) UNSIGNED NULL'
        assert dt.INTEGER_UNSIGNED(nullable=False) == 'INTEGER UNSIGNED NOT NULL'
        assert dt.INTEGER_UNSIGNED(default=100) == \
            'INTEGER UNSIGNED NULL DEFAULT 100'

        assert dt.BIGINT() == 'BIGINT NULL'
        assert dt.BIGINT(5) == 'BIGINT(5) NULL'
        assert dt.BIGINT(nullable=False) == 'BIGINT NOT NULL'
        assert dt.BIGINT(default=100) == 'BIGINT NULL DEFAULT 100'
        assert dt.BIGINT(unsigned=True, default=100) == \
            'BIGINT UNSIGNED NULL DEFAULT 100'

        assert dt.BIGINT_UNSIGNED() == 'BIGINT UNSIGNED NULL'
        assert dt.BIGINT_UNSIGNED(5) == 'BIGINT(5) UNSIGNED NULL'
        assert dt.BIGINT_UNSIGNED(nullable=False) == 'BIGINT UNSIGNED NOT NULL'
        assert dt.BIGINT_UNSIGNED(default=100) == \
            'BIGINT UNSIGNED NULL DEFAULT 100'

        assert dt.BIGINT() == 'BIGINT NULL'
        assert dt.BIGINT(5) == 'BIGINT(5) NULL'
        assert dt.BIGINT(nullable=False) == 'BIGINT NOT NULL'
        assert dt.BIGINT(default=100) == 'BIGINT NULL DEFAULT 100'
        assert dt.BIGINT(unsigned=True, default=100) == \
            'BIGINT UNSIGNED NULL DEFAULT 100'

        assert dt.FLOAT() == 'FLOAT NULL'
        assert dt.FLOAT(5) == 'FLOAT(5) NULL'
        assert dt.FLOAT(nullable=False) == 'FLOAT NOT NULL'
        assert dt.FLOAT(default=1.234) == 'FLOAT NULL DEFAULT 1.234e0'

        assert dt.DOUBLE() == 'DOUBLE NULL'
        assert dt.DOUBLE(5) == 'DOUBLE(5) NULL'
        assert dt.DOUBLE(nullable=False) == 'DOUBLE NOT NULL'
        assert dt.DOUBLE(default=1.234) == 'DOUBLE NULL DEFAULT 1.234e0'

        assert dt.REAL() == 'REAL NULL'
        assert dt.REAL(5) == 'REAL(5) NULL'
        assert dt.REAL(nullable=False) == 'REAL NOT NULL'
        assert dt.REAL(default=1.234) == 'REAL NULL DEFAULT 1.234e0'

        with self.assertRaises(TypeError):
            dt.DECIMAL()
        with self.assertRaises(TypeError):
            dt.DECIMAL(5)
        assert dt.DECIMAL(10, 5) == 'DECIMAL(10, 5) NULL'
        assert dt.DECIMAL(10, 5, nullable=False) == 'DECIMAL(10, 5) NOT NULL'
        assert dt.DECIMAL(10, 5, default=1.234) == \
            'DECIMAL(10, 5) NULL DEFAULT 1.234e0'

        with self.assertRaises(TypeError):
            dt.DEC()
        with self.assertRaises(TypeError):
            dt.DEC(5)
        assert dt.DEC(10, 5) == 'DEC(10, 5) NULL'
        assert dt.DEC(10, 5, nullable=False) == 'DEC(10, 5) NOT NULL'
        assert dt.DEC(10, 5, default=1.234) == \
            'DEC(10, 5) NULL DEFAULT 1.234e0'

        with self.assertRaises(TypeError):
            dt.FIXED()
        with self.assertRaises(TypeError):
            dt.FIXED(5)
        assert dt.FIXED(10, 5) == 'FIXED(10, 5) NULL'
        assert dt.FIXED(10, 5, nullable=False) == 'FIXED(10, 5) NOT NULL'
        assert dt.FIXED(10, 5, default=1.234) == \
            'FIXED(10, 5) NULL DEFAULT 1.234e0'

        with self.assertRaises(TypeError):
            dt.NUMERIC()
        with self.assertRaises(TypeError):
            dt.NUMERIC(5)
        assert dt.NUMERIC(10, 5) == 'NUMERIC(10, 5) NULL'
        assert dt.NUMERIC(10, 5, nullable=False) == 'NUMERIC(10, 5) NOT NULL'
        assert dt.NUMERIC(10, 5, default=1.234) == \
            'NUMERIC(10, 5) NULL DEFAULT 1.234e0'

        assert dt.DATE() == 'DATE NULL'
        assert dt.DATE(nullable=False) == 'DATE NOT NULL'
        assert dt.DATE(default=datetime.date(2020, 1, 2)) == \
            "DATE NULL DEFAULT '2020-01-02'"

        assert dt.TIME() == 'TIME NULL'
        assert dt.TIME(6) == 'TIME(6) NULL'
        assert dt.TIME(nullable=False) == 'TIME NOT NULL'
        assert dt.TIME(default=datetime.timedelta(seconds=1000)) == \
            "TIME NULL DEFAULT '00:16:40'"

        assert dt.DATETIME() == 'DATETIME NULL'
        assert dt.DATETIME(6) == 'DATETIME(6) NULL'
        assert dt.DATETIME(nullable=False) == 'DATETIME NOT NULL'
        assert dt.DATETIME(default=datetime.datetime(2020, 1, 2, 3, 4, 5)) == \
            "DATETIME NULL DEFAULT '2020-01-02 03:04:05'"

        assert dt.TIMESTAMP() == 'TIMESTAMP NULL'
        assert dt.TIMESTAMP(6) == 'TIMESTAMP(6) NULL'
        assert dt.TIMESTAMP(nullable=False) == 'TIMESTAMP NOT NULL'
        assert dt.TIMESTAMP(default=datetime.datetime(2020, 1, 2, 3, 4, 5)) == \
            "TIMESTAMP NULL DEFAULT '2020-01-02 03:04:05'"

        assert dt.YEAR() == 'YEAR NULL'
        assert dt.YEAR(nullable=False) == 'YEAR NOT NULL'
        assert dt.YEAR(default=1961) == 'YEAR NULL DEFAULT 1961'

        assert dt.CHAR() == 'CHAR NULL'
        assert dt.CHAR(10) == 'CHAR(10) NULL'
        assert dt.CHAR(charset=dt.utf8, collate=dt.utf8_bin) == \
            'CHAR CHARACTER SET utf8 COLLATE utf8_bin NULL'
        assert dt.CHAR(nullable=False) == 'CHAR NOT NULL'
        assert dt.CHAR(default='hi') == "CHAR NULL DEFAULT 'hi'"

        assert dt.VARCHAR() == 'VARCHAR NULL'
        assert dt.VARCHAR(10) == 'VARCHAR(10) NULL'
        assert dt.VARCHAR(charset=dt.utf8, collate=dt.utf8_bin) == \
            'VARCHAR CHARACTER SET utf8 COLLATE utf8_bin NULL'
        assert dt.VARCHAR(nullable=False) == 'VARCHAR NOT NULL'
        assert dt.VARCHAR(default='hi') == "VARCHAR NULL DEFAULT 'hi'"

        assert dt.LONGTEXT() == 'LONGTEXT NULL'
        assert dt.LONGTEXT(10) == 'LONGTEXT(10) NULL'
        assert dt.LONGTEXT(charset=dt.utf8, collate=dt.utf8_bin) == \
            'LONGTEXT CHARACTER SET utf8 COLLATE utf8_bin NULL'
        assert dt.LONGTEXT(nullable=False) == 'LONGTEXT NOT NULL'
        assert dt.LONGTEXT(default='hi') == "LONGTEXT NULL DEFAULT 'hi'"

        assert dt.MEDIUMTEXT() == 'MEDIUMTEXT NULL'
        assert dt.MEDIUMTEXT(10) == 'MEDIUMTEXT(10) NULL'
        assert dt.MEDIUMTEXT(charset=dt.utf8, collate=dt.utf8_bin) == \
            'MEDIUMTEXT CHARACTER SET utf8 COLLATE utf8_bin NULL'
        assert dt.MEDIUMTEXT(nullable=False) == 'MEDIUMTEXT NOT NULL'
        assert dt.MEDIUMTEXT(default='hi') == "MEDIUMTEXT NULL DEFAULT 'hi'"

        assert dt.TEXT() == 'TEXT NULL'
        assert dt.TEXT(10) == 'TEXT(10) NULL'
        assert dt.TEXT(charset=dt.utf8, collate=dt.utf8_bin) == \
            'TEXT CHARACTER SET utf8 COLLATE utf8_bin NULL'
        assert dt.TEXT(nullable=False) == 'TEXT NOT NULL'
        assert dt.TEXT(default='hi') == "TEXT NULL DEFAULT 'hi'"

        assert dt.TINYTEXT() == 'TINYTEXT NULL'
        assert dt.TINYTEXT(10) == 'TINYTEXT(10) NULL'
        assert dt.TINYTEXT(charset=dt.utf8, collate=dt.utf8_bin) == \
            'TINYTEXT CHARACTER SET utf8 COLLATE utf8_bin NULL'
        assert dt.TINYTEXT(nullable=False) == 'TINYTEXT NOT NULL'
        assert dt.TINYTEXT(default='hi') == "TINYTEXT NULL DEFAULT 'hi'"

        assert dt.BINARY() == 'BINARY NULL'
        assert dt.BINARY(10) == 'BINARY(10) NULL'
        assert dt.BINARY(collate=dt.utf8_bin) == \
            'BINARY COLLATE utf8_bin NULL'
        assert dt.BINARY(nullable=False) == 'BINARY NOT NULL'
        assert dt.BINARY(default='hi') == "BINARY NULL DEFAULT 'hi'"

        assert dt.VARBINARY() == 'VARBINARY NULL'
        assert dt.VARBINARY(10) == 'VARBINARY(10) NULL'
        assert dt.VARBINARY(collate=dt.utf8_bin) == \
            'VARBINARY COLLATE utf8_bin NULL'
        assert dt.VARBINARY(nullable=False) == 'VARBINARY NOT NULL'
        assert dt.VARBINARY(default='hi') == "VARBINARY NULL DEFAULT 'hi'"

        assert dt.BLOB() == 'BLOB NULL'
        assert dt.BLOB(10) == 'BLOB(10) NULL'
        assert dt.BLOB(collate=dt.utf8_bin) == \
            'BLOB COLLATE utf8_bin NULL'
        assert dt.BLOB(nullable=False) == 'BLOB NOT NULL'
        assert dt.BLOB(default='hi') == "BLOB NULL DEFAULT 'hi'"

        assert dt.TINYBLOB() == 'TINYBLOB NULL'
        assert dt.TINYBLOB(10) == 'TINYBLOB(10) NULL'
        assert dt.TINYBLOB(collate=dt.utf8_bin) == \
            'TINYBLOB COLLATE utf8_bin NULL'
        assert dt.TINYBLOB(nullable=False) == 'TINYBLOB NOT NULL'
        assert dt.TINYBLOB(default='hi') == "TINYBLOB NULL DEFAULT 'hi'"

        assert dt.JSON() == 'JSON NULL'
        assert dt.JSON(10) == 'JSON(10) NULL'
        assert dt.JSON(charset=dt.utf8, collate=dt.utf8_bin) == \
            'JSON CHARACTER SET utf8 COLLATE utf8_bin NULL'
        assert dt.JSON(nullable=False) == 'JSON NOT NULL'
        assert dt.JSON(default='hi') == "JSON NULL DEFAULT 'hi'"

        assert dt.GEOGRAPHYPOINT() == 'GEOGRAPHYPOINT NULL'
        assert dt.GEOGRAPHYPOINT(nullable=False) == 'GEOGRAPHYPOINT NOT NULL'
        assert dt.GEOGRAPHYPOINT(default='hi') == "GEOGRAPHYPOINT NULL DEFAULT 'hi'"

        assert dt.GEOGRAPHY() == 'GEOGRAPHY NULL'
        assert dt.GEOGRAPHY(nullable=False) == 'GEOGRAPHY NOT NULL'
        assert dt.GEOGRAPHY(default='hi') == "GEOGRAPHY NULL DEFAULT 'hi'"

        with self.assertRaises(AssertionError):
            dt.RECORD()
        assert dt.RECORD(('a', dt.INT), ('b', dt.FLOAT)) == \
            'RECORD(`a` INT NULL, `b` FLOAT NULL) NULL'
        assert dt.RECORD(('a', dt.INT), ('b', dt.FLOAT), nullable=False) == \
            'RECORD(`a` INT NULL, `b` FLOAT NULL) NOT NULL'

        assert dt.ARRAY(dt.INT) == 'ARRAY(INT NULL) NULL'
        assert dt.ARRAY(dt.INT, nullable=False) == 'ARRAY(INT NULL) NOT NULL'
