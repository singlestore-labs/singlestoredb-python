#!/usr/bin/env python
"""SingleStore data type utilities."""
from __future__ import annotations

import datetime
import decimal
import time
from typing import Dict
from typing import List
from typing import Set


Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime
Binary = bytes


def DateFromTicks(ticks: int) -> Date:
    """
    Convert ticks to a date object.

    Parameters
    ----------
    ticks : int
        Number of seconds since the epoch

    Returns
    -------
    Date

    """
    return Date(*time.localtime(ticks)[:3])


def TimeFromTicks(ticks: int) -> Time:
    """
    Convert ticks to a time object.

    Parameters
    ----------
    ticks : int
        Number of seconds since the epoch

    Returns
    -------
    Time

    """
    return Time(*time.localtime(ticks)[3:6])


def TimestampFromTicks(ticks: int) -> Timestamp:
    """
    Convert ticks to a datetime object.

    Parameters
    ----------
    ticks : int
        Number of seconds since the epoch

    Returns
    -------
    Timestamp

    """
    return Timestamp(*time.localtime(ticks)[:6])


class DBAPIType(object):
    """
    Base class for DB-API data types.

    Parameters
    ----------
    *values : int or str, optional
        Names and codes of data types

    """

    def __init__(self, *values: int | str | type):
        self.values: Set[int | str | type] = set()
        for item in values:
            if isinstance(item, DBAPIType):
                self.values.update(item.values)
            else:
                self.values.add(item)

    def __eq__(self, other: object) -> bool:
        """
        Determine if `other` object is equivalent.

        Parameters
        ----------
        other : int or str or DBIAPIType
            Object to compare to

        Returns
        -------
        bool

        """
        if other in self.values:
            return True
        return False

    def __ne__(self, other: object) -> bool:
        """
        Determine if `other` object is not equivalent.

        Parameters
        ----------
        other : int or str or DBIAPIType
            Object to compare to

        Returns
        -------
        bool

        """
        return not(self.__eq__(other))

    def __str__(self) -> str:
        """Return string representation."""
        return '<{} object [{}]>'.format(
            type(self).__name__,
            ', '.join(sorted(str(x) for x in self.values)),
        )

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)


class StringDBAPIType(DBAPIType):
    """STRING DB-API types."""


class BinaryDBAPIType(DBAPIType):
    """BINARY DB-API types."""


class NumberDBAPIType(DBAPIType):
    """NUMBER DB-API types."""


class DatetimeDBAPIType(DBAPIType):
    """DATETIME DB-API types."""


class ColumnType(object):
    """Column types and utilities."""

    # Note that the first name given will be the name returned by
    # the `get_name` method.
    DECIMAL = NumberDBAPIType(
        'DECIMAL', 'DEC', 'FIXED', 'NUMERIC', 0, decimal.Decimal,
    )
    DEC = FIXED = NUMERIC = DECIMAL
    TINY = TINYINT = NumberDBAPIType('TINY', 'TINYINT', 1)
    SHORT = SMALLINT = NumberDBAPIType('SHORT', 'SMALLINT', 2)
    LONG = INT = NumberDBAPIType('LONG', 'INT', 3)
    FLOAT = NumberDBAPIType('FLOAT', 4)
    DOUBLE = REAL = NumberDBAPIType('DOUBLE', 5, float)
    NULL = DBAPIType('NULL', 6)
    TIMESTAMP = DatetimeDBAPIType('TIMESTAMP', 7)
    LONGLONG = BIGINT = NumberDBAPIType('LONGLONG', 'BIGINT', 8, int)
    MEDIUMINT = INT24 = NumberDBAPIType('MEDIUMINT', 'INT24', 9)
    DATE = DBAPIType('DATE', 10, datetime.date)
    TIME = DBAPIType('TIME', 11, datetime.time)
    DATETIME = DatetimeDBAPIType('DATETIME', 12, datetime.datetime)
    YEAR = DBAPIType('YEAR', 13)
    NEWDATE = DBAPIType('NEWDATE', 14)
    VARCHAR = VARBINARY = StringDBAPIType(
        'VARBINARY', 'VARCHAR', 15, str, bytearray, bytes,
    )
    BIT = NumberDBAPIType('BIT', 16)
    JSON = DBAPIType('JSON', 245)
    NEWDECIMAL = NumberDBAPIType('NEWDECIMAL', 246)
    ENUM = StringDBAPIType('ENUM', 247)
    SET = DBAPIType('SET', 248)
    TINYBLOB = TINYTEXT = BinaryDBAPIType('TINYBLOB', 'TINYTEXT', 249)
    MEDIUMBLOB = MEDIUMTEXT = BinaryDBAPIType('MEDIUMBLOB', 'MEDIUMTEXT', 250)
    LONGBLOB = LONGTEXT = BinaryDBAPIType('LONGBLOB', 'LONGTEXT', 251)
    BLOB = TEXT = BinaryDBAPIType('BLOB', 'TEXT', 252)
    VARSTRING = StringDBAPIType('VARSTRING', 253)
    STRING = CHAR = BINARY = StringDBAPIType('BINARY', 'STRING', 'CHAR', 254)
    GEOMETRY = DBAPIType('GEOMETRY', 255)

    _type_name_map: Dict[str, int] = {}
    _type_code_map: Dict[int, str] = {}
    _type_type_map: Dict[type, int] = {}

    @classmethod
    def get_code(cls, name: str) -> int:
        """
        Return the numeric database type code corresponding to `name`.

        If `name` is given as an int, it is immediately returned.

        Parameters
        ----------
        name : str
            Name of the database type

        Returns
        -------
        int

        """
        if isinstance(name, int):
            return name
        if not cls._type_name_map:
            cls._update_type_maps()
        if type(name) is type:
            return cls._type_type_map[name]
        return cls._type_name_map[name.upper()]

    @classmethod
    def get_name(cls, code: int) -> str:
        """
        Return the database type name corresponding to integer value `code`.

        If `code` is given as a string, it is immediately returned.

        Parameters
        ----------
        code : int
            Integer code value of the database type

        Returns
        -------
        str

        """
        if isinstance(code, str):
            return code.upper()
        if not cls._type_code_map:
            cls._update_type_maps()
        if type(code) is type:
            code = cls._type_type_map[code]
        return cls._type_code_map[code]

    @classmethod
    def _update_type_maps(cls) -> None:
        """Update the type code and name maps."""
        for k, v in vars(cls).items():
            if not isinstance(v, DBAPIType):
                continue
            names = [x.upper() for x in v.values if isinstance(x, str)]
            codes = [x for x in v.values if isinstance(x, int)]
            types = [x for x in v.values if isinstance(x, type)]
            for code in codes:
                for name in names:
                    cls._type_name_map[name] = code
                    cls._type_code_map[code] = name
                for typ in types:
                    cls._type_type_map[typ] = code

    @classmethod
    def get_string_types(cls) -> List[str]:
        """
        Return all database types that correspond to DB-API strings.

        Returns
        -------
        list of StringDBAPIType instances

        """
        return [k for k, v in vars(cls).items() if type(v) is StringDBAPIType]

    @classmethod
    def get_binary_types(cls) -> List[str]:
        """
        Return all database types that correspond to DB-API binary objects.

        Returns
        -------
        list of BinaryDBAPIType instances

        """
        return [k for k, v in vars(cls).items() if type(v) is BinaryDBAPIType]

    @classmethod
    def get_number_types(cls) -> List[str]:
        """
        Return all database types that correspond to DB-API number objects.

        Returns
        -------
        list of NumberDBAPIType instances

        """
        return [k for k, v in vars(cls).items() if type(v) is NumberDBAPIType]

    @classmethod
    def get_datetime_types(cls) -> List[str]:
        """
        Return all database types that correspond to DB-API datetime objects.

        Returns
        -------
        list of DatetimeDBAPIType instances

        """
        return [k for k, v in vars(cls).items() if type(v) is DatetimeDBAPIType]

    @classmethod
    def get_non_dbapi_types(cls) -> List[str]:
        """
        Return all database types that do not correspond to DB-API typed objects.

        Returns
        -------
        list of DBAPIType instances

        """
        return [k for k, v in vars(cls).items() if type(v) is DBAPIType]


# DB-API type constants
STRING = DBAPIType(*ColumnType.get_string_types())
BINARY = DBAPIType(*ColumnType.get_binary_types())
NUMBER = DBAPIType(*ColumnType.get_number_types())
DATETIME = DBAPIType(*ColumnType.get_datetime_types())
ROWID = DBAPIType()
