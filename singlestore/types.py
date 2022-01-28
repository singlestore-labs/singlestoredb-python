#!/usr/bin/env python

''' SingleStore data type utilities '''

import datetime
import time


Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime
Binary = bytes


def DateFromTicks(ticks):
    ''' Convert ticks to a date object '''
    return Date(*time.localtime(ticks)[:3])


def TimeFromTicks(ticks):
    ''' Convert ticks to a time object '''
    return Time(*time.localtime(ticks)[3:6])


def TimestampFromTicks(ticks):
    ''' Convert ticks to a datetime object '''
    return Timestamp(*time.localtime(ticks)[:6])


class DBAPIType(object):
    ''' Base class for DB-API data types '''

    def __init__(self, *values):
        self.values = set()
        for item in values:
            if isinstance(item, DBAPIType):
                self.values.update(item.values)
            else:
                self.values.add(item)

    def __eq__(self, other):
        if other in self.values:
            return True
        return False

    def __ne__(self, other):
        return not(self.__eq__(other))

    def __str__(self):
        return "<{} object [{}]>".format(type(self).__name__,
                                         ', '.join(str(x) for x in sorted(self.values)))

    def __repr__(self):
        return str(self)


class StringDBAPIType(DBAPIType):
    ''' STRING DB-API types '''
    pass


class BinaryDBAPIType(DBAPIType):
    ''' BINARY DB-API types '''
    pass


class NumberDBAPIType(DBAPIType):
    ''' NUMBER DB-API types '''
    pass


class DatetimeDBAPIType(DBAPIType):
    ''' DATETIME DB-API types '''
    pass


class ColumnType(object):
    ''' Column types and utilities '''

    DECIMAL = NumberDBAPIType('DECIMAL', 0x00)
    TINY = NumberDBAPIType('TINY', 0x01)
    SHORT = NumberDBAPIType('SHORT', 0x02)
    LONG = NumberDBAPIType('LONG', 0x03)
    INT = NumberDBAPIType('INT', 0x03)
    FLOAT = NumberDBAPIType('FLOAT', 0x04)
    DOUBLE = NumberDBAPIType('DOUBLE', 0x05)
    NULL = DBAPIType('NULL', 0x06)
    TIMESTAMP = DatetimeDBAPIType('TIMESTAMP', 0x07)
    LONGLONG = NumberDBAPIType('LONGLONG', 0x08)
    BIGINT = NumberDBAPIType('BIGINT', 0x08)
    INT24 = NumberDBAPIType('INT24', 0x09)
    DATE = DBAPIType('DATE', 0x0a)
    TIME = DBAPIType('TIME', 0x0b)
    DATETIME = DatetimeDBAPIType('DATETIME', 0x0c)
    YEAR = DBAPIType('YEAR', 0x0d)
    NEWDATE = DBAPIType('NEWDATE', 0x0e)
    VARCHAR = StringDBAPIType('VARCHAR', 0x0f)
    BIT = NumberDBAPIType('BIT', 0x10)
    JSON = DBAPIType('JSON', 0xf5)
    NEWDECIMAL = NumberDBAPIType('NEWDECIMAL', 0xf6)
    ENUM = StringDBAPIType('ENUM', 0xf7)
    SET = DBAPIType('SET', 0xf8)
    TINY_BLOB = BinaryDBAPIType('TINY_BLOB', 0xf9)
    MEDIUM_BLOB = BinaryDBAPIType('MEDIUM_BLOB', 0xfa)
    LONG_BLOB = BinaryDBAPIType('LONG_BLOB', 0xfb)
    BLOB = BinaryDBAPIType('BLOB', 0xfc)
    TEXT = BinaryDBAPIType('TEXT', 0xfc)
    VAR_STRING = StringDBAPIType('VAR_STRING', 0xfd)
    STRING = StringDBAPIType('STRING', 0xfe)
    GEOMETRY = DBAPIType('GEOMETRY', 0xff)

    _type_name_map = {}
    _type_code_map = {}

    @classmethod
    def get_code(cls, name):
        '''
        Return the numeric database type code corresponding to `name`

        If `name` is given as an int, it is immediately returned.

        Parameters
        ----------
        name : str
            Name of the database type

        Returns
        -------
        int

        '''
        if isinstance(name, int):
            return name
        if not cls._type_name_map:
            cls._update_type_maps()
        return cls._type_name_map[name.upper()]

    @classmethod
    def get_name(cls, code):
        '''
        Return the database type name corresponding to integer value `code`

        If `code` is given as a string, it is immediately returned.

        Parameters
        ----------
        code : int
            Integer code value of the database type

        Returns
        -------
        str

        '''
        if isinstance(code, str):
            return code.upper()
        if not cls._type_code_map:
            cls._update_type_maps()
        return cls._type_code_map[code]

    @classmethod
    def _update_type_maps(cls):
        ''' Update the type code and name maps '''
        for k, v in vars(cls).items():
            if not isinstance(v, DBAPIType):
                continue
            names = [x.upper() for x in v.values if isinstance(x, str)]
            codes = [x for x in v.values if isinstance(x, int)]
            for name in names:
                for code in codes:
                    cls._type_name_map[name] = code
                    cls._type_code_map[code] = name

    @classmethod
    def get_string_types(cls):
        '''
        Return all database types that correspond to DB-API strings

        Returns
        -------
        list of StringDBAPIType instances

        '''
        return [k for k, v in vars(cls).items() if type(v) is StringDBAPIType]

    @classmethod
    def get_binary_types(cls):
        '''
        Return all database types that correspond to DB-API binary objects

        Returns
        -------
        list of BinaryDBAPIType instances

        '''
        return [k for k, v in vars(cls).items() if type(v) is BinaryDBAPIType]

    @classmethod
    def get_number_types(cls):
        '''
        Return all database types that correspond to DB-API number objects

        Returns
        -------
        list of NumberDBAPIType instances

        '''
        return [k for k, v in vars(cls).items() if type(v) is NumberDBAPIType]

    @classmethod
    def get_datetime_types(cls):
        '''
        Return all database types that correspond to DB-API datetime objects

        Returns
        -------
        list of DatetimeDBAPIType instances

        '''
        return [k for k, v in vars(cls).items() if type(v) is DatetimeDBAPIType]

    @classmethod
    def get_non_dbapi_types(cls):
        '''
        Return all database types that do not correspond to DB-API typed objects

        Returns
        -------
        list of DBAPIType instances

        '''
        return [k for k, v in vars(cls).items() if type(v) is DBAPIType]


# DB-API type constants
STRING = DBAPIType(*ColumnType.get_string_types())
BINARY = DBAPIType(*ColumnType.get_binary_types())
NUMBER = DBAPIType(*ColumnType.get_number_types())
DATETIME = DBAPIType(*ColumnType.get_datetime_types())
ROWID = DBAPIType()
