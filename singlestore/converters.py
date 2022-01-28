#!/usr/bin/env python
'''
Data value conversion utilities

'''
from __future__ import annotations

import datetime
import decimal
import json


def int_or_none(x):
    ''' Convert value to int or None '''
    if x is None:
        return
    return int(x)


def float_or_none(x):
    ''' Convert value to float or None '''
    if x is None:
        return
    return float(x)


def decimal_or_none(x):
    ''' Convert value to decimal or None '''
    if x is None:
        return
    return decimal.Decimal(x)


def date_or_none(x):
    ''' Convert value to date or None '''
    if x is None:
        return
    return datetime.date.fromisoformat(x)


def time_or_none(x):
    ''' Convert value to time or None '''
    if x is None:
        return
    return datetime.time.fromisoformat(x)


def datetime_or_none(x):
    ''' Convert value to datetime or None '''
    if x is None:
        return
    return datetime.datetime.fromisoformat(x)


def bytes_or_none(x):
    ''' Convert value to bytes or None '''
    if x is None:
        return
    if isinstance(x, bytes):
        return x
    return bytes(x)


def string_or_none(x, encoding='utf-8'):
    ''' Convert value to string or None '''
    if x is None:
        return
    if isinstance(x, str):
        return x
    return str(x, 'utf-8')


def none(x):
    ''' Return None '''
    return


def json_or_none(x):
    ''' Convert JSON to dict or None '''
    if x is None:
        return
    return json.loads(x)


def set_or_none(x):
    ''' Convert value to set of strings or None '''
    if x is None:
        return
    return set(y.strip() for y in x.split(','))


def geometry_or_none(x):
    ''' Convert value to geometry coordinates or None '''
    if x is None:
        return
    return x


# Map of database types and conversion functions
converters = {
    'DECIMAL': decimal_or_none,
    0x00: decimal_or_none,
    'TINY': int_or_none,
    0x01: int_or_none,
    'SHORT': int_or_none,
    0x02: int_or_none,
    'LONG': int_or_none,
    0x03: int_or_none,
    'INT': int_or_none,
    'FLOAT': float_or_none,
    0x04: float_or_none,
    'DOUBLE': float_or_none,
    0x05: float_or_none,
    'NULL': none,
    0x06: none,
    'TIMESTAMP': datetime_or_none,
    0x07: datetime_or_none,
    'LONGLONG': int_or_none,
    'BIGINT': int_or_none,
    0x08: int_or_none,
    'INT32': int_or_none,
    0x09: int_or_none,
    'DATE': date_or_none,
    0x0a: date_or_none,
    'TIME': time_or_none,
    0x0b: time_or_none,
    'DATETIME': datetime_or_none,
    0x0c: datetime_or_none,
    'YEAR': int_or_none,
    0x0d: int_or_none,
    'NEWDATE': date_or_none,
    0x0e: date_or_none,
    'VARCHAR': string_or_none,
    0x0f: string_or_none,
    'BIT': int_or_none,
    0x10: int_or_none,
    'JSON': json_or_none,
    0xf5: json_or_none,
    'NEWDECIMAL': decimal_or_none,
    0xf6: decimal_or_none,
    'ENUM': string_or_none,
    0xf7: string_or_none,
    'SET': set_or_none,
    0xf8: set_or_none,
    'TINY_BLOB': bytes_or_none,
    0xf9: bytes_or_none,
    'MEDIUM_BLOB': bytes_or_none,
    0xfa: bytes_or_none,
    'LONG_BLOB': bytes_or_none,
    0xfb: bytes_or_none,
    'BLOB': bytes_or_none,
    0xfc: bytes_or_none,
    'TEXT': bytes_or_none,
    'VAR_STRING': string_or_none,
    0xfd: string_or_none,
    'STRING': string_or_none,
    0xfe: string_or_none,
    'GEOMETRY': geometry_or_none,
    0xff: geometry_or_none,
}
