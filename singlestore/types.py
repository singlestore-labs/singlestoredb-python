
import time


def DateFromTicks(ticks):
    return Date(*time.localtime(ticks)[:3])


def TimeFromTicks(ticks):
    return Time(*time.localtime(ticks)[3:6])


def TimestampFromTicks(ticks):
    return Timestamp(*time.localtime(ticks)[:6])


class Date(object):

    def __init__(self, year, month, day):
        self.year = year
        self.month = month
        self.day = day


class Time(object):

    def __init__(self, hour, minute, second):
        self.hour = hour
        self.minute = minute
        self.second = second


class Timestamp(object):

    def __init__(self, year, month, day, hour, minute, second):
        self.date = Date(year, month, day)
        self.time = Time(hour, minute, second)


class Binary(object):

    def __init__(self, string):
        self.data = string


class STRING(object):
    pass


class BINARY(object):
    pass


class NUMBER(object):
    pass


class DATETIME(object):
    pass


class ROWID(object):
    pass

MAP = {
    'DECIMAL': 0x00,
    'TINY': 0x01,
    'SHORT': 0x02,
    'LONG': 0x03,
    'INT': 0x03,
    'FLOAT': 0x04,
    'DOUBLE': 0x05,
    'NULL': 0x06,
    'TIMESTAMP': 0x07,
    'LONGLONG': 0x08,
    'INT24': 0x09,
    'DATE': 0x0a,
    'TIME': 0x0b,
    'DATETIME': 0x0c,
    'YEAR': 0x0d,
    'NEWDATE': 0x0e,
    'VARCHAR': 0x0f,
    'BIT': 0x10,
    'JSON': 0xf5,
    'NEWDECIMAL': 0xf6,
    'ENUM': 0xf7,
    'SET': 0xf8,
    'TINY_BLOB': 0xf9,
    'MEDIUM_BLOB': 0xfa,
    'LONG_BLOB': 0xfb,
    'BLOB': 0xfc,
    'TEXT': 0xfc,
    'VAR_STRING': 0xfd,
    'STRING': 0xfe,
    'GEOMETRY': 0xff,
}

for v, k in list(MAP.items()):
    MAP[k] = v
