
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
