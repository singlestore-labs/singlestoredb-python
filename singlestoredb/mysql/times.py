from datetime import date
from datetime import datetime
from datetime import time
from datetime import timedelta
from time import localtime


Date = date
Time = time
TimeDelta = timedelta
Timestamp = datetime


def DateFromTicks(ticks: int) -> date:
    return date(*localtime(ticks)[:3])


def TimeFromTicks(ticks: int) -> time:
    return time(*localtime(ticks)[3:6])


def TimestampFromTicks(ticks: int) -> datetime:
    return datetime(*localtime(ticks)[:6])
