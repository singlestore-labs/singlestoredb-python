"""Offline cursor parameter tests for signed TIME durations."""
import datetime

import pytest

from singlestoredb.converters import timedelta_fromisoformat
from singlestoredb.http.connection import encode_timedelta
from singlestoredb.mysql import connect


CASES = [
    (datetime.timedelta(microseconds=-1), '-00:00:00.000001'),
    (datetime.timedelta(seconds=-1), '-00:00:01'),
    (datetime.timedelta(minutes=-1), '-00:01:00'),
    (datetime.timedelta(hours=-1), '-01:00:00'),
    (datetime.timedelta(hours=-25, seconds=-17), '-25:00:17'),
    (datetime.timedelta(seconds=-1, microseconds=-123456), '-00:00:01.123456'),
    (datetime.timedelta(days=-2), '-48:00:00'),
    (datetime.timedelta(0), '00:00:00'),
    (datetime.timedelta(microseconds=1), '00:00:00.000001'),
    (datetime.timedelta(hours=25, seconds=17), '25:00:17'),
]


@pytest.mark.parametrize('duration, literal', CASES)
def test_mogrify_preserves_timedelta_sign_and_magnitude(
    duration: datetime.timedelta, literal: str,
) -> None:
    """Bind the correct TIME literal without opening a database connection."""
    connection = connect(defer_connect=True)
    cursor = connection.cursor()

    assert cursor.mogrify('SELECT %s', (duration,)) == f"SELECT '{literal}'"
    assert timedelta_fromisoformat(literal) == duration


@pytest.mark.parametrize('duration, literal', CASES)
def test_http_encode_timedelta_preserves_sign_and_magnitude(
    duration: datetime.timedelta, literal: str,
) -> None:
    """The HTTP API encoder writes the same TIME literal."""
    assert encode_timedelta(duration) == literal
