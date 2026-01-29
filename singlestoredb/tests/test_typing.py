#!/usr/bin/env python3
# type: ignore
"""Test UDF typing functions."""
import datetime
import unittest

import msgpack

from singlestoredb.functions.typing import msgpack_or_null_dumps
from singlestoredb.functions.typing import msgpack_or_null_loads


class TestMsgpackTimestamp(unittest.TestCase):
    """Test msgpack timestamp conversion."""

    def test_simple_timestamp(self):
        """Test deserializing a simple msgpack timestamp."""
        dt = datetime.datetime(2024, 1, 15, 12, 30, 45)
        ts = msgpack.Timestamp.from_datetime(dt)
        packed = msgpack.packb(ts)

        result = msgpack_or_null_loads(packed)

        self.assertIsInstance(result, datetime.datetime)
        self.assertEqual(result.year, 2024)
        self.assertEqual(result.month, 1)
        self.assertEqual(result.day, 15)

    def test_timestamp_in_dict(self):
        """Test deserializing a dict containing a timestamp."""
        dt = datetime.datetime(2024, 6, 20, 8, 15, 30)
        ts = msgpack.Timestamp.from_datetime(dt)
        data = {'timestamp': ts, 'value': 42}
        packed = msgpack.packb(data)

        result = msgpack_or_null_loads(packed)

        self.assertIsInstance(result, dict)
        self.assertIn('timestamp', result)
        self.assertIsInstance(result['timestamp'], datetime.datetime)
        self.assertEqual(result['timestamp'].year, 2024)
        self.assertEqual(result['timestamp'].month, 6)
        self.assertEqual(result['value'], 42)

    def test_timestamp_in_list(self):
        """Test deserializing a list containing timestamps."""
        # Use UTC datetimes to avoid timezone conversion issues
        dt1 = datetime.datetime(2024, 1, 15, 12, 0, 0, tzinfo=datetime.timezone.utc)
        dt2 = datetime.datetime(2024, 6, 20, 12, 0, 0, tzinfo=datetime.timezone.utc)
        ts1 = msgpack.Timestamp.from_datetime(dt1)
        ts2 = msgpack.Timestamp.from_datetime(dt2)
        data = [ts1, ts2]
        packed = msgpack.packb(data)

        result = msgpack_or_null_loads(packed)

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], datetime.datetime)
        self.assertIsInstance(result[1], datetime.datetime)
        self.assertEqual(result[0].month, 1)
        self.assertEqual(result[1].month, 6)

    def test_nested_timestamp(self):
        """Test deserializing nested structures with timestamps."""
        dt = datetime.datetime(2024, 3, 14, 9, 26, 53)
        ts = msgpack.Timestamp.from_datetime(dt)
        data = {
            'level1': {
                'level2': {
                    'timestamp': ts,
                },
            },
        }
        packed = msgpack.packb(data)

        result = msgpack_or_null_loads(packed)

        self.assertIsInstance(result['level1']['level2']['timestamp'], datetime.datetime)

    def test_none_input(self):
        """Test that None input returns None."""
        result = msgpack_or_null_loads(None)
        self.assertIsNone(result)

    def test_empty_bytes(self):
        """Test that empty bytes returns None."""
        result = msgpack_or_null_loads(b'')
        self.assertIsNone(result)

    def test_roundtrip_without_timestamp(self):
        """Test roundtrip of data without timestamps."""
        data = {'key': 'value', 'num': 123, 'list': [1, 2, 3]}
        packed = msgpack_or_null_dumps(data)
        result = msgpack_or_null_loads(packed)

        self.assertEqual(result, data)

    def test_datetime_true_encoding(self):
        """Test unpacking msgpack created with datetime=True."""
        # This mimics how users typically create msgpack with datetimes
        data = {'foo': 'bar', 'dt': datetime.datetime.now(tz=datetime.timezone.utc)}
        packed = msgpack.dumps(data, datetime=True)

        result = msgpack_or_null_loads(packed)

        self.assertIsInstance(result, dict)
        self.assertEqual(result['foo'], 'bar')
        self.assertIsInstance(result['dt'], datetime.datetime)

    def test_datetime_roundtrip(self):
        """Test that datetime objects survive a full round-trip."""
        # This tests the UDF scenario: input with datetime -> process -> output
        original_dt = datetime.datetime(
            2024, 6, 15, 10, 30, 0, tzinfo=datetime.timezone.utc,
        )
        data = {'value': 42, 'timestamp': original_dt}
        packed_in = msgpack.packb(data, datetime=True)

        # Unpack (simulating UDF input)
        unpacked = msgpack_or_null_loads(packed_in)
        self.assertIsInstance(unpacked['timestamp'], datetime.datetime)

        # Pack again (simulating UDF output)
        packed_out = msgpack_or_null_dumps(unpacked)
        self.assertIsNotNone(packed_out)

        # Verify final result
        final = msgpack_or_null_loads(packed_out)
        self.assertEqual(final['value'], 42)
        self.assertEqual(final['timestamp'], original_dt)


if __name__ == '__main__':
    unittest.main()
