#!/usr/bin/env python3
from typing import Any
from typing import List
from typing import Optional
from typing import Tuple

from .. import connection
from ..utils.results import Description
from ..utils.results import format_results

# MySQL data types
BOOL = 1
DOUBLE = 5
INT = 8
DATE = 10
TIME = 11
DATETIME = 12
JSON = 245
BLOB = 252
STRING = 253


class DummyField(object):
    """Field for PyMySQL compatibility."""

    def __init__(self, name: str, flags: int, charset: int) -> None:
        self.name = name
        self.flags = flags
        self.charsetnr = charset


class DummySQLResult:

    def __init__(self, connection: connection.Connection, unbuffered: bool = False):
        self.connection: Any = connection
        self.affected_rows: Optional[int] = None
        self.insert_id: int = 0
        self.server_status: Optional[int] = None
        self.warning_count: int = 0
        self.message: Optional[str] = None
        self.description: List[Description] = []
        self.rows: Any = []
        self.has_next: bool = False
        self.unbuffered_active: bool = False
        self.converters: List[Any] = []
        self.fields: List[DummyField] = []
        self._row_idx: int = -1

    def _read_rowdata_packet_unbuffered(self, size: int = 1) -> Optional[List[Any]]:
        if not self.rows:
            return None

        out = []

        try:
            for i in range(1, size + 1):
                out.append(self.rows[self._row_idx + i])
        except IndexError:
            self._row_idx = -1
            self.rows = []
            return None
        else:
            self._row_idx += size

        return out

    def _finish_unbuffered_query(self) -> None:
        self._row_idx = -1
        self.rows = []
        self.affected_rows = None

    def inject_data(
        self,
        desc: List[Tuple[str, int]],
        data: List[Tuple[Any, ...]],
    ) -> None:
        self.description = []
        self.rows = []
        self.affected_rows = 0
        self.converters = []
        self.fields = []

        if not desc:
            return

        for name, field_type in desc:
            charset = 0
            if field_type in [JSON, STRING]:
                encoding = 'utf-8'
            elif field_type == BLOB:
                charset = 63
                encoding = None
            else:
                encoding = 'ascii'
            self.description.append(
                Description(name, field_type, None, None, 0, 0, True, 0, charset),
            )
            self.fields.append(DummyField(name, 0, charset))
            # converter = self.connection.decoders.get(field_type)
            # if converter is converters.through:
            #    converter = None
            converter = None
            # if DEBUG:
            #    print(f'DEBUG: field={field}, converter={converter}')
            self.converters.append((encoding, converter))

        self.rows = format_results(self.connection._results_type, self.description, data)
