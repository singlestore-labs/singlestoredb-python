#!/usr/bin/env python3
from __future__ import annotations

from typing import Any
from typing import List
from typing import Optional
from typing import Tuple

from .. import connection
from ..mysql.constants.FIELD_TYPE import BLOB  # noqa: F401
from ..mysql.constants.FIELD_TYPE import BOOL  # noqa: F401
from ..mysql.constants.FIELD_TYPE import DATE  # noqa: F401
from ..mysql.constants.FIELD_TYPE import DATETIME  # noqa: F401
from ..mysql.constants.FIELD_TYPE import DOUBLE  # noqa: F401
from ..mysql.constants.FIELD_TYPE import JSON  # noqa: F401
from ..mysql.constants.FIELD_TYPE import LONGLONG as INTEGER  # noqa: F401
from ..mysql.constants.FIELD_TYPE import STRING  # noqa: F401
from ..utils.results import Description
from ..utils.results import format_results


class FusionField(object):
    """Field for PyMySQL compatibility."""

    def __init__(self, name: str, flags: int, charset: int) -> None:
        self.name = name
        self.flags = flags
        self.charsetnr = charset


class FusionSQLResult:
    """Result for Fusion SQL commands."""

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
        self.fields: List[FusionField] = []
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

    def add_field(self, name: str, dtype: int) -> None:
        """
        Add a new field / column to the data set.

        Parameters
        ----------
        name : str
            The name of the field / column
        dtype : int
            The MySQL field type: BLOB, BOOL, DATE, DATETIME,
            DOUBLE, JSON, INTEGER, or STRING

        """
        charset = 0
        if dtype in (JSON, STRING):
            encoding = 'utf-8'
        elif dtype == BLOB:
            charset = 63
            encoding = None
        else:
            encoding = 'ascii'
        self.description.append(
            Description(name, dtype, None, None, 0, 0, True, 0, charset),
        )
        self.fields.append(FusionField(name, 0, charset))
        converter = self.connection.decoders.get(dtype)
        self.converters.append((encoding, converter))

    def set_rows(self, data: List[Tuple[Any, ...]]) -> None:
        """
        Set the rows of the result.

        Parameters
        ----------
        data : List[Tuple[Any, ...]]
            The data should be a list of tuples where each element of the
            tuple corresponds to a field added to the result with
            the :meth:`add_field` method.

        """
        # Convert values
        for i, row in enumerate(list(data)):
            new_row = []
            for (_, converter), value in zip(self.converters, row):
                new_row.append(converter(value) if converter is not None else value)
            data[i] = tuple(new_row)

        self.rows = format_results(self.connection._results_type, self.description, data)
        self.affected_rows = 0
