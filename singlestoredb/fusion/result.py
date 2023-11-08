#!/usr/bin/env python3
from __future__ import annotations

import re
from typing import Any
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

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


class FusionSQLColumn(object):
    """Column accessor for a FusionSQLResult."""

    def __init__(self, result: FusionSQLResult, index: int) -> None:
        self._result = result
        self._index = index

    def __getitem__(self, index: Any) -> Any:
        return self._result.rows[index][self._index]

    def __iter__(self) -> Iterable[Any]:
        def gen() -> Iterable[Any]:
            for row in iter(self._result):
                yield row[self._index]
        return gen()


class FieldIndexDict(dict):  # type: ignore
    """Case-insensitive dictionary for column name lookups."""

    def __getitem__(self, key: str) -> int:
        return super().__getitem__(key.lower())

    def __setitem__(self, key: str, value: int) -> None:
        super().__setitem__(key.lower(), value)

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        return super().__contains__(str(key).lower())

    def copy(self) -> FieldIndexDict:
        out = type(self)()
        for k, v in self.items():
            out[k.lower()] = v
        return out


class FusionSQLResult(object):
    """Result for Fusion SQL commands."""

    def __init__(self) -> None:
        self.connection: Any = None
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
        self._field_indexes: FieldIndexDict = FieldIndexDict()
        self._row_idx: int = -1

    def copy(self) -> FusionSQLResult:
        """Copy the result."""
        out = type(self)()
        for k, v in vars(self).items():
            if isinstance(v, list):
                setattr(out, k, list(v))
            elif isinstance(v, dict):
                setattr(out, k, v.copy())
            else:
                setattr(out, k, v)
        return out

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

    def format_results(self, connection: connection.Connection) -> None:
        """
        Format the results using the connection converters and options.

        Parameters
        ----------
        connection : Connection
            The connection containing the converters and options

        """
        self.converters = []

        for item in self.description:
            self.converters.append((
                item.charset,
                connection.decoders.get(item.type_code),
            ))

        # Convert values
        for i, row in enumerate(self.rows):
            new_row = []
            for (_, converter), value in zip(self.converters, row):
                new_row.append(converter(value) if converter is not None else value)
            self.rows[i] = tuple(new_row)

        self.rows[:] = format_results(
            connection._results_type, self.description, self.rows,
        )

    def __iter__(self) -> Iterable[Tuple[Any, ...]]:
        return iter(self.rows)

    def __len__(self) -> int:
        return len(self.rows)

    def __getitem__(self, key: Any) -> Tuple[Any, ...]:
        if isinstance(key, str):
            return self.__getattr__(key)
        return self.rows[key]

    def __getattr__(self, name: str) -> Any:
        return FusionSQLColumn(self, self._field_indexes[name])

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
        self._field_indexes[name] = len(self.fields) - 1
        self.converters.append((encoding, None))

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
        self.rows = list(data)
        self.affected_rows = 0

    def like(self, **kwargs: str) -> FusionSQLResult:
        """
        Return a new result containing only rows that match all `kwargs` like patterns.

        Parameters
        ----------
        **kwargs : str
            Each parameter name corresponds to a column name in the result. The value
            of the parameters is a LIKE pattern to match.

        Returns
        -------
        FusionSQLResult

        """
        likers = []
        for k, v in kwargs.items():
            if k not in self._field_indexes:
                raise KeyError(f'field name does not exist in results: {k}')
            if not v:
                continue
            regex = re.compile(
                '^{}$'.format(
                    re.sub(r'\\%', r'.*', re.sub(r'([^\w])', r'\\\1', v)),
                ), flags=re.I | re.M,
            )
            likers.append((self._field_indexes[k], regex))

        filtered_rows = []
        for row in self.rows:
            found = True
            for i, liker in likers:
                if not liker.match(row[i]):
                    found = False
                    break
            if found:
                filtered_rows.append(row)

        out = self.copy()
        out.rows[:] = filtered_rows
        return out

    like_all = like

    def like_any(self, **kwargs: str) -> FusionSQLResult:
        """
        Return a new result containing only rows that match any `kwargs` like patterns.

        Parameters
        ----------
        **kwargs : str
            Each parameter name corresponds to a column name in the result. The value
            of the parameters is a LIKE pattern to match.

        Returns
        -------
        FusionSQLResult

        """
        likers = []
        for k, v in kwargs.items():
            if k not in self._field_indexes:
                raise KeyError(f'field name does not exist in results: {k}')
            if not v:
                continue
            regex = re.compile(
                '^{}$'.format(
                    re.sub(r'\\%', r'.*', re.sub(r'([^\w])', r'\\\1', v)),
                ), flags=re.I | re.M,
            )
            likers.append((self._field_indexes[k], regex))

        filtered_rows = []
        for row in self.rows:
            found = False
            for i, liker in likers:
                if liker.match(row[i]):
                    found = True
                    break
            if found:
                filtered_rows.append(row)

        out = self.copy()
        out.rows[:] = filtered_rows
        return out

    def filter(self, **kwargs: str) -> FusionSQLResult:
        """
        Return a new result containing only rows that match all `kwargs` values.

        Parameters
        ----------
        **kwargs : str
            Each parameter name corresponds to a column name in the result. The value
            of the parameters is the value to match.

        Returns
        -------
        FusionSQLResult

        """
        if not kwargs:
            return self.copy()

        values = []
        for k, v in kwargs.items():
            if k not in self._field_indexes:
                raise KeyError(f'field name does not exist in results: {k}')
            values.append((self._field_indexes[k], v))

        filtered_rows = []
        for row in self.rows:
            found = True
            for i, val in values:
                if row[0] != val:
                    found = False
                    break
            if found:
                filtered_rows.append(row)

        out = self.copy()
        out.rows[:] = filtered_rows
        return out

    def limit(self, n_rows: int) -> FusionSQLResult:
        """
        Return a new result containing only `n_rows` rows.

        Parameters
        ----------
        n_rows : int
            The number of rows to limit the result to

        Returns
        -------
        FusionSQLResult

        """
        out = self.copy()
        if n_rows:
            out.rows[:] = out.rows[:n_rows]
        return out

    def sort_by(
        self,
        by: Union[str, List[str]],
        ascending: Union[bool, List[bool]] = True,
    ) -> FusionSQLResult:
        """
        Return a new result with rows sorted in specified order.

        Parameters
        ----------
        by : str or List[str]
            Name or names of columns to sort by
        ascending : bool or List[bool], optional
            Should the sort order be ascending? If not all sort columns
            use the same ordering, a list of booleans can be supplied to
            indicate the order for each column.

        Returns
        -------
        FusionSQLResult

        """
        if not by:
            return self.copy()

        if isinstance(by, str):
            by = [by]
        by = list(reversed(by))

        if isinstance(ascending, bool):
            ascending = [ascending]
        ascending = list(reversed(ascending))

        out = self.copy()
        for i, byvar in enumerate(by):
            out.rows.sort(
                key=lambda x: (
                    0 if x[self._field_indexes[byvar]] is None else 1,
                    x[self._field_indexes[byvar]],
                ),
                reverse=not ascending[i],
            )
        return out

    order_by = sort_by
