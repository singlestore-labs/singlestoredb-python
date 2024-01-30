# type: ignore
import re
from collections import namedtuple

from . import err
from ..connection import Cursor as BaseCursor
from ..utils.debug import log_query


#: Regular expression for :meth:`Cursor.executemany`.
#: executemany only supports simple bulk insert.
#: You can use it to load large dataset.
RE_INSERT_VALUES = re.compile(
    r'\s*((?:INSERT|REPLACE)\b.+\bVALUES?\s*)'
    + r'(\(\s*(?:%s|%\(.+\)s)\s*(?:,\s*(?:%s|%\(.+\)s)\s*)*\))'
    + r'(\s*(?:ON DUPLICATE.*)?);?\s*\Z',
    re.IGNORECASE | re.DOTALL,
)


class Cursor(BaseCursor):
    """
    This is the object used to interact with the database.

    Do not create an instance of a Cursor yourself. Call
    connection.Connection.cursor().

    See `Cursor <https://www.python.org/dev/peps/pep-0249/#cursor-objects>`_ in
    the specification.

    Parameters
    ----------
    connection : Connection
        The connection the cursor is associated with.

    """

    #: Max statement size which :meth:`executemany` generates.
    #:
    #: Max size of allowed statement is max_allowed_packet - packet_header_size.
    #: Default value of max_allowed_packet is 1048576.
    max_stmt_length = 1024000

    def __init__(self, connection):
        self._connection = connection
        self.warning_count = 0
        self._description = None
        self._rownumber = 0
        self.rowcount = -1
        self.arraysize = 1
        self._executed = None
        self._result = None
        self._rows = None
        self.lastrowid = None

    @property
    def messages(self):
        # TODO
        return []

    @property
    def description(self):
        return self._description

    @property
    def connection(self):
        return self._connection

    @property
    def rownumber(self):
        return self._rownumber

    def close(self):
        """Closing a cursor just exhausts all remaining data."""
        conn = self._connection
        if conn is None:
            return
        try:
            while self.nextset():
                pass
        finally:
            self._connection = None

    @property
    def open(self) -> bool:
        conn = self._connection
        if conn is None:
            return False
        return True

    def is_connected(self):
        return self.open

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        del exc_info
        self.close()

    def _get_db(self):
        if not self._connection:
            raise err.ProgrammingError('Cursor closed')
        return self._connection

    def _check_executed(self):
        if not self._executed:
            raise err.ProgrammingError('execute() first')

    def _conv_row(self, row):
        return row

    def setinputsizes(self, *args):
        """Does nothing, required by DB API."""

    def setoutputsizes(self, *args):
        """Does nothing, required by DB API."""

    setoutputsize = setoutputsizes

    def _nextset(self, unbuffered=False):
        """Get the next query set."""
        conn = self._get_db()
        current_result = self._result
        if current_result is None or current_result is not conn._result:
            return None
        if not current_result.has_next:
            return None
        self._result = None
        self._clear_result()
        conn.next_result(unbuffered=unbuffered)
        self._do_get_result()
        return True

    def nextset(self):
        return self._nextset(False)

    def _escape_args(self, args, conn):
        dtype = type(args)
        literal = conn.literal
        if dtype is tuple or dtype is list or isinstance(args, (tuple, list)):
            return tuple(literal(arg) for arg in args)
        elif dtype is dict or isinstance(args, dict):
            return {key: literal(val) for (key, val) in args.items()}
        # If it's not a dictionary let's try escaping it anyways.
        # Worst case it will throw a Value error
        return conn.escape(args)

    def mogrify(self, query, args=None):
        """
        Returns the exact string sent to the database by calling the execute() method.

        This method follows the extension to the DB API 2.0 followed by Psycopg.

        Parameters
        ----------
        query : str
            Query to mogrify.
        args : Sequence[Any] or Dict[str, Any] or Any, optional
            Parameters used with query. (optional)

        Returns
        -------
        str : The query with argument binding applied.

        """
        conn = self._get_db()

        if args:
            query = query % self._escape_args(args, conn)

        return query

    def execute(self, query, args=None):
        """
        Execute a query.

        If args is a list or tuple, :1, :2, etc. can be used as a
        placeholder in the query.  If args is a dict, :name can be used
        as a placeholder in the query.

        Parameters
        ----------
        query : str
            Query to execute.
        args : Sequence[Any] or Dict[str, Any] or Any, optional
            Parameters used with query. (optional)

        Returns
        -------
        int : Number of affected rows.

        """
        while self.nextset():
            pass

        log_query(query, args)

        query = self.mogrify(query, args)

        result = self._query(query)
        self._executed = query
        return result

    def executemany(self, query, args=None):
        """
        Run several data against one query.

        This method improves performance on multiple-row INSERT and
        REPLACE. Otherwise it is equivalent to looping over args with
        execute().

        Parameters
        ----------
        query : str,
            Query to execute.
        args : Sequnce[Any], optional
            Sequence of sequences or mappings. It is used as parameter.

        Returns
        -------
        int : Number of rows affected, if any.

        """
        if args is None or len(args) == 0:
            return

        m = RE_INSERT_VALUES.match(query)
        if m:
            q_prefix = m.group(1) % ()
            q_values = m.group(2).rstrip()
            q_postfix = m.group(3) or ''
            assert q_values[0] == '(' and q_values[-1] == ')'
            return self._do_execute_many(
                q_prefix,
                q_values,
                q_postfix,
                args,
                self.max_stmt_length,
                self._get_db().encoding,
            )

        self.rowcount = sum(self.execute(query, arg) for arg in args)
        return self.rowcount

    def _do_execute_many(
        self, prefix, values, postfix, args, max_stmt_length, encoding,
    ):
        conn = self._get_db()
        escape = self._escape_args
        if isinstance(prefix, str):
            prefix = prefix.encode(encoding)
        if isinstance(postfix, str):
            postfix = postfix.encode(encoding)
        sql = bytearray(prefix)
        # Detect dataframes
        if hasattr(args, 'itertuples'):
            args = args.itertuples(index=False)
        else:
            args = iter(args)
        v = values % escape(next(args), conn)
        if isinstance(v, str):
            v = v.encode(encoding, 'surrogateescape')
        sql += v
        rows = 0
        for arg in args:
            v = values % escape(arg, conn)
            if type(v) is str or isinstance(v, str):
                v = v.encode(encoding, 'surrogateescape')
            if len(sql) + len(v) + len(postfix) + 1 > max_stmt_length:
                rows += self.execute(sql + postfix)
                sql = bytearray(prefix)
            else:
                sql += b','
            sql += v
        rows += self.execute(sql + postfix)
        self.rowcount = rows
        return rows

    def callproc(self, procname, args=()):
        """
        Execute stored procedure procname with args.

        Compatibility warning: PEP-249 specifies that any modified
        parameters must be returned. This is currently impossible
        as they are only available by storing them in a server
        variable and then retrieved by a query. Since stored
        procedures return zero or more result sets, there is no
        reliable way to get at OUT or INOUT parameters via callproc.
        The server variables are named @_procname_n, where procname
        is the parameter above and n is the position of the parameter
        (from zero). Once all result sets generated by the procedure
        have been fetched, you can issue a SELECT @_procname_0, ...
        query using .execute() to get any OUT or INOUT values.

        Compatibility warning: The act of calling a stored procedure
        itself creates an empty result set. This appears after any
        result sets generated by the procedure. This is non-standard
        behavior with respect to the DB-API. Be sure to use nextset()
        to advance through all result sets; otherwise you may get
        disconnected.

        Parameters
        ----------
        procname : str
            Name of procedure to execute on server.
        args : Sequence[Any], optional
            Sequence of parameters to use with procedure.

        Returns
        -------
        Sequence[Any] : The original args.

        """
        conn = self._get_db()
        if args:
            fmt = f'@_{procname}_%d=%s'
            self._query(
                'SET %s'
                % ','.join(
                    fmt % (index, conn.escape(arg)) for index, arg in enumerate(args)
                ),
            )
            self.nextset()

        q = 'CALL {}({})'.format(
            procname,
            ','.join(['@_%s_%d' % (procname, i) for i in range(len(args))]),
        )
        self._query(q)
        self._executed = q
        return args

    def fetchone(self):
        """Fetch the next row."""
        self._check_executed()
        return self._unchecked_fetchone()

    def _unchecked_fetchone(self):
        """Fetch the next row."""
        if self._rows is None or self._rownumber >= len(self._rows):
            return None
        result = self._rows[self._rownumber]
        self._rownumber += 1
        return result

    def fetchmany(self, size=None):
        """Fetch several rows."""
        self._check_executed()
        if self._rows is None:
            self.warning_count = self._result.warning_count
            return ()
        end = self._rownumber + (size or self.arraysize)
        result = self._rows[self._rownumber: end]
        self._rownumber = min(end, len(self._rows))
        return result

    def fetchall(self):
        """Fetch all the rows."""
        self._check_executed()
        if self._rows is None:
            return ()
        if self._rownumber:
            result = self._rows[self._rownumber:]
        else:
            result = self._rows
        self._rownumber = len(self._rows)
        return result

    def scroll(self, value, mode='relative'):
        self._check_executed()
        if mode == 'relative':
            r = self._rownumber + value
        elif mode == 'absolute':
            r = value
        else:
            raise err.ProgrammingError('unknown scroll mode %s' % mode)

        if not (0 <= r < len(self._rows)):
            raise IndexError('out of range')
        self._rownumber = r

    def _query(self, q):
        conn = self._get_db()
        self._clear_result()
        conn.query(q)
        self._do_get_result()
        return self.rowcount

    def _clear_result(self):
        self._rownumber = 0
        self._result = None

        self.rowcount = 0
        self.warning_count = 0
        self._description = None
        self.lastrowid = None
        self._rows = None

    def _do_get_result(self):
        conn = self._get_db()

        self._result = result = conn._result

        self.rowcount = result.affected_rows
        self.warning_count = result.warning_count
        # Affected rows is set to max int64 for compatibility with MySQLdb, but
        # the DB-API requires this value to be -1. This happens in unbuffered mode.
        if self.rowcount == 18446744073709551615:
            self.rowcount = -1
        self._description = result.description
        self.lastrowid = result.insert_id
        self._rows = result.rows

    def __iter__(self):
        self._check_executed()

        def fetchall_unbuffered_gen(_unchecked_fetchone=self._unchecked_fetchone):
            while True:
                out = _unchecked_fetchone()
                if out is not None:
                    yield out
                else:
                    break
        return fetchall_unbuffered_gen()

    Warning = err.Warning
    Error = err.Error
    InterfaceError = err.InterfaceError
    DatabaseError = err.DatabaseError
    DataError = err.DataError
    OperationalError = err.OperationalError
    IntegrityError = err.IntegrityError
    InternalError = err.InternalError
    ProgrammingError = err.ProgrammingError
    NotSupportedError = err.NotSupportedError


class CursorSV(Cursor):
    """Cursor class for C extension."""


class DictCursorMixin:
    # You can override this to use OrderedDict or other dict-like types.
    dict_type = dict

    def _do_get_result(self):
        super(DictCursorMixin, self)._do_get_result()
        fields = []
        if self._description:
            for f in self._result.fields:
                name = f.name
                if name in fields:
                    name = f.table_name + '.' + name
                fields.append(name)
            self._fields = fields

        if fields and self._rows:
            self._rows = [self._conv_row(r) for r in self._rows]

    def _conv_row(self, row):
        if row is None:
            return None
        return self.dict_type(zip(self._fields, row))


class DictCursor(DictCursorMixin, Cursor):
    """A cursor which returns results as a dictionary."""


class DictCursorSV(Cursor):
    """A cursor which returns results as a dictionary for C extension."""


class NamedtupleCursorMixin:

    def _do_get_result(self):
        super(NamedtupleCursorMixin, self)._do_get_result()
        fields = []
        if self._description:
            for f in self._result.fields:
                name = f.name
                if name in fields:
                    name = f.table_name + '.' + name
                fields.append(name)
            self._fields = fields
            self._namedtuple = namedtuple('Row', self._fields, rename=True)

        if fields and self._rows:
            self._rows = [self._conv_row(r) for r in self._rows]

    def _conv_row(self, row):
        if row is None:
            return None
        return self._namedtuple(*row)


class NamedtupleCursor(NamedtupleCursorMixin, Cursor):
    """A cursor which returns results in a named tuple."""


class NamedtupleCursorSV(Cursor):
    """A cursor which returns results as a named tuple for C extension."""


class SSCursor(Cursor):
    """
    Unbuffered Cursor, mainly useful for queries that return a lot of data,
    or for connections to remote servers over a slow network.

    Instead of copying every row of data into a buffer, this will fetch
    rows as needed. The upside of this is the client uses much less memory,
    and rows are returned much faster when traveling over a slow network
    or if the result set is very big.

    There are limitations, though. The MySQL protocol doesn't support
    returning the total number of rows, so the only way to tell how many rows
    there are is to iterate over every row returned. Also, it currently isn't
    possible to scroll backwards, as only the current row is held in memory.

    """

    def _conv_row(self, row):
        return row

    def close(self):
        conn = self._connection
        if conn is None:
            return

        if self._result is not None and self._result is conn._result:
            self._result._finish_unbuffered_query()

        try:
            while self.nextset():
                pass
        finally:
            self._connection = None

    __del__ = close

    def _query(self, q):
        conn = self._get_db()
        self._clear_result()
        conn.query(q, unbuffered=True)
        self._do_get_result()
        return self.rowcount

    def nextset(self):
        return self._nextset(unbuffered=True)

    def read_next(self):
        """Read next row."""
        return self._conv_row(self._result._read_rowdata_packet_unbuffered())

    def fetchone(self):
        """Fetch next row."""
        self._check_executed()
        return self._unchecked_fetchone()

    def _unchecked_fetchone(self):
        """Fetch next row."""
        row = self.read_next()
        if row is None:
            self.warning_count = self._result.warning_count
            return None
        self._rownumber += 1
        return row

    def fetchall(self):
        """
        Fetch all, as per MySQLdb.

        Pretty useless for large queries, as it is buffered.
        See fetchall_unbuffered(), if you want an unbuffered
        generator version of this method.

        """
        return list(self.fetchall_unbuffered())

    def fetchall_unbuffered(self):
        """
        Fetch all, implemented as a generator.

        This is not a standard DB-API operation, however, it doesn't make
        sense to return everything in a list, as that would use ridiculous
        memory for large result sets.

        """
        self._check_executed()

        def fetchall_unbuffered_gen(_unchecked_fetchone=self._unchecked_fetchone):
            while True:
                out = _unchecked_fetchone()
                if out is not None:
                    yield out
                else:
                    break
        return fetchall_unbuffered_gen()

    def __iter__(self):
        return self.fetchall_unbuffered()

    def fetchmany(self, size=None):
        """Fetch many."""
        self._check_executed()
        if size is None:
            size = self.arraysize

        rows = []
        for i in range(size):
            row = self.read_next()
            if row is None:
                self.warning_count = self._result.warning_count
                break
            rows.append(row)
            self._rownumber += 1
        return rows

    def scroll(self, value, mode='relative'):
        self._check_executed()

        if mode == 'relative':
            if value < 0:
                raise err.NotSupportedError(
                    'Backwards scrolling not supported by this cursor',
                )

            for _ in range(value):
                self.read_next()
            self._rownumber += value
        elif mode == 'absolute':
            if value < self._rownumber:
                raise err.NotSupportedError(
                    'Backwards scrolling not supported by this cursor',
                )

            end = value - self._rownumber
            for _ in range(end):
                self.read_next()
            self._rownumber = value
        else:
            raise err.ProgrammingError('unknown scroll mode %s' % mode)


class SSCursorSV(SSCursor):
    """An unbuffered cursor for use with PyMySQLsv."""

    def _unchecked_fetchone(self):
        """Fetch next row."""
        row = self._result._read_rowdata_packet_unbuffered(1)
        if row is None:
            return None
        self._rownumber += 1
        return row

    def fetchone(self):
        """Fetch next row."""
        self._check_executed()
        return self._unchecked_fetchone()

    def fetchmany(self, size=None):
        """Fetch many."""
        self._check_executed()
        if size is None:
            size = self.arraysize
        out = self._result._read_rowdata_packet_unbuffered(size)
        if out is None:
            return []
        if size == 1:
            self._rownumber += 1
            return [out]
        self._rownumber += len(out)
        return out

    def scroll(self, value, mode='relative'):
        self._check_executed()

        if mode == 'relative':
            if value < 0:
                raise err.NotSupportedError(
                    'Backwards scrolling not supported by this cursor',
                )

            self._result._read_rowdata_packet_unbuffered(value)
            self._rownumber += value
        elif mode == 'absolute':
            if value < self._rownumber:
                raise err.NotSupportedError(
                    'Backwards scrolling not supported by this cursor',
                )

            end = value - self._rownumber
            self._result._read_rowdata_packet_unbuffered(end)
            self._rownumber = value
        else:
            raise err.ProgrammingError('unknown scroll mode %s' % mode)


class SSDictCursor(DictCursorMixin, SSCursor):
    """An unbuffered cursor, which returns results as a dictionary"""


class SSDictCursorSV(SSCursorSV):
    """An unbuffered cursor for the C extension, which returns a dictionary"""


class SSNamedtupleCursor(NamedtupleCursorMixin, SSCursor):
    """An unbuffered cursor, which returns results as a named tuple"""


class SSNamedtupleCursorSV(SSCursorSV):
    """An unbuffered cursor for the C extension, which returns results as a named tuple"""
