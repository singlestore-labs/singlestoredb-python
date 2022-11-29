# type: ignore
import struct

from ..exceptions import DatabaseError  # noqa: F401
from ..exceptions import DataError  # noqa: F401
from ..exceptions import Error  # noqa: F401
from ..exceptions import IntegrityError  # noqa: F401
from ..exceptions import InterfaceError  # noqa: F401
from ..exceptions import InternalError  # noqa: F401
from ..exceptions import MySQLError  # noqa: F401
from ..exceptions import NotSupportedError  # noqa: F401
from ..exceptions import OperationalError  # noqa: F401
from ..exceptions import ProgrammingError  # noqa: F401
from ..exceptions import Warning  # noqa: F401
from .constants import ER


error_map = {}


def _map_error(exc, *errors):
    for error in errors:
        error_map[error] = exc


_map_error(
    ProgrammingError,
    ER.DB_CREATE_EXISTS,
    ER.SYNTAX_ERROR,
    ER.PARSE_ERROR,
    ER.NO_SUCH_TABLE,
    ER.WRONG_DB_NAME,
    ER.WRONG_TABLE_NAME,
    ER.FIELD_SPECIFIED_TWICE,
    ER.INVALID_GROUP_FUNC_USE,
    ER.UNSUPPORTED_EXTENSION,
    ER.TABLE_MUST_HAVE_COLUMNS,
    ER.CANT_DO_THIS_DURING_AN_TRANSACTION,
    ER.WRONG_DB_NAME,
    ER.WRONG_COLUMN_NAME,
)
_map_error(
    DataError,
    ER.WARN_DATA_TRUNCATED,
    ER.WARN_NULL_TO_NOTNULL,
    ER.WARN_DATA_OUT_OF_RANGE,
    ER.NO_DEFAULT,
    ER.PRIMARY_CANT_HAVE_NULL,
    ER.DATA_TOO_LONG,
    ER.DATETIME_FUNCTION_OVERFLOW,
    ER.TRUNCATED_WRONG_VALUE_FOR_FIELD,
    ER.ILLEGAL_VALUE_FOR_TYPE,
)
_map_error(
    IntegrityError,
    ER.DUP_ENTRY,
    ER.NO_REFERENCED_ROW,
    ER.NO_REFERENCED_ROW_2,
    ER.ROW_IS_REFERENCED,
    ER.ROW_IS_REFERENCED_2,
    ER.CANNOT_ADD_FOREIGN,
    ER.BAD_NULL_ERROR,
)
_map_error(
    NotSupportedError,
    ER.WARNING_NOT_COMPLETE_ROLLBACK,
    ER.NOT_SUPPORTED_YET,
    ER.FEATURE_DISABLED,
    ER.UNKNOWN_STORAGE_ENGINE,
)
_map_error(
    OperationalError,
    ER.DBACCESS_DENIED_ERROR,
    ER.ACCESS_DENIED_ERROR,
    ER.CON_COUNT_ERROR,
    ER.TABLEACCESS_DENIED_ERROR,
    ER.COLUMNACCESS_DENIED_ERROR,
    ER.CONSTRAINT_FAILED,
    ER.LOCK_DEADLOCK,
)


del _map_error, ER


def raise_mysql_exception(data):
    errno = struct.unpack('<h', data[1:3])[0]
    errval = data[9:].decode('utf-8', 'replace')
    errorclass = error_map.get(errno)
    if errorclass is None:
        errorclass = InternalError if errno < 1000 else OperationalError
    raise errorclass(errno, errval)
