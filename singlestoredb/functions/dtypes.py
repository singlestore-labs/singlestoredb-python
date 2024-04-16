#!/usr/bin/env python3
import datetime
import decimal
import re
from typing import Any
from typing import Callable
from typing import Optional
from typing import Tuple
from typing import Union

from ..converters import converters
from ..mysql.converters import escape_item  # type: ignore
from ..utils.dtypes import DEFAULT_VALUES  # noqa
from ..utils.dtypes import NUMPY_TYPE_MAP  # noqa
from ..utils.dtypes import PANDAS_TYPE_MAP  # noqa
from ..utils.dtypes import POLARS_TYPE_MAP  # noqa
from ..utils.dtypes import PYARROW_TYPE_MAP  # noqa


DataType = Union[str, Callable[..., Any]]


class NULL:
    """NULL (for use in default values)."""
    pass


def escape_name(name: str) -> str:
    """Escape a function parameter name."""
    if '`' in name:
        name = name.replace('`', '``')
    return f'`{name}`'


# charsets
utf8mb4 = 'utf8mb4'
utf8 = 'utf8'
binary = 'binary'

# collations
utf8_general_ci = 'utf8_general_ci'
utf8_bin = 'utf8_bin'
utf8_unicode_ci = 'utf8_unicode_ci'
utf8_icelandic_ci = 'utf8_icelandic_ci'
utf8_latvian_ci = 'utf8_latvian_ci'
utf8_romanian_ci = 'utf8_romanian_ci'
utf8_slovenian_ci = 'utf8_slovenian_ci'
utf8_polish_ci = 'utf8_polish_ci'
utf8_estonian_ci = 'utf8_estonian_ci'
utf8_spanish_ci = 'utf8_spanish_ci'
utf8_swedish_ci = 'utf8_swedish_ci'
utf8_turkish_ci = 'utf8_turkish_ci'
utf8_czech_ci = 'utf8_czech_ci'
utf8_danish_ci = 'utf8_danish_ci'
utf8_lithuanian_ci = 'utf8_lithuanian_ci'
utf8_slovak_ci = 'utf8_slovak_ci'
utf8_spanish2_ci = 'utf8_spanish2_ci'
utf8_roman_ci = 'utf8_roman_ci'
utf8_persian_ci = 'utf8_persian_ci'
utf8_esperanto_ci = 'utf8_esperanto_ci'
utf8_hungarian_ci = 'utf8_hungarian_ci'
utf8_sinhala_ci = 'utf8_sinhala_ci'
utf8mb4_general_ci = 'utf8mb4_general_ci'
utf8mb4_bin = 'utf8mb4_bin'
utf8mb4_unicode_ci = 'utf8mb4_unicode_ci'
utf8mb4_icelandic_ci = 'utf8mb4_icelandic_ci'
utf8mb4_latvian_ci = 'utf8mb4_latvian_ci'
utf8mb4_romanian_ci = 'utf8mb4_romanian_ci'
utf8mb4_slovenian_ci = 'utf8mb4_slovenian_ci'
utf8mb4_polish_ci = 'utf8mb4_polish_ci'
utf8mb4_estonian_ci = 'utf8mb4_estonian_ci'
utf8mb4_spanish_ci = 'utf8mb4_spanish_ci'
utf8mb4_swedish_ci = 'utf8mb4_swedish_ci'
utf8mb4_turkish_ci = 'utf8mb4_turkish_ci'
utf8mb4_czech_ci = 'utf8mb4_czech_ci'
utf8mb4_danish_ci = 'utf8mb4_danish_ci'
utf8mb4_lithuanian_ci = 'utf8mb4_lithuanian_ci'
utf8mb4_slovak_ci = 'utf8mb4_slovak_ci'
utf8mb4_spanish2_ci = 'utf8mb4_spanish2_ci'
utf8mb4_roman_ci = 'utf8mb4_roman_ci'
utf8mb4_persian_ci = 'utf8mb4_persian_ci'
utf8mb4_esperanto_ci = 'utf8mb4_esperanto_ci'
utf8mb4_hungarian_ci = 'utf8mb4_hungarian_ci'
utf8mb4_sinhala_ci = 'utf8mb4_sinhala_ci'


def identity(x: Any) -> Any:
    return x


def utf8str(x: Any) -> Optional[str]:
    if x is None:
        return x
    if isinstance(x, str):
        return x
    return str(x, 'utf-8')


def bytestr(x: Any) -> Optional[bytes]:
    if x is None:
        return x
    if isinstance(x, bytes):
        return x
    return bytes.fromhex(x)


PYTHON_CONVERTERS = {
    -1: converters[1],
    -2: converters[2],
    -3: converters[3],
    -8: converters[8],
    -9: converters[9],
    15: utf8str,
    -15: bytestr,
    249: utf8str,
    -249: bytestr,
    250: utf8str,
    -250: bytestr,
    251: utf8str,
    -251: bytestr,
    252: utf8str,
    -252: bytestr,
    254: utf8str,
    -254: bytestr,
    255: utf8str,
}

PYTHON_CONVERTERS = dict(list(converters.items()) + list(PYTHON_CONVERTERS.items()))


def _modifiers(
    *,
    nullable: Optional[bool] = None,
    charset: Optional[str] = None,
    collate: Optional[str] = None,
    default: Optional[Any] = None,
    unsigned: Optional[bool] = None,
) -> str:
    """
    Format type modifiers.

    Parameters
    ----------
    nullable : bool, optional
        Can the value be NULL?
    charset : str, optional
        Character set
    collate : str, optional
        Collation
    default ; Any, optional
        Default value
    unsigned : bool, optional
        Is the value unsigned? (ints only)

    Returns
    -------
    str

    """
    out = []

    if unsigned is not None:
        if unsigned:
            out.append('UNSIGNED')

    if charset is not None:
        if not re.match(r'^[A-Za-z0-9_]+$', charset):
            raise ValueError(f'charset value is invalid: {charset}')
        out.append(f'CHARACTER SET {charset}')

    if collate is not None:
        if not re.match(r'^[A-Za-z0-9_]+$', collate):
            raise ValueError(f'collate value is invalid: {collate}')
        out.append(f'COLLATE {collate}')

    if nullable is not None:
        if nullable:
            out.append('NULL')
        else:
            out.append('NOT NULL')

    if default is NULL:
        out.append('DEFAULT NULL')
    elif default is not None:
        out.append(f'DEFAULT {escape_item(default, "utf-8")}')

    return ' ' + ' '.join(out)


def _bool(x: Optional[bool] = None) -> Optional[bool]:
    """Cast bool."""
    if x is None:
        return None
    return bool(x)


def BOOL(*, nullable: bool = True, default: Optional[bool] = None) -> str:
    """
    BOOL type specification.

    Parameters
    ----------
    nullable : bool, optional
        Can the value be NULL?
    default : bool, optional
        Default value

    Returns
    -------
    str

    """
    return 'BOOL' + _modifiers(nullable=nullable, default=_bool(default))


def BOOLEAN(*, nullable: bool = True, default: Optional[bool] = None) -> str:
    """
    BOOLEAN type specification.

    Parameters
    ----------
    nullable : bool, optional
        Can the value be NULL?
    default : bool, optional
        Default value

    Returns
    -------
    str

    """
    return 'BOOLEAN' + _modifiers(nullable=nullable, default=_bool(default))


def BIT(*, nullable: bool = True, default: Optional[int] = None) -> str:
    """
    BIT type specification.

    Parameters
    ----------
    nullable : bool, optional
        Can the value be NULL?
    default : int, optional
        Default value

    Returns
    -------
    str

    """
    return 'BIT' + _modifiers(nullable=nullable, default=default)


def TINYINT(
    display_width: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[int] = None,
    unsigned: bool = False,
) -> str:
    """
    TINYINT type specification.

    Parameters
    ----------
    display_width : int, optional
        Display width used by some clients
    nullable : bool, optional
        Can the value be NULL?
    default : int, optional
        Default value
    unsigned : bool, optional
        Is the int unsigned?

    Returns
    -------
    str

    """
    out = f'TINYINT({display_width})' if display_width else 'TINYINT'
    return out + _modifiers(nullable=nullable, default=default, unsigned=unsigned)


def TINYINT_UNSIGNED(
    display_width: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[int] = None,
) -> str:
    """
    TINYINT UNSIGNED type specification.

    Parameters
    ----------
    display_width : int, optional
        Display width used by some clients
    nullable : bool, optional
        Can the value be NULL?
    default : int, optional
        Default value

    Returns
    -------
    str

    """
    out = f'TINYINT({display_width})' if display_width else 'TINYINT'
    return out + _modifiers(nullable=nullable, default=default, unsigned=True)


def SMALLINT(
    display_width: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[int] = None,
    unsigned: bool = False,
) -> str:
    """
    SMALLINT type specification.

    Parameters
    ----------
    display_width : int, optional
        Display width used by some clients
    nullable : bool, optional
        Can the value be NULL?
    default : int, optional
        Default value
    unsigned : bool, optional
        Is the int unsigned?

    Returns
    -------
    str

    """
    out = f'SMALLINT({display_width})' if display_width else 'SMALLINT'
    return out + _modifiers(nullable=nullable, default=default, unsigned=unsigned)


def SMALLINT_UNSIGNED(
    display_width: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[int] = None,
) -> str:
    """
    SMALLINT UNSIGNED type specification.

    Parameters
    ----------
    display_width : int, optional
        Display width used by some clients
    nullable : bool, optional
        Can the value be NULL?
    default : int, optional
        Default value

    Returns
    -------
    str

    """
    out = f'SMALLINT({display_width})' if display_width else 'SMALLINT'
    return out + _modifiers(nullable=nullable, default=default, unsigned=True)


def MEDIUMINT(
    display_width: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[int] = None,
    unsigned: bool = False,
) -> str:
    """
    MEDIUMINT type specification.

    Parameters
    ----------
    display_width : int, optional
        Display width used by some clients
    nullable : bool, optional
        Can the value be NULL?
    default : int, optional
        Default value
    unsigned : bool, optional
        Is the int unsigned?

    Returns
    -------
    str

    """
    out = f'MEDIUMINT({display_width})' if display_width else 'MEDIUMINT'
    return out + _modifiers(nullable=nullable, default=default, unsigned=unsigned)


def MEDIUMINT_UNSIGNED(
    display_width: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[int] = None,
) -> str:
    """
    MEDIUMINT UNSIGNED type specification.

    Parameters
    ----------
    display_width : int, optional
        Display width used by some clients
    nullable : bool, optional
        Can the value be NULL?
    default : int, optional
        Default value

    Returns
    -------
    str

    """
    out = f'MEDIUMINT({display_width})' if display_width else 'MEDIUMINT'
    return out + _modifiers(nullable=nullable, default=default, unsigned=True)


def INT(
    display_width: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[int] = None,
    unsigned: bool = False,
) -> str:
    """
    INT type specification.

    Parameters
    ----------
    display_width : int, optional
        Display width used by some clients
    nullable : bool, optional
        Can the value be NULL?
    default : int, optional
        Default value
    unsigned : bool, optional
        Is the int unsigned?

    Returns
    -------
    str

    """
    out = f'INT({display_width})' if display_width else 'INT'
    return out + _modifiers(nullable=nullable, default=default, unsigned=unsigned)


def INT_UNSIGNED(
    display_width: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[int] = None,
) -> str:
    """
    INT UNSIGNED type specification.

    Parameters
    ----------
    display_width : int, optional
        Display width used by some clients
    nullable : bool, optional
        Can the value be NULL?
    default : int, optional
        Default value

    Returns
    -------
    str

    """
    out = f'INT({display_width})' if display_width else 'INT'
    return out + _modifiers(nullable=nullable, default=default, unsigned=True)


def INTEGER(
    display_width: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[int] = None,
    unsigned: bool = False,
) -> str:
    """
    INTEGER type specification.

    Parameters
    ----------
    display_width : int, optional
        Display width used by some clients
    nullable : bool, optional
        Can the value be NULL?
    default : int, optional
        Default value
    unsigned : bool, optional
        Is the int unsigned?

    Returns
    -------
    str

    """
    out = f'INTEGER({display_width})' if display_width else 'INTEGER'
    return out + _modifiers(nullable=nullable, default=default, unsigned=unsigned)


def INTEGER_UNSIGNED(
    display_width: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[int] = None,
) -> str:
    """
    INTEGER UNSIGNED type specification.

    Parameters
    ----------
    display_width : int, optional
        Display width used by some clients
    nullable : bool, optional
        Can the value be NULL?
    default : int, optional
        Default value

    Returns
    -------
    str

    """
    out = f'INTEGER({display_width})' if display_width else 'INTEGER'
    return out + _modifiers(nullable=nullable, default=default, unsigned=True)


def BIGINT(
    display_width: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[int] = None,
    unsigned: bool = False,
) -> str:
    """
    BIGINT type specification.

    Parameters
    ----------
    display_width : int, optional
        Display width used by some clients
    nullable : bool, optional
        Can the value be NULL?
    default : int, optional
        Default value
    unsigned : bool, optional
        Is the int unsigned?

    Returns
    -------
    str

    """
    out = f'BIGINT({display_width})' if display_width else 'BIGINT'
    return out + _modifiers(nullable=nullable, default=default, unsigned=unsigned)


def BIGINT_UNSIGNED(
    display_width: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[int] = None,
) -> str:
    """
    BIGINT UNSIGNED type specification.

    Parameters
    ----------
    display_width : int, optional
        Display width used by some clients
    nullable : bool, optional
        Can the value be NULL?
    default : int, optional
        Default value

    Returns
    -------
    str

    """
    out = f'BIGINT({int(display_width)})' if display_width else 'BIGINT'
    return out + _modifiers(nullable=nullable, default=default, unsigned=True)


def FLOAT(
    display_decimals: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[float] = None,
) -> str:
    """
    FLOAT type specification.

    Parameters
    ----------
    display_decimals : int, optional
        Number of decimal places to display
    nullable : bool, optional
        Can the value be NULL?
    default : float, optional
        Default value

    Returns
    -------
    str

    """
    out = f'FLOAT({int(display_decimals)})' if display_decimals else 'FLOAT'
    return out + _modifiers(nullable=nullable, default=default)


def DOUBLE(
    display_decimals: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[float] = None,
) -> str:
    """
    DOUBLE type specification.

    Parameters
    ----------
    display_decimals : int, optional
        Number of decimal places to display
    nullable : bool, optional
        Can the value be NULL?
    default : float, optional
        Default value

    Returns
    -------
    str

    """
    out = f'DOUBLE({int(display_decimals)})' if display_decimals else 'DOUBLE'
    return out + _modifiers(nullable=nullable, default=default)


def REAL(
    display_decimals: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[float] = None,
) -> str:
    """
    REAL type specification.

    Parameters
    ----------
    display_decimals : int, optional
        Number of decimal places to display
    nullable : bool, optional
        Can the value be NULL?
    default : float, optional
        Default value

    Returns
    -------
    str

    """
    out = f'REAL({int(display_decimals)})' if display_decimals else 'REAL'
    return out + _modifiers(nullable=nullable, default=default)


def DECIMAL(
    precision: int,
    scale: int,
    *,
    nullable: bool = True,
    default: Optional[Union[str, decimal.Decimal]] = None,
) -> str:
    """
    DECIMAL type specification.

    Parameters
    ----------
    precision : int
        Decimal precision
    scale : int
        Decimal scale
    nullable : bool, optional
        Can the value be NULL?
    default : str or decimal.Decimal, optional
        Default value

    Returns
    -------
    str

    """
    return f'DECIMAL({int(precision)}, {int(scale)})' + \
           _modifiers(nullable=nullable, default=default)


def DEC(
    precision: int,
    scale: int,
    *,
    nullable: bool = True,
    default: Optional[Union[str, decimal.Decimal]] = None,
) -> str:
    """
    DEC type specification.

    Parameters
    ----------
    precision : int
        Decimal precision
    scale : int
        Decimal scale
    nullable : bool, optional
        Can the value be NULL?
    default : str or decimal.Decimal, optional
        Default value

    Returns
    -------
    str

    """
    return f'DEC({int(precision)}, {int(scale)})' + \
           _modifiers(nullable=nullable, default=default)


def FIXED(
    precision: int,
    scale: int,
    *,
    nullable: bool = True,
    default: Optional[Union[str, decimal.Decimal]] = None,
) -> str:
    """
    FIXED type specification.

    Parameters
    ----------
    precision : int
        Decimal precision
    scale : int
        Decimal scale
    nullable : bool, optional
        Can the value be NULL?
    default : str or decimal.Decimal, optional
        Default value

    Returns
    -------
    str

    """
    return f'FIXED({int(precision)}, {int(scale)})' + \
           _modifiers(nullable=nullable, default=default)


def NUMERIC(
    precision: int,
    scale: int,
    *,
    nullable: bool = True,
    default: Optional[Union[str, decimal.Decimal]] = None,
) -> str:
    """
    NUMERIC type specification.

    Parameters
    ----------
    precision : int
        Decimal precision
    scale : int
        Decimal scale
    nullable : bool, optional
        Can the value be NULL?
    default : str or decimal.Decimal, optional
        Default value

    Returns
    -------
    str

    """
    return f'NUMERIC({int(precision)}, {int(scale)})' + \
           _modifiers(nullable=nullable, default=default)


def DATE(
    *,
    nullable: bool = True,
    default: Optional[Union[str, datetime.date]] = None,
) -> str:
    """
    DATE type specification.

    Parameters
    ----------
    nullable : bool, optional
        Can the value be NULL?
    default : str or datetime.date, optional
        Default value

    Returns
    -------
    str

    """
    return 'DATE' + _modifiers(nullable=nullable, default=default)


def TIME(
    precision: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[Union[str, datetime.timedelta]] = None,
) -> str:
    """
    TIME type specification.

    Parameters
    ----------
    precision : int, optional
        Sub-second precision
    nullable : bool, optional
        Can the value be NULL?
    default : str or datetime.timedelta, optional
        Default value

    Returns
    -------
    str

    """
    out = f'TIME({int(precision)})' if precision else 'TIME'
    return out + _modifiers(nullable=nullable, default=default)


def DATETIME(
    precision: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[Union[str, datetime.datetime]] = None,
) -> str:
    """
    DATETIME type specification.

    Parameters
    ----------
    precision : int, optional
        Sub-second precision
    nullable : bool, optional
        Can the value be NULL?
    default : str or datetime.datetime, optional
        Default value

    Returns
    -------
    str

    """
    out = f'DATETIME({int(precision)})' if precision else 'DATETIME'
    return out + _modifiers(nullable=nullable, default=default)


def TIMESTAMP(
    precision: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[Union[str, datetime.datetime]] = None,
) -> str:
    """
    TIMESTAMP type specification.

    Parameters
    ----------
    precision : int, optional
        Sub-second precision
    nullable : bool, optional
        Can the value be NULL?
    default : str or datetime.datetime, optional
        Default value

    Returns
    -------
    str

    """
    out = f'TIMESTAMP({int(precision)})' if precision else 'TIMESTAMP'
    return out + _modifiers(nullable=nullable, default=default)


def YEAR(*, nullable: bool = True, default: Optional[int] = None) -> str:
    """
    YEAR type specification.

    Parameters
    ----------
    nullable : bool, optional
        Can the value be NULL?
    default : int, optional
        Default value

    Returns
    -------
    str

    """
    return 'YEAR' + _modifiers(nullable=nullable, default=default)


def CHAR(
    length: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[str] = None,
    collate: Optional[str] = None,
    charset: Optional[str] = None,
) -> str:
    """
    CHAR type specification.

    Parameters
    ----------
    length : int, optional
        Maximum string length
    nullable : bool, optional
        Can the value be NULL?
    default : str, optional
        Default value
    collate : str, optional
        Collation
    charset : str, optional
        Character set

    Returns
    -------
    str

    """
    out = f'CHAR({int(length)})' if length else 'CHAR'
    return out + _modifiers(
        nullable=nullable, default=default,
        collate=collate, charset=charset,
    )


def VARCHAR(
    length: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[str] = None,
    collate: Optional[str] = None,
    charset: Optional[str] = None,
) -> str:
    """
    VARCHAR type specification.

    Parameters
    ----------
    length : int, optional
        Maximum string length
    nullable : bool, optional
        Can the value be NULL?
    default : str, optional
        Default value
    collate : str, optional
        Collation
    charset : str, optional
        Character set

    Returns
    -------
    str

    """
    out = f'VARCHAR({int(length)})' if length else 'VARCHAR'
    return out + _modifiers(
        nullable=nullable, default=default,
        collate=collate, charset=charset,
    )


def LONGTEXT(
    length: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[str] = None,
    collate: Optional[str] = None,
    charset: Optional[str] = None,
) -> str:
    """
    LONGTEXT type specification.

    Parameters
    ----------
    length : int, optional
        Maximum string length
    nullable : bool, optional
        Can the value be NULL?
    default : str, optional
        Default value
    collate : str, optional
        Collation
    charset : str, optional
        Character set

    Returns
    -------
    str

    """
    out = f'LONGTEXT({int(length)})' if length else 'LONGTEXT'
    return out + _modifiers(
        nullable=nullable, default=default,
        collate=collate, charset=charset,
    )


def MEDIUMTEXT(
    length: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[str] = None,
    collate: Optional[str] = None,
    charset: Optional[str] = None,
) -> str:
    """
    MEDIUMTEXT type specification.

    Parameters
    ----------
    length : int, optional
        Maximum string length
    nullable : bool, optional
        Can the value be NULL?
    default : str, optional
        Default value
    collate : str, optional
        Collation
    charset : str, optional
        Character set

    Returns
    -------
    str

    """
    out = f'MEDIUMTEXT({int(length)})' if length else 'MEDIUMTEXT'
    return out + _modifiers(
        nullable=nullable, default=default,
        collate=collate, charset=charset,
    )


def TEXT(
    length: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[str] = None,
    collate: Optional[str] = None,
    charset: Optional[str] = None,
) -> str:
    """
    TEXT type specification.

    Parameters
    ----------
    length : int, optional
        Maximum string length
    nullable : bool, optional
        Can the value be NULL?
    default : str, optional
        Default value
    collate : str, optional
        Collation
    charset : str, optional
        Character set

    Returns
    -------
    str

    """
    out = f'TEXT({int(length)})' if length else 'TEXT'
    return out + _modifiers(
        nullable=nullable, default=default,
        collate=collate, charset=charset,
    )


def TINYTEXT(
    length: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[str] = None,
    collate: Optional[str] = None,
    charset: Optional[str] = None,
) -> str:
    """
    TINYTEXT type specification.

    Parameters
    ----------
    length : int, optional
        Maximum string length
    nullable : bool, optional
        Can the value be NULL?
    default : str, optional
        Default value
    collate : str, optional
        Collation
    charset : str, optional
        Character set

    Returns
    -------
    str

    """
    out = f'TINYTEXT({int(length)})' if length else 'TINYTEXT'
    return out + _modifiers(
        nullable=nullable, default=default,
        collate=collate, charset=charset,
    )


def BINARY(
    length: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[bytes] = None,
    collate: Optional[str] = None,
) -> str:
    """
    BINARY type specification.

    Parameters
    ----------
    length : int, optional
        Maximum string length
    nullable : bool, optional
        Can the value be NULL?
    default : str, optional
        Default value
    collate : str, optional
        Collation

    Returns
    -------
    str

    """
    out = f'BINARY({int(length)})' if length else 'BINARY'
    return out + _modifiers(
        nullable=nullable, default=default, collate=collate,
    )


def VARBINARY(
    length: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[bytes] = None,
    collate: Optional[str] = None,
) -> str:
    """
    VARBINARY type specification.

    Parameters
    ----------
    length : int, optional
        Maximum string length
    nullable : bool, optional
        Can the value be NULL?
    default : str, optional
        Default value
    collate : str, optional
        Collation

    Returns
    -------
    str

    """
    out = f'VARBINARY({int(length)})' if length else 'VARBINARY'
    return out + _modifiers(
        nullable=nullable, default=default, collate=collate,
    )


def LONGBLOB(
    length: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[bytes] = None,
    collate: Optional[str] = None,
) -> str:
    """
    LONGBLOB type specification.

    Parameters
    ----------
    length : int, optional
        Maximum string length
    nullable : bool, optional
        Can the value be NULL?
    default : str, optional
        Default value
    collate : str, optional
        Collation

    Returns
    -------
    str

    """
    out = f'LONGBLOB({int(length)})' if length else 'LONGBLOB'
    return out + _modifiers(
        nullable=nullable, default=default, collate=collate,
    )


def MEDIUMBLOB(
    length: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[bytes] = None,
    collate: Optional[str] = None,
) -> str:
    """
    MEDIUMBLOB type specification.

    Parameters
    ----------
    length : int, optional
        Maximum string length
    nullable : bool, optional
        Can the value be NULL?
    default : str, optional
        Default value
    collate : str, optional
        Collation

    Returns
    -------
    str

    """
    out = f'MEDIUMBLOB({int(length)})' if length else 'MEDIUMBLOB'
    return out + _modifiers(
        nullable=nullable, default=default, collate=collate,
    )


def BLOB(
    length: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[bytes] = None,
    collate: Optional[str] = None,
) -> str:
    """
    BLOB type specification.

    Parameters
    ----------
    length : int, optional
        Maximum string length
    nullable : bool, optional
        Can the value be NULL?
    default : str, optional
        Default value
    collate : str, optional
        Collation

    Returns
    -------
    str

    """
    out = f'BLOB({int(length)})' if length else 'BLOB'
    return out + _modifiers(
        nullable=nullable, default=default, collate=collate,
    )


def TINYBLOB(
    length: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[bytes] = None,
    collate: Optional[str] = None,
) -> str:
    """
    TINYBLOB type specification.

    Parameters
    ----------
    length : int, optional
        Maximum string length
    nullable : bool, optional
        Can the value be NULL?
    default : str, optional
        Default value
    collate : str, optional
        Collation

    Returns
    -------
    str

    """
    out = f'TINYBLOB({int(length)})' if length else 'TINYBLOB'
    return out + _modifiers(
        nullable=nullable, default=default, collate=collate,
    )


def JSON(
    length: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[str] = None,
    collate: Optional[str] = None,
    charset: Optional[str] = None,
) -> str:
    """
    JSON type specification.

    Parameters
    ----------
    length : int, optional
        Maximum string length
    nullable : bool, optional
        Can the value be NULL?
    default : str, optional
        Default value
    collate : str, optional
        Collation
    charset : str, optional
        Character set

    Returns
    -------
    str

    """
    out = f'JSON({int(length)})' if length else 'JSON'
    return out + _modifiers(
        nullable=nullable, default=default,
        collate=collate, charset=charset,
    )


def GEOGRAPHYPOINT(*, nullable: bool = True, default: Optional[str] = None) -> str:
    """
    GEOGRAPHYPOINT type specification.

    Parameters
    ----------
    nullable : bool, optional
        Can the value be NULL?
    default : str, optional
        Default value

    Returns
    -------
    str

    """
    return 'GEOGRAPHYPOINT' + _modifiers(nullable=nullable, default=default)


def GEOGRAPHY(*, nullable: bool = True, default: Optional[str] = None) -> str:
    """
    GEOGRAPHYPOINT type specification.

    Parameters
    ----------
    nullable : bool, optional
        Can the value be NULL?
    default : str, optional
        Default value

    Returns
    -------
    str

    """
    return 'GEOGRAPHY' + _modifiers(nullable=nullable, default=default)


def RECORD(*args: Tuple[str, DataType], nullable: bool = True) -> str:
    """
    RECORD type specification.

    Parameters
    ----------
    *args : Tuple[str, DataType]
        Field specifications
    nullable : bool, optional
        Can the value be NULL?

    Returns
    -------
    str

    """
    assert len(args) > 0
    fields = []
    for name, value in args:
        if callable(value):
            fields.append(f'{escape_name(name)} {value()}')
        else:
            fields.append(f'{escape_name(name)} {value}')
    return f'RECORD({", ".join(fields)})' + _modifiers(nullable=nullable)


def ARRAY(dtype: DataType, nullable: bool = True) -> str:
    """
    ARRAY type specification.

    Parameters
    ----------
    dtype : DataType
        The data type of the array elements
    nullable : bool, optional
        Can the value be NULL?

    Returns
    -------
    str

    """
    if callable(dtype):
        dtype = dtype()
    return f'ARRAY({dtype})' + _modifiers(nullable=nullable)
