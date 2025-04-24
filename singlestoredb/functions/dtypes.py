#!/usr/bin/env python3
import base64
import datetime
import decimal
import re
from typing import Any
from typing import Callable
from typing import Optional
from typing import Union

from ..converters import converters
from ..mysql.converters import escape_item  # type: ignore
from ..utils.dtypes import DEFAULT_VALUES  # noqa
from ..utils.dtypes import NUMPY_TYPE_MAP  # noqa
from ..utils.dtypes import PANDAS_TYPE_MAP  # noqa
from ..utils.dtypes import POLARS_TYPE_MAP  # noqa
from ..utils.dtypes import PYARROW_TYPE_MAP  # noqa


DataType = Union[str, Callable[..., Any]]


class SQLString(str):
    """SQL string type."""
    name: Optional[str] = None


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
    return base64.b64decode(x)


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


def BOOL(
    *,
    nullable: bool = True,
    default: Optional[bool] = None,
    name: Optional[str] = None,
) -> SQLString:
    """
    BOOL type specification.

    Parameters
    ----------
    nullable : bool, optional
        Can the value be NULL?
    default : bool, optional
        Default value
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = SQLString('BOOL' + _modifiers(nullable=nullable, default=_bool(default)))
    out.name = name
    return out


def BOOLEAN(
    *,
    nullable: bool = True,
    default: Optional[bool] = None,
    name: Optional[str] = None,
) -> SQLString:
    """
    BOOLEAN type specification.

    Parameters
    ----------
    nullable : bool, optional
        Can the value be NULL?
    default : bool, optional
        Default value
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = SQLString('BOOLEAN' + _modifiers(nullable=nullable, default=_bool(default)))
    out.name = name
    return out


def BIT(
    *,
    nullable: bool = True,
    default: Optional[int] = None,
    name: Optional[str] = None,
) -> SQLString:
    """
    BIT type specification.

    Parameters
    ----------
    nullable : bool, optional
        Can the value be NULL?
    default : int, optional
        Default value
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = SQLString('BIT' + _modifiers(nullable=nullable, default=default))
    out.name = name
    return out


def TINYINT(
    display_width: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[int] = None,
    unsigned: bool = False,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'TINYINT({display_width})' if display_width else 'TINYINT'
    out = SQLString(
        out + _modifiers(nullable=nullable, default=default, unsigned=unsigned),
    )
    out.name = name
    return out


def TINYINT_UNSIGNED(
    display_width: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[int] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'TINYINT({display_width})' if display_width else 'TINYINT'
    out = SQLString(out + _modifiers(nullable=nullable, default=default, unsigned=True))
    out.name = name
    return out


def SMALLINT(
    display_width: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[int] = None,
    unsigned: bool = False,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'SMALLINT({display_width})' if display_width else 'SMALLINT'
    out = SQLString(
        out + _modifiers(nullable=nullable, default=default, unsigned=unsigned),
    )
    out.name = name
    return out


def SMALLINT_UNSIGNED(
    display_width: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[int] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'SMALLINT({display_width})' if display_width else 'SMALLINT'
    out = SQLString(out + _modifiers(nullable=nullable, default=default, unsigned=True))
    out.name = name
    return out


def MEDIUMINT(
    display_width: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[int] = None,
    unsigned: bool = False,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'MEDIUMINT({display_width})' if display_width else 'MEDIUMINT'
    out = SQLString(
        out + _modifiers(nullable=nullable, default=default, unsigned=unsigned),
    )
    out.name = name
    return out


def MEDIUMINT_UNSIGNED(
    display_width: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[int] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'MEDIUMINT({display_width})' if display_width else 'MEDIUMINT'
    out = SQLString(out + _modifiers(nullable=nullable, default=default, unsigned=True))
    out.name = name
    return out


def INT(
    display_width: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[int] = None,
    unsigned: bool = False,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'INT({display_width})' if display_width else 'INT'
    out = SQLString(
        out + _modifiers(nullable=nullable, default=default, unsigned=unsigned),
    )
    out.name = name
    return out


def INT_UNSIGNED(
    display_width: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[int] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'INT({display_width})' if display_width else 'INT'
    out = SQLString(out + _modifiers(nullable=nullable, default=default, unsigned=True))
    out.name = name
    return out


def INTEGER(
    display_width: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[int] = None,
    unsigned: bool = False,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'INTEGER({display_width})' if display_width else 'INTEGER'
    out = SQLString(
        out + _modifiers(nullable=nullable, default=default, unsigned=unsigned),
    )
    out.name = name
    return out


def INTEGER_UNSIGNED(
    display_width: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[int] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'INTEGER({display_width})' if display_width else 'INTEGER'
    out = SQLString(out + _modifiers(nullable=nullable, default=default, unsigned=True))
    out.name = name
    return out


def BIGINT(
    display_width: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[int] = None,
    unsigned: bool = False,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'BIGINT({display_width})' if display_width else 'BIGINT'
    out = SQLString(
        out + _modifiers(nullable=nullable, default=default, unsigned=unsigned),
    )
    out.name = name
    return out


def BIGINT_UNSIGNED(
    display_width: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[int] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'BIGINT({int(display_width)})' if display_width else 'BIGINT'
    out = SQLString(out + _modifiers(nullable=nullable, default=default, unsigned=True))
    out.name = name
    return out


def FLOAT(
    display_decimals: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[float] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'FLOAT({int(display_decimals)})' if display_decimals else 'FLOAT'
    out = SQLString(out + _modifiers(nullable=nullable, default=default))
    out.name = name
    return out


def DOUBLE(
    display_decimals: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[float] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'DOUBLE({int(display_decimals)})' if display_decimals else 'DOUBLE'
    out = SQLString(out + _modifiers(nullable=nullable, default=default))
    out.name = name
    return out


def REAL(
    display_decimals: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[float] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'REAL({int(display_decimals)})' if display_decimals else 'REAL'
    out = SQLString(out + _modifiers(nullable=nullable, default=default))
    out.name = name
    return out


def DECIMAL(
    precision: int,
    scale: int,
    *,
    nullable: bool = True,
    default: Optional[Union[str, decimal.Decimal]] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = SQLString(
        f'DECIMAL({int(precision)}, {int(scale)})' +
        _modifiers(nullable=nullable, default=default),
    )
    out.name = name
    return out


def DEC(
    precision: int,
    scale: int,
    *,
    nullable: bool = True,
    default: Optional[Union[str, decimal.Decimal]] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = SQLString(
        f'DEC({int(precision)}, {int(scale)})' +
        _modifiers(nullable=nullable, default=default),
    )
    out.name = name
    return out


def FIXED(
    precision: int,
    scale: int,
    *,
    nullable: bool = True,
    default: Optional[Union[str, decimal.Decimal]] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = SQLString(
        f'FIXED({int(precision)}, {int(scale)})' +
        _modifiers(nullable=nullable, default=default),
    )
    out.name = name
    return out


def NUMERIC(
    precision: int,
    scale: int,
    *,
    nullable: bool = True,
    default: Optional[Union[str, decimal.Decimal]] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = SQLString(
        f'NUMERIC({int(precision)}, {int(scale)})' +
        _modifiers(nullable=nullable, default=default),
    )
    out.name = name
    return out


def DATE(
    *,
    nullable: bool = True,
    default: Optional[Union[str, datetime.date]] = None,
    name: Optional[str] = None,
) -> SQLString:
    """
    DATE type specification.

    Parameters
    ----------
    nullable : bool, optional
        Can the value be NULL?
    default : str or datetime.date, optional
        Default value
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = SQLString('DATE' + _modifiers(nullable=nullable, default=default))
    out.name = name
    return out


def TIME(
    precision: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[Union[str, datetime.timedelta]] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'TIME({int(precision)})' if precision else 'TIME'
    out = SQLString(out + _modifiers(nullable=nullable, default=default))
    out.name = name
    return out


def DATETIME(
    precision: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[Union[str, datetime.datetime]] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'DATETIME({int(precision)})' if precision else 'DATETIME'
    out = SQLString(out + _modifiers(nullable=nullable, default=default))
    out.name = name
    return out


def TIMESTAMP(
    precision: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[Union[str, datetime.datetime]] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'TIMESTAMP({int(precision)})' if precision else 'TIMESTAMP'
    out = SQLString(out + _modifiers(nullable=nullable, default=default))
    out.name = name
    return out


def YEAR(
    *,
    nullable: bool = True,
    default: Optional[int] = None,
    name: Optional[str] = None,
) -> SQLString:
    """
    YEAR type specification.

    Parameters
    ----------
    nullable : bool, optional
        Can the value be NULL?
    default : int, optional
        Default value
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = SQLString('YEAR' + _modifiers(nullable=nullable, default=default))
    out.name = name
    return out


def CHAR(
    length: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[str] = None,
    collate: Optional[str] = None,
    charset: Optional[str] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'CHAR({int(length)})' if length else 'CHAR'
    out = SQLString(
        out + _modifiers(
            nullable=nullable, default=default,
            collate=collate, charset=charset,
        ),
    )
    out.name = name
    return out


def VARCHAR(
    length: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[str] = None,
    collate: Optional[str] = None,
    charset: Optional[str] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'VARCHAR({int(length)})' if length else 'VARCHAR'
    out = SQLString(
        out + _modifiers(
            nullable=nullable, default=default,
            collate=collate, charset=charset,
        ),
    )
    out.name = name
    return out


def LONGTEXT(
    length: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[str] = None,
    collate: Optional[str] = None,
    charset: Optional[str] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'LONGTEXT({int(length)})' if length else 'LONGTEXT'
    out = SQLString(
        out + _modifiers(
            nullable=nullable, default=default,
            collate=collate, charset=charset,
        ),
    )
    out.name = name
    return out


def MEDIUMTEXT(
    length: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[str] = None,
    collate: Optional[str] = None,
    charset: Optional[str] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'MEDIUMTEXT({int(length)})' if length else 'MEDIUMTEXT'
    out = SQLString(
        out + _modifiers(
            nullable=nullable, default=default,
            collate=collate, charset=charset,
        ),
    )
    out.name = name
    return out


def TEXT(
    length: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[str] = None,
    collate: Optional[str] = None,
    charset: Optional[str] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'TEXT({int(length)})' if length else 'TEXT'
    out = SQLString(
        out + _modifiers(
            nullable=nullable, default=default,
            collate=collate, charset=charset,
        ),
    )
    out.name = name
    return out


def TINYTEXT(
    length: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[str] = None,
    collate: Optional[str] = None,
    charset: Optional[str] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'TINYTEXT({int(length)})' if length else 'TINYTEXT'
    out = SQLString(
        out + _modifiers(
            nullable=nullable, default=default,
            collate=collate, charset=charset,
        ),
    )
    out.name = name
    return out


def BINARY(
    length: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[bytes] = None,
    collate: Optional[str] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'BINARY({int(length)})' if length else 'BINARY'
    out = SQLString(
        out + _modifiers(
            nullable=nullable, default=default, collate=collate,
        ),
    )
    out.name = name
    return out


def VARBINARY(
    length: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[bytes] = None,
    collate: Optional[str] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'VARBINARY({int(length)})' if length else 'VARBINARY'
    out = SQLString(
        out + _modifiers(
            nullable=nullable, default=default, collate=collate,
        ),
    )
    out.name = name
    return out


def LONGBLOB(
    length: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[bytes] = None,
    collate: Optional[str] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'LONGBLOB({int(length)})' if length else 'LONGBLOB'
    out = SQLString(
        out + _modifiers(
            nullable=nullable, default=default, collate=collate,
        ),
    )
    out.name = name
    return out


def MEDIUMBLOB(
    length: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[bytes] = None,
    collate: Optional[str] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'MEDIUMBLOB({int(length)})' if length else 'MEDIUMBLOB'
    out = SQLString(
        out + _modifiers(
            nullable=nullable, default=default, collate=collate,
        ),
    )
    out.name = name
    return out


def BLOB(
    length: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[bytes] = None,
    collate: Optional[str] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'BLOB({int(length)})' if length else 'BLOB'
    out = SQLString(
        out + _modifiers(
            nullable=nullable, default=default, collate=collate,
        ),
    )
    out.name = name
    return out


def TINYBLOB(
    length: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[bytes] = None,
    collate: Optional[str] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'TINYBLOB({int(length)})' if length else 'TINYBLOB'
    out = SQLString(
        out + _modifiers(
            nullable=nullable, default=default, collate=collate,
        ),
    )
    out.name = name
    return out


def JSON(
    length: Optional[int] = None,
    *,
    nullable: bool = True,
    default: Optional[str] = None,
    collate: Optional[str] = None,
    charset: Optional[str] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = f'JSON({int(length)})' if length else 'JSON'
    out = SQLString(
        out + _modifiers(
            nullable=nullable, default=default,
            collate=collate, charset=charset,
        ),
    )
    out.name = name
    return out


def GEOGRAPHYPOINT(
    *,
    nullable: bool = True,
    default: Optional[str] = None,
    name: Optional[str] = None,
) -> SQLString:
    """
    GEOGRAPHYPOINT type specification.

    Parameters
    ----------
    nullable : bool, optional
        Can the value be NULL?
    default : str, optional
        Default value
    name : str, optional
        Name of the column / parameter

    Returns
    -------
    SQLString

    """
    out = SQLString('GEOGRAPHYPOINT' + _modifiers(nullable=nullable, default=default))
    out.name = name
    return out


def GEOGRAPHY(
    *,
    nullable: bool = True,
    default: Optional[str] = None,
    name: Optional[str] = None,
) -> SQLString:
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
    out = SQLString('GEOGRAPHY' + _modifiers(nullable=nullable, default=default))
    out.name = name
    return out


# def RECORD(
#     *args: Tuple[str, DataType],
#     nullable: bool = True,
#     name: Optional[str] = None,
# ) -> SQLString:
#     """
#     RECORD type specification.
#
#     Parameters
#     ----------
#     *args : Tuple[str, DataType]
#         Field specifications
#     nullable : bool, optional
#         Can the value be NULL?
#     name : str, optional
#         Name of the column / parameter
#
#     Returns
#     -------
#     SQLString
#
#     """
#     assert len(args) > 0
#     fields = []
#     for name, value in args:
#         if callable(value):
#             fields.append(f'{escape_name(name)} {value()}')
#         else:
#             fields.append(f'{escape_name(name)} {value}')
#     out = SQLString(f'RECORD({", ".join(fields)})' + _modifiers(nullable=nullable))
#     out.name = name
#     return out


# def ARRAY(
#     dtype: DataType,
#     nullable: bool = True,
#     name: Optional[str] = None,
# ) -> SQLString:
#     """
#     ARRAY type specification.
#
#     Parameters
#     ----------
#     dtype : DataType
#         The data type of the array elements
#     nullable : bool, optional
#         Can the value be NULL?
#     name : str, optional
#         Name of the column / parameter
#
#     Returns
#     -------
#     SQLString
#
#     """
#     if callable(dtype):
#         dtype = dtype()
#     out = SQLString(f'ARRAY({dtype})' + _modifiers(nullable=nullable))
#     out.name = name
#     return out


# F32 = 'F32'
# F64 = 'F64'
# I8 = 'I8'
# I16 = 'I16'
# I32 = 'I32'
# I64 = 'I64'


# def VECTOR(
#     length: int,
#     element_type: str = F32,
#     *,
#     nullable: bool = True,
#     default: Optional[bytes] = None,
#     name: Optional[str] = None,
# ) -> SQLString:
#     """
#     VECTOR type specification.
#
#     Parameters
#     ----------
#     n : int
#         Number of elements in vector
#     element_type : str, optional
#         Type of the elements in the vector:
#         F32, F64, I8, I16, I32, I64
#     nullable : bool, optional
#         Can the value be NULL?
#     default : str, optional
#         Default value
#     name : str, optional
#         Name of the column / parameter
#
#     Returns
#     -------
#     SQLString
#
#     """
#     out = f'VECTOR({int(length)}, {element_type})'
#     out = SQLString(
#         out + _modifiers(
#             nullable=nullable, default=default,
#         ),
#     )
#     out.name = name
#     return out
