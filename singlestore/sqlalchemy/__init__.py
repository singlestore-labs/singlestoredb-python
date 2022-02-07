#!/usr/bin/env python
"""SingleStore SQLAlchemy dialects."""
from __future__ import annotations

from sqlalchemy.dialects.mysql import aiomysql  # noqa
from sqlalchemy.dialects.mysql import asyncmy  # noqa
from sqlalchemy.dialects.mysql import base  # noqa
from sqlalchemy.dialects.mysql import cymysql  # noqa
from sqlalchemy.dialects.mysql import mariadbconnector  # noqa
from sqlalchemy.dialects.mysql import mysqlconnector  # noqa
from sqlalchemy.dialects.mysql import mysqldb  # noqa
from sqlalchemy.dialects.mysql import pymysql  # noqa
from sqlalchemy.dialects.mysql import pyodbc  # noqa
from sqlalchemy.dialects.mysql.base import BIGINT
from sqlalchemy.dialects.mysql.base import BINARY
from sqlalchemy.dialects.mysql.base import BIT
from sqlalchemy.dialects.mysql.base import BLOB
from sqlalchemy.dialects.mysql.base import BOOLEAN
from sqlalchemy.dialects.mysql.base import CHAR
from sqlalchemy.dialects.mysql.base import DATE
from sqlalchemy.dialects.mysql.base import DATETIME
from sqlalchemy.dialects.mysql.base import DECIMAL
from sqlalchemy.dialects.mysql.base import DOUBLE
from sqlalchemy.dialects.mysql.base import ENUM
from sqlalchemy.dialects.mysql.base import FLOAT
from sqlalchemy.dialects.mysql.base import INTEGER
from sqlalchemy.dialects.mysql.base import JSON
from sqlalchemy.dialects.mysql.base import LONGBLOB
from sqlalchemy.dialects.mysql.base import LONGTEXT
from sqlalchemy.dialects.mysql.base import MEDIUMBLOB
from sqlalchemy.dialects.mysql.base import MEDIUMINT
from sqlalchemy.dialects.mysql.base import MEDIUMTEXT
from sqlalchemy.dialects.mysql.base import NCHAR
from sqlalchemy.dialects.mysql.base import NUMERIC
from sqlalchemy.dialects.mysql.base import NVARCHAR
from sqlalchemy.dialects.mysql.base import REAL
from sqlalchemy.dialects.mysql.base import SET
from sqlalchemy.dialects.mysql.base import SMALLINT
from sqlalchemy.dialects.mysql.base import TEXT
from sqlalchemy.dialects.mysql.base import TIME
from sqlalchemy.dialects.mysql.base import TIMESTAMP
from sqlalchemy.dialects.mysql.base import TINYBLOB
from sqlalchemy.dialects.mysql.base import TINYINT
from sqlalchemy.dialects.mysql.base import TINYTEXT
from sqlalchemy.dialects.mysql.base import VARBINARY
from sqlalchemy.dialects.mysql.base import VARCHAR
from sqlalchemy.dialects.mysql.base import YEAR
from sqlalchemy.dialects.mysql.dml import Insert
from sqlalchemy.dialects.mysql.dml import insert
from sqlalchemy.dialects.mysql.expression import match

# default dialect
base.dialect = dialect = pymysql.dialect

__all__ = (
    'BIGINT',
    'BINARY',
    'BIT',
    'BLOB',
    'BOOLEAN',
    'CHAR',
    'DATE',
    'DATETIME',
    'DECIMAL',
    'DOUBLE',
    'ENUM',
    'FLOAT',
    'INTEGER',
    'INTEGER',
    'JSON',
    'LONGBLOB',
    'LONGTEXT',
    'MEDIUMBLOB',
    'MEDIUMINT',
    'MEDIUMTEXT',
    'NCHAR',
    'NVARCHAR',
    'NUMERIC',
    'SET',
    'SMALLINT',
    'REAL',
    'TEXT',
    'TIME',
    'TIMESTAMP',
    'TINYBLOB',
    'TINYINT',
    'TINYTEXT',
    'VARBINARY',
    'VARCHAR',
    'YEAR',
    'dialect',
    'insert',
    'Insert',
    'match',
)
