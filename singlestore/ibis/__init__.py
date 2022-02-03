from __future__ import annotations

import contextlib
import warnings
from typing import Any
from typing import Iterator
from typing import Optional

import ibis.expr.datatypes as dt
import ibis.expr.schema as sc
import ibis.expr.types as ir
import sqlalchemy
import sqlalchemy.dialects.mysql as singlestore
from ibis.backends.base.sql.alchemy import BaseAlchemyBackend

from .compiler import SingleStoreCompiler


class Backend(BaseAlchemyBackend):

    name = 'singlestore'
    compiler = SingleStoreCompiler

    def do_connect(
        self,
        url: Optional[str] = None,
        host: Optional[str] = 'localhost',
        user: Optional[str] = None,
        password: Optional[str] = None,
        port: Optional[int] = 3306,
        database: Optional[str] = None,
        driver: Optional[str] = 'mysqlconnector',
    ) -> None:
        """Create an Ibis client located at `user`:`password`@`host`:`port`
        connected to a SingleStore database named `database`.

        Parameters
        ----------
        host : string, default 'localhost'
        user : string, default None
        password : string, default None
        port : string or integer, default 3306
        database : string, default None
        url : string, default None
            Complete SQLAlchemy connection string. If passed, the other
            connection arguments are ignored.
        driver : string, default 'mysqlconnector'

        Examples
        --------
        >>> import os
        >>> import getpass
        >>> url = os.environ.get('IBIS_TEST_SINGLESTORE_URL')
        >>> host = os.environ.get('IBIS_TEST_SINGLESTORE_HOST', 'localhost')
        >>> port = int(os.environ.get('IBIS_TEST_SINGLESTORE_PORT', 3306))
        >>> user = os.environ.get('IBIS_TEST_SINGLESTORE_USER', getpass.getuser())
        >>> password = os.environ.get('IBIS_TEST_SINGLESTORE_PASSWORD')
        >>> database = os.environ.get('IBIS_TEST_SINGLESTORE_DATABASE',
        ...                           'ibis_testing')
        >>> con = connect(
        ...     url=url,
        ...     host=host,
        ...     port=port,
        ...     user=user,
        ...     password=password,
        ...     database=database
        ... )
        >>> con.list_tables()  # doctest: +ELLIPSIS
        [...]
        >>> t = con.table('functional_alltypes')
        >>> t
        SingleStoreTable[table]
          name: functional_alltypes
          schema:
            index : int64
            Unnamed: 0 : int64
            id : int32
            bool_col : int8
            tinyint_col : int8
            smallint_col : int16
            int_col : int32
            bigint_col : int64
            float_col : float32
            double_col : float64
            date_string_col : string
            string_col : string
            timestamp_col : timestamp
            year : int32
            month : int32
        """
        if url and '//' not in url:
            url = f'singlestore+{driver}://{url}'
        if url and 'singlestore+' not in url:
            url = f'singlestore+{url}'
        alchemy_url = self._build_alchemy_url(
            url=url,
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            driver=f'singlestore+{driver}',
        )

        self.database_name = alchemy_url.database
        super().do_connect(sqlalchemy.create_engine(alchemy_url))

    @contextlib.contextmanager
    def begin(self) -> Iterator[Any]:
        with super().begin() as bind:
            previous_timezone = bind.execute(
                'SELECT @@session.time_zone',
            ).scalar()
            try:
                bind.execute("SET @@session.time_zone = 'UTC'")
            except Exception as e:
                warnings.warn(f"Couldn't set singlestore timezone: {str(e)}")

            try:
                yield bind
            finally:
                query = "SET @@session.time_zone = '{}'"
                bind.execute(query.format(previous_timezone))

    def table(
        self,
        name: str,
        database: Optional[str] = None,
        schema: Optional[sc.Schema] = None,
    ) -> ir.TableExpr:
        """Create a table expression that references a particular a table
        called `name` in a SingleStore database called `database`.

        Parameters
        ----------
        name : str
            The name of the table to retrieve.
        database : str, optional
            The database in which the table referred to by `name` resides. If
            ``None`` then the ``current_database`` is used.
        schema : str, optional
            The schema in which the table resides.  If ``None`` then the
            `public` schema is assumed.

        Returns
        -------
        table : TableExpr
            A table expression.
        """
        if database is not None and database != self.current_database:
            return self.database(name=database).table(name=name, schema=schema)
        else:
            alch_table = self._get_sqla_table(name, schema=schema)
            node = self.table_class(alch_table, self, self._schemas.get(name))
            return self.table_expr_class(node)


@dt.dtype.register((singlestore.DOUBLE, singlestore.REAL))
def singlestore_double(satype: Any, nullable: bool = True) -> dt.DataType:
    return dt.Double(nullable=nullable)


@dt.dtype.register(singlestore.FLOAT)
def singlestore_float(satype: Any, nullable: bool = True) -> dt.DataType:
    return dt.Float(nullable=nullable)


@dt.dtype.register(singlestore.TINYINT)
def singlestore_tinyint(satype: Any, nullable: bool = True) -> dt.DataType:
    return dt.Int8(nullable=nullable)


@dt.dtype.register(singlestore.BLOB)
def singlestore_blob(satype: Any, nullable: bool = True) -> dt.DataType:
    return dt.Binary(nullable=nullable)
