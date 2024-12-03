import os
from typing import Any
from typing import Optional

from IPython.core.interactiveshell import InteractiveShell
from IPython.core.magic import cell_magic
from IPython.core.magic import line_magic
from IPython.core.magic import Magics
from IPython.core.magic import magics_class
from IPython.core.magic import needs_local_scope
from IPython.core.magic import no_var_expand
from sql.magic import SqlMagic
from sqlalchemy import create_engine
from sqlalchemy import Engine
from sqlalchemy import PoolProxiedConnection

DEFAULT_POOL_SIZE = 10  # Maximum number of connections in the pool
DEFAULT_MAX_OVERFLOW = 5  # additional connections (temporary overflow)
DEFAULT_POOL_TIMEOUT = 30  # Wait time for a connection from the pool


@magics_class
class SqlxMagic(Magics):
    def __init__(self, shell: InteractiveShell):
        Magics.__init__(self, shell=shell)
        self.magic = SqlMagic(shell)
        self.engine: Optional['Engine'] = None

    @no_var_expand
    @needs_local_scope
    @line_magic('sqlx')
    @cell_magic('sqlx')
    def sqlx(self, line: str, cell: str = '', local_ns: Any = None) -> Any:
        """
        Runs SQL statement against a database, specified by
        SQLAlchemy connect string present in DATABASE_URL environment variable.

        The magic can be used both as a cell magic `%%sqlx` and
        line magic `%sqlx` (see examples below).

        This is a thin wrapper around the [jupysql](https://jupysql.ploomber.io/) magic,
        allowing multi-threaded execution.
        A connection pool will be maintained internally.

        Examples::

          # Line usage

          %sqlx SELECT * FROM mytable

          result = %sqlx SELECT 1


          # Cell usage

          %%sqlx
          DELETE FROM mytable

          %%sqlx
          DROP TABLE mytable

        """

        connection = self.get_connection()
        try:
            result = self.magic.execute(line, cell, local_ns, connection)
        finally:
            connection.close()

        return result

    def get_connection(self) -> PoolProxiedConnection:
        if self.engine is None:
            if 'DATABASE_URL' not in os.environ:
                raise RuntimeError(
                    'Cannot create connection pool, environment variable'
                    " 'DATABASE_URL' is missing.",
                )

            # TODO: allow configuring engine
            # idea: %sqlx engine
            # idea: %%sqlx engine
            self.engine = create_engine(
                os.environ['DATABASE_URL'],
                pool_size=DEFAULT_POOL_SIZE,
                max_overflow=DEFAULT_MAX_OVERFLOW,
                pool_timeout=DEFAULT_POOL_TIMEOUT,
            )

        return self.engine.raw_connection()


# In order to actually use these magics, you must register them with a
# running IPython.


def load_ipython_extension(ip: InteractiveShell) -> None:
    """
    Any module file that define a function named `load_ipython_extension`
    can be loaded via `%load_ext module.path` or be configured to be
    autoloaded by IPython at startup time.
    """

    # Load jupysql extension
    # This is necessary for jupysql to initialize internal state
    # required to render messages
    assert ip.extension_manager is not None
    result = ip.extension_manager.load_extension('sql')
    if result == 'no load function':
        raise RuntimeError('Could not load sql extension. Is jupysql installed?')

    # Register sqlx
    ip.register_magics(SqlxMagic(ip))
