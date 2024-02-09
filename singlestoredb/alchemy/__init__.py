#!/usr/bin/env python3
import inspect
from typing import Any
from urllib.parse import quote

try:
    import sqlalchemy
    from sqlalchemy_singlestoredb import *  # noqa: F403, F401
    has_sqlalchemy = True
except ImportError:
    import warnings
    warnings.warn(
        'sqlalchemy_singlestoredb must be installed to use this module',
        RuntimeWarning,
    )
    has_sqlalchemy = False

from ..connection import build_params
from ..connection import connect


def create_engine(*args: Any, **kwargs: Any) -> Any:
    """
    Create an SQLAlchemy engine for SingleStoreDB.

    Parameters
    ----------
    **kwargs : Any
        The parameters taken here are the same as for
        `sqlalchemy.create_engine`. However, this function can be
        called without any parameters in order to inherit parameters
        set by environment variables or parameters set in by
        options in Python code.

    See Also
    --------
    `sqlalchemy.create_engine`

    Returns
    -------
    SQLAlchemy engine

    """
    if not has_sqlalchemy:
        raise RuntimeError('sqlalchemy_singlestoredb package is not installed')

    if len(args) > 1:
        raise ValueError(
            '`args` can only have a single element '
            'containing the database URL',
        )

    if args:
        kwargs['host'] = args[0]

    conn_params = {}
    sa_params = {}

    conn_args = inspect.getfullargspec(connect).args

    for key, value in kwargs.items():
        if key in conn_args:
            conn_params[key] = value
        else:
            sa_params[key] = value

    params = build_params(**conn_params)
    driver = params.pop('driver', None)
    host = params.pop('host')
    port = params.pop('port')
    user = params.pop('user', None)
    password = params.pop('password', None)
    database = params.pop('database', '')

    if not driver:
        driver = 'singlestoredb+mysql'
    elif not driver.startswith('singlestoredb'):
        driver = f'singlestoredb+{driver}'

    if user is not None and password is not None:
        url = f'{driver}://{quote(user)}:{quote(password)}@' \
              f'{host}:{port}/{quote(database)}'
    elif user is not None:
        url = f'{driver}://{quote(user)}@{host}:{port}/{quote(database)}'
    elif password is not None:
        url = f'{driver}://:{quote(password)}@{host}:{port}/{quote(database)}'
    else:
        url = f'{driver}://{host}:{port}/{quote(database)}'

    return sqlalchemy.create_engine(url, connect_args=params, **sa_params)
