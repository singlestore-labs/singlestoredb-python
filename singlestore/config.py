#!/usr/bin/env python
"""SingleStore package options."""
from __future__ import annotations

import functools

from .utils.config import check_bool  # noqa: F401
from .utils.config import check_float  # noqa: F401
from .utils.config import check_int  # noqa: F401
from .utils.config import check_str  # noqa: F401
from .utils.config import check_url  # noqa: F401
from .utils.config import describe_option  # noqa: F401
from .utils.config import get_default  # noqa: F401
from .utils.config import get_option  # noqa: F401
from .utils.config import get_suboptions  # noqa: F401
from .utils.config import option_context  # noqa: F401
from .utils.config import options  # noqa: F401
from .utils.config import register_option  # noqa: F401
from .utils.config import reset_option  # noqa: F401
from .utils.config import set_option  # noqa: F401


#
# Connection options
#
register_option(
    'url', 'string', check_url, None,
    'Specifies the full connection URL just as in SQLAlchemy.',
    environ=['SINGLESTORE_URL', 'SINGLESTORE_DSN'],
)

register_option(
    'host', 'string', check_str, 'localhost',
    'Specifies the database host name or IP address.',
    environ='SINGLESTORE_HOST',
)

register_option(
    'port', 'int', check_int, 0,
    'Specifies the database port number.',
    environ='SINGLESTORE_PORT',
)

register_option(
    'user', 'string', check_str, None,
    'Specifies the database user name.',
    environ='SINGLESTORE_USER',
)

register_option(
    'password', 'string', check_str, None,
    'Specifies the database user password.',
    environ='SINGLESTORE_PASSWORD',
)

register_option(
    'driver', 'string', check_str, None,
    'Specifies the Python DB-API module to use for communicating'
    'with the database.',
    environ='SINGLESTORE_DRIVER',
)


#
# Query results options
#
register_option(
    'results.format', 'string',
    functools.partial(
        check_str,
        valid_values=[
            'tuple', 'namedtuple',
            'dict', 'dataframe',
        ],
    ),
    'tuple',
    'What form should the query results take?',
    environ='SINGLESTORE_RESULTS_FORMAT',
)
