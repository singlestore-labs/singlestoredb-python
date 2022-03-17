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
    'host', 'string', check_str, '127.0.0.1',
    'Specifies the database host name or IP address.',
    environ=['SINGLESTORE_HOST', 'SINGLESTORE_URL'],
)

register_option(
    'port', 'int', check_int, 0,
    'Specifies the database port number.',
    environ='SINGLESTORE_PORT',
)

register_option(
    'http_port', 'int', check_int, 0,
    'Specifies the database port number for the HTTP API.',
    environ='SINGLESTORE_HTTP_PORT',
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
    'driver', 'string', check_str, 'mysql-connector',
    'Specifies the Python DB-API module to use for communicating'
    'with the database.',
    environ='SINGLESTORE_DRIVER',
)

register_option(
    'database', 'string', check_str, None,
    'Name of the database to connect to.',
    environ='SINGLESTORE_DATABASE',
)

register_option(
    'pure_python', 'bool', check_bool, False,
    'Should the driver use a pure Python implementation?',
    environ='SINGLESTORE_PURE_PYTHON',
)

register_option(
    'charset', 'string', check_str, 'utf8',
    'Specifies the character set for the session.',
    environ='SINGLESTORE_CHARSET',
)

register_option(
    'local_infile', 'bool', check_bool, False,
    'Should it be possible to load local files?',
    environ='SINGLESTORE_LOCAL_INFILE',
)

register_option(
    'odbc_driver', 'str', check_str, 'SingleStore ODBC 1.0 Unicode Driver',
    'Name of the ODBC driver for ODBC connections',
    environ='SINGLESTORE_ODBC_DRIVER',
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


#
# Cluster manager options
#
register_option(
    'cluster_manager.token', 'string', check_str, None,
    'Specifies the authentication token for the cluster manager API.',
    environ=['SINGLESTORE_CLUSTER_MANAGER_TOKEN'],
)
