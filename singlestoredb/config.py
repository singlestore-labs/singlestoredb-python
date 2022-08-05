#!/usr/bin/env python
"""SingleStoreDB package options."""
from __future__ import annotations

import functools

from . import auth
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
    environ=['SINGLESTOREDB_HOST', 'SINGLESTOREDB_URL'],
)

register_option(
    'port', 'int', check_int, 0,
    'Specifies the database port number.',
    environ='SINGLESTOREDB_PORT',
)

register_option(
    'http_port', 'int', check_int, 0,
    'Specifies the database port number for the HTTP API.',
    environ='SINGLESTOREDB_HTTP_PORT',
)

register_option(
    'user', 'string', check_str, None,
    'Specifies the database user name.',
    environ='SINGLESTOREDB_USER',
)

register_option(
    'password', 'string', check_str, None,
    'Specifies the database user password.',
    environ='SINGLESTOREDB_PASSWORD',
)

register_option(
    'driver', 'string', check_str, 'pymysql',
    'Specifies the Python DB-API module to use for communicating'
    'with the database.',
    environ='SINGLESTOREDB_DRIVER',
)

register_option(
    'database', 'string', check_str, None,
    'Name of the database to connect to.',
    environ='SINGLESTOREDB_DATABASE',
)

register_option(
    'pure_python', 'bool', check_bool, False,
    'Should the driver use a pure Python implementation?',
    environ='SINGLESTOREDB_PURE_PYTHON',
)

register_option(
    'charset', 'string', check_str, 'utf8',
    'Specifies the character set for the session.',
    environ='SINGLESTOREDB_CHARSET',
)

register_option(
    'local_infile', 'bool', check_bool, False,
    'Should it be possible to load local files?',
    environ='SINGLESTOREDB_LOCAL_INFILE',
)

register_option(
    'odbc_driver', 'str', check_str, 'SingleStore ODBC 1.0 Unicode Driver',
    'Name of the ODBC driver for ODBC connections',
    environ='SINGLESTOREDB_ODBC_DRIVER',
)

register_option(
    'ssl_key', 'str', check_str, None,
    'File containing SSL key',
    environ='SINGLESTOREDB_SSL_KEY',
)

register_option(
    'ssl_cert', 'str', check_str, None,
    'File containing SSL certificate',
    environ='SINGLESTOREDB_SSL_CERT',
)

register_option(
    'ssl_ca', 'str', check_str, None,
    'File containing SSL certificate authority',
    environ='SINGLESTOREDB_SSL_CA',
)

register_option(
    'ssl_disabled', 'bool', check_bool, False,
    'Disable SSL usage',
    environ='SINGLESTOREDB_SSL_DISABLED',
)

register_option(
    'credential_type', 'str',
    functools.partial(
        check_str, valid_values=[
            auth.PASSWORD,
            auth.JWT,
            auth.BROWSER_SSO,
        ],
    ),
    None,
    'Type of authentication method to use.',
    environ='SINGLESTOREDB_CREDENTIAL_TYPE',
)

register_option(
    'sso_browser', 'str', check_str, None,
    'Browser to use for single sign-on. This should be a web browser name '
    'registered with Python\'s webbrowser module.',
    environ='SINGLESTOREDB_SSO_BROWSER',
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
    environ='SINGLESTOREDB_RESULTS_FORMAT',
)

register_option(
    'results.arraysize', 'int', check_int, 100,
    'Number of result rows to download in `fetchmany` calls',
    environ='SINGLESTOREDB_RESULTS_ARRAYSIZE',
)


#
# Cluster manager options
#
register_option(
    'management.token', 'string', check_str, None,
    'Specifies the authentication token for the management API.',
    environ=['SINGLESTOREDB_MANAGEMENT_TOKEN'],
)
