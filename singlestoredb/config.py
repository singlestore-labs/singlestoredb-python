#!/usr/bin/env python
"""SingleStoreDB package options."""
import functools

from . import auth
from .utils.config import check_bool  # noqa: F401
from .utils.config import check_dict_str_str  # noqa: F401
from .utils.config import check_float  # noqa: F401
from .utils.config import check_int  # noqa: F401
from .utils.config import check_optional_bool  # noqa: F401
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
    'driver', 'string', check_str, 'mysql',
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
    'pure_python', 'bool', check_optional_bool, None,
    'Should the driver use a pure Python implementation? If the value is '
    '`None`, the C extension will be used if it exists, and pure python '
    'will be used otherwise. If the value is `False`, the pure python '
    'implementation will be used. If the value is `True` and the C extension '
    'exists, it will be used. If the value is `True` and the C extension '
    'doesn\'t exist or can\'t be loaded, a `NotSupportedError` is raised.',
    environ='SINGLESTOREDB_PURE_PYTHON',
)

register_option(
    'charset', 'string', check_str, 'utf8',
    'Specifies the character set for the session.',
    environ='SINGLESTOREDB_CHARSET',
)

register_option(
    'encoding_errors', 'string', check_str, 'strict',
    'Specifies the error handling behavior for decoding string values.',
    environ='SINGLESTOREDB_ENCODING_ERRORS',
)

register_option(
    'local_infile', 'bool', check_bool, False,
    'Should it be possible to load local files?',
    environ='SINGLESTOREDB_LOCAL_INFILE',
)

register_option(
    'multi_statements', 'bool', check_bool, False,
    'Should it be possible use multiple statements in one query?',
    environ='SINGLESTOREDB_MULTI_STATEMENTS',
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
    'ssl_cipher', 'str', check_str, 'HIGH',
    'Sets the SSL cipher list',
    environ='SINGLESTOREDB_SSL_CIPHER',
)

register_option(
    'ssl_disabled', 'bool', check_bool, False,
    'Disable SSL usage',
    environ='SINGLESTOREDB_SSL_DISABLED',
)

register_option(
    'ssl_verify_cert', 'bool', check_optional_bool, None,
    'Verify the server\'s certificate',
    environ='SINGLESTOREDB_SSL_VERIFY_CERT',
)

register_option(
    'ssl_verify_identity', 'bool', check_optional_bool, None,
    'Verify the server\'s identity',
    environ='SINGLESTOREDB_SSL_VERIFY_IDENTITY',
)

register_option(
    'program_name', 'string', check_str, None,
    'Name of the program',
)

register_option(
    'conn_attrs', 'dict', check_dict_str_str, None,
    'Additional connection attributes for telemetry',
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

register_option(
    'autocommit', 'bool', check_bool, True,
    'Enable autocommits',
    environ='SINGLESTOREDB_AUTOCOMMIT',
)

register_option(
    'buffered', 'bool', check_bool, True,
    'Should query results be buffered before processing?',
    environ='SINGLESTOREDB_BUFFERED',
)

register_option(
    'connect_timeout', 'int', check_int, 10,
    'The timeout for connecting to the database in seconds. '
    '(default: 10, min: 1, max: 31536000)',
    environ='SINGLESTOREDB_CONNECT_TIMEOUT',
)

register_option(
    'nan_as_null', 'bool', check_bool, False,
    'Should NaN values be treated as NULLs in query parameter substitutions '
    'including uploaded data?',
    environ='SINGLESTOREDB_NAN_AS_NULL',
)

register_option(
    'inf_as_null', 'bool', check_bool, False,
    'Should Inf values be treated as NULLs in query parameter substitutions '
    'including uploaded data?',
    environ='SINGLESTOREDB_INF_AS_NULL',
)

register_option(
    'track_env', 'bool', check_bool, False,
    'Should connections track the SINGLESTOREDB_URL environment variable?',
    environ='SINGLESTOREDB_TRACK_ENV',
)

register_option(
    'fusion.enabled', 'bool', check_bool, False,
    'Should Fusion SQL queries be enabled?',
    environ='SINGLESTOREDB_FUSION_ENABLED',
)

#
# Query results options
#
register_option(
    'results.type', 'string',
    functools.partial(
        check_str,
        valid_values=[
            'tuple', 'tuples', 'namedtuple', 'namedtuples',
            'dict', 'dicts', 'structsequence', 'structsequences',
        ],
    ),
    'tuples',
    'What form should the query results take?',
    environ='SINGLESTOREDB_RESULTS_TYPE',
)

register_option(
    'results.arraysize', 'int', check_int, 1,
    'Number of result rows to download in `fetchmany` calls.',
    environ='SINGLESTOREDB_RESULTS_ARRAYSIZE',
)


#
# Workspace manager options
#
register_option(
    'management.token', 'string', check_str, None,
    'Specifies the authentication token for the management API.',
    environ=['SINGLESTOREDB_MANAGEMENT_TOKEN'],
)


#
# Debugging options
#
register_option(
    'debug.queries', 'bool', check_bool, False,
    'Print queries and parameters to stderr.',
    environ='SINGLESTOREDB_DEBUG_QUERIES',
)

register_option(
    'debug.connection', 'bool', check_bool, False,
    'Print connection tracing information.',
    environ='SINGLESTOREDB_DEBUG_CONNECTION',
)
