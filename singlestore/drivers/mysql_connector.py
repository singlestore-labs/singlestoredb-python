from __future__ import annotations

from typing import Any
from typing import Dict

try:
    from mysql.connector.conversion import MySQLConverter
    from mysql.connector.constants import FieldFlag
    from mysql.connector.constants import FieldType
except ImportError:
    FieldFlag = 0

    class FieldType(object):  # type: ignore
        """Dummy class."""
    class MySQLConverter(object):  # type: ignore
        """Dummy class."""

from .base import Driver
from ..converters import converters as conv


maybe_blobs = set([249, 250, 251, 252, 253, 254])


class Converter(MySQLConverter):

    def to_python(self, vtype: tuple[Any, ...], value: Any) -> Any:
        """Convert value bytearray value to Python object."""
        if value is None:
            return None
        if value == 0 and vtype[1] != FieldType.BIT:
            return None
        if vtype[1] == FieldType.BIT or \
                (vtype[7] & FieldFlag.BINARY and vtype[1] in maybe_blobs):
            #           print('binary', vtype, value)
            return conv[vtype[1]](value)
#       print('text', vtype, value)
        return conv[vtype[1]](value.decode(self.charset))


class MySQLConnectorDriver(Driver):

    name = 'mysql.connector'

    pkg_name = 'mysql.connector'
    pypi = 'mysql-connector-python'
    anaconda = 'mysql-connector-python'

    def remap_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        params.pop('driver', None)
        params.pop('odbc_driver', None)
        if params.pop('pure_python', False):
            params['use_pure'] = True
        params['port'] = params['port'] or 3306
        params['allow_local_infile'] = params.pop('local_infile')
        params['raw'] = params.pop('raw_values')
        params['converter_class'] = Converter
        return params

    def is_connected(self, conn: Any, reconnect: bool = False) -> bool:
        try:
            conn.ping(reconnect=reconnect)
            return True
        except conn.InterfaceError:
            return False
