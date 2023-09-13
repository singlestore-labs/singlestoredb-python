#!/usr/bin/env python3
import os
import re
from typing import Any
from typing import Union

from .. import connection
from .. import manage_workspaces
from .handler import Handler
from .parser import SQLParser


def is_management_query(sql: Union[str, bytes]) -> bool:
    if isinstance(sql, (bytes, bytearray)):
        sql = sql.decode('utf-8')
    return bool(re.match(r'\s*((show|create)\s+(workspace|region))', sql, flags=re.I))


def execute(connection: connection.Connection, sql: str) -> Any:
    manager = manage_workspaces(os.environ['SINGLESTOREDB_MANAGEMENT_TOKEN'])
    parser = SQLParser(Handler(connection, manager))
    return parser.execute(sql)
