#!/usr/bin/env python
# type: ignore
"""Utilities for testing."""
import glob
import logging
import os
import re
import uuid
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from urllib.parse import urlparse

import singlestoredb as s2
from singlestoredb.connection import build_params


logger = logging.getLogger(__name__)


def apply_template(content: str, vars: Dict[str, Any]) -> str:
    for k, v in vars.items():
        key = '{{%s}}' % k
        if key in content:
            content = content.replace(key, v)
    return content


def get_server_version(cursor: Any) -> Tuple[int, int]:
    """
    Get the server version as a (major, minor) tuple.

    Parameters
    ----------
    cursor : Cursor
        Database cursor to execute queries

    Returns
    -------
    (int, int)
        Tuple of (major_version, minor_version)
    """
    cursor.execute('SELECT @@memsql_version')
    version_str = cursor.fetchone()[0]
    # Parse version string like "9.1.2" or "9.1.2-abc123"
    version_parts = version_str.split('-')[0].split('.')
    major = int(version_parts[0])
    minor = int(version_parts[1]) if len(version_parts) > 1 else 0
    logger.info(f'Detected server version: {major}.{minor} (full: {version_str})')
    return (major, minor)


def find_version_specific_sql_files(base_dir: str) -> List[Tuple[int, int, str]]:
    """
    Find all version-specific SQL files in the given directory.

    Looks for files matching the pattern test_X_Y.sql where X is major
    version and Y is minor version.

    Parameters
    ----------
    base_dir : str
        Directory to search for SQL files

    Returns
    -------
    List[Tuple[int, int, str]]
        List of (major, minor, filepath) tuples sorted by version
    """
    pattern = os.path.join(base_dir, 'test_*_*.sql')
    files = []

    for filepath in glob.glob(pattern):
        filename = os.path.basename(filepath)
        # Match pattern: test_X_Y.sql
        match = re.match(r'test_(\d+)_(\d+)\.sql$', filename)
        if match:
            major = int(match.group(1))
            minor = int(match.group(2))
            files.append((major, minor, filepath))
            logger.debug(
                f'Found version-specific SQL file: {filename} '
                f'(v{major}.{minor})',
            )

    # Sort by version (major, minor)
    files.sort()
    return files


def load_version_specific_sql(
    cursor: Any,
    base_dir: str,
    server_version: Tuple[int, int],
    template_vars: Dict[str, Any],
) -> None:
    """
    Load version-specific SQL files based on server version.

    Parameters
    ----------
    cursor : Cursor
        Database cursor to execute queries
    base_dir : str
        Directory containing SQL files
    server_version : Tuple[int, int]
        Server version as (major, minor)
    template_vars : Dict[str, Any]
        Template variables to apply to SQL content
    """
    sql_files = find_version_specific_sql_files(base_dir)
    server_major, server_minor = server_version

    for file_major, file_minor, filepath in sql_files:
        # Load if server version >= file version
        if (
            server_major > file_major or
            (server_major == file_major and server_minor >= file_minor)
        ):
            logger.info(
                f'Loading version-specific SQL: {os.path.basename(filepath)} '
                f'(requires {file_major}.{file_minor}, '
                f'server is {server_major}.{server_minor})',
            )
            with open(filepath, 'r') as sql_file:
                for cmd in sql_file.read().split(';\n'):
                    cmd = apply_template(cmd.strip(), template_vars)
                    if cmd:
                        cmd += ';'
                        cursor.execute(cmd)
        else:
            logger.info(
                f'Skipping version-specific SQL: {os.path.basename(filepath)} '
                f'(requires {file_major}.{file_minor}, '
                f'server is {server_major}.{server_minor})',
            )


def load_sql(sql_file: str) -> str:
    """
    Load a file containing SQL code.

    Parameters
    ----------
    sql_file : str
        Name of the SQL file to load.

    Returns
    -------
    (str, bool)
        Name of database created for SQL file and a boolean indicating
        whether the database already existed (meaning that it should not
        be deleted when tests are finished).

    """
    dbname = None

    # Use an existing database name if given.
    if 'SINGLESTOREDB_URL' in os.environ:
        dbname = build_params(host=os.environ['SINGLESTOREDB_URL']).get('database')
    elif 'SINGLESTOREDB_HOST' in os.environ:
        dbname = build_params(host=os.environ['SINGLESTOREDB_HOST']).get('database')
    elif 'SINGLESTOREDB_DATABASE' in os.environ:
        dbname = os.environ['SINGLESTOREDB_DATBASE']

    # Use initializer URL if given for setup operations.
    # HTTP can't change databases or execute certain commands like SET GLOBAL,
    # so we always use the MySQL protocol URL for initialization.
    args = {'local_infile': True}
    if 'SINGLESTOREDB_INIT_DB_URL' in os.environ:
        args['host'] = os.environ['SINGLESTOREDB_INIT_DB_URL']
        logger.info(
            f'load_sql: Using SINGLESTOREDB_INIT_DB_URL for setup: '
            f'{os.environ["SINGLESTOREDB_INIT_DB_URL"]}',
        )

    http_port = 0
    if 'SINGLESTOREDB_URL' in os.environ:
        url = os.environ['SINGLESTOREDB_URL']
        if url.startswith('http:') or url.startswith('https:'):
            urlp = urlparse(url)
            if urlp.port:
                http_port = urlp.port

    if 'SINGLESTOREDB_HTTP_PORT' in os.environ:
        http_port = int(os.environ['SINGLESTOREDB_HTTP_PORT'])

    dbexisted = bool(dbname)

    template_vars = dict(DATABASE_NAME=dbname, TEST_PATH=os.path.dirname(sql_file))

    # Always use the default driver since not all operations are
    # permitted in the HTTP API.
    with open(sql_file, 'r') as infile:
        with s2.connect(**args) as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute('SET GLOBAL default_partitions_per_leaf=2')
                    cur.execute('SET GLOBAL log_file_size_partitions=1048576')
                    cur.execute('SET GLOBAL log_file_size_ref_dbs=1048576')
                except s2.OperationalError:
                    pass

                if not dbname:
                    dbname = 'TEST_{}'.format(uuid.uuid4()).replace('-', '_')
                    cur.execute(f'CREATE DATABASE {dbname};')
                    cur.execute(f'USE {dbname};')

                    template_vars['DATABASE_NAME'] = dbname

                    # Execute lines in SQL.
                    for cmd in infile.read().split(';\n'):
                        cmd = apply_template(cmd.strip(), template_vars)
                        if cmd:
                            cmd += ';'
                            cur.execute(cmd)

                elif not conn.driver.startswith('http'):
                    cur.execute(f'USE {dbname};')

                # Start HTTP server as needed.
                if http_port and not conn.driver.startswith('http'):
                    cur.execute(f'SET GLOBAL HTTP_PROXY_PORT={http_port};')
                    cur.execute('SET GLOBAL HTTP_API=ON;')
                    cur.execute('RESTART PROXY;')

                # Load version-specific SQL files (e.g., test_9_1.sql for 9.1+)
                try:
                    server_version = get_server_version(cur)
                    sql_dir = os.path.dirname(sql_file)
                    load_version_specific_sql(
                        cur,
                        sql_dir,
                        server_version,
                        template_vars,
                    )
                except Exception as e:
                    logger.warning(
                        f'Failed to load version-specific SQL files: {e}',
                    )

    return dbname, dbexisted


def drop_database(name: str) -> None:
    """Drop a database with the given name."""
    if name:
        args = {}
        if 'SINGLESTOREDB_INIT_DB_URL' in os.environ:
            args['host'] = os.environ['SINGLESTOREDB_INIT_DB_URL']
        with s2.connect(**args) as conn:
            with conn.cursor() as cur:
                cur.execute(f'DROP DATABASE {name};')


def create_user(name: str, password: str, dbname: str) -> None:
    """Create a user for the test database."""
    if name:
        args = {}
        if 'SINGLESTOREDB_INIT_DB_URL' in os.environ:
            args['host'] = os.environ['SINGLESTOREDB_INIT_DB_URL']
        with s2.connect(**args) as conn:
            with conn.cursor() as cur:
                cur.execute(f'DROP USER IF EXISTS {name};')
                cur.execute(f'CREATE USER "{name}"@"%" IDENTIFIED BY "{password}"')
                cur.execute(f'GRANT ALL ON {dbname}.* to "{name}"@"%"')


def drop_user(name: str) -> None:
    """Drop a database with the given name."""
    if name:
        args = {}
        if 'SINGLESTOREDB_INIT_DB_URL' in os.environ:
            args['host'] = os.environ['SINGLESTOREDB_INIT_DB_URL']
        with s2.connect(**args) as conn:
            with conn.cursor() as cur:
                cur.execute(f'DROP USER IF EXISTS {name};')
