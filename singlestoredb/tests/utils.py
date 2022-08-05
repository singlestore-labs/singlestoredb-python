#!/usr/bin/env python
# type: ignore
"""Utilities for testing."""
from __future__ import annotations

import os
import uuid
from urllib.parse import urlparse

import singlestoredb as s2
from singlestoredb.connection import build_params


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

    # If no database name was specified, use initializer URL if given.
    # HTTP can't change databases, so you can't initialize from HTTP
    # while also creating a database.
    args = {}
    if not dbname and 'SINGLESTOREDB_INIT_DB_URL' in os.environ:
        args['host'] = os.environ['SINGLESTOREDB_INIT_DB_URL']

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

    # Always use the default driver since not all operations are
    # permitted in the HTTP API.
    with open(sql_file, 'r') as infile:
        with s2.connect(**args) as conn:
            with conn.cursor() as cur:
                if not dbname:
                    dbname = 'TEST_{}'.format(uuid.uuid4()).replace('-', '_')
                    cur.execute(f'CREATE DATABASE {dbname};')
                    cur.execute(f'USE {dbname};')

                    # Execute lines in SQL.
                    for cmd in infile.read().split(';\n'):
                        cmd = cmd.strip()
                        if cmd:
                            cmd += ';'
                            cur.execute(cmd)

                # Start HTTP server as needed.
                if http_port:
                    cur.execute(f'SET GLOBAL HTTP_PROXY_PORT={http_port};')
                    cur.execute('SET GLOBAL HTTP_API=ON;')
                    cur.execute('RESTART PROXY;')

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
