#!/usr/bin/env python
# type: ignore
"""Utilities for testing."""
from __future__ import annotations

import os
import uuid

import singlestore as s2


def load_sql(sql_file: str) -> str:
    """
    Load a file containing SQL code.

    Parameters
    ----------
    sql_file : str
        Name of the SQL file to load.

    Returns
    -------
    str : name of database created for SQL file

    """
    dbname = 'TEST_{}'.format(uuid.uuid4()).replace('-', '_')

    # Always use the default driver since not all operations are
    # permitted in the HTTP API.
    with open(sql_file, 'r') as infile:
        with s2.connect(driver='mysql-connector') as conn:
            with conn.cursor() as cur:
                cur.execute(f'CREATE DATABASE {dbname};')
                cur.execute(f'USE {dbname};')

                # Start HTTP server as needed.
                if 'SINGLESTORE_HTTP_PORT' in os.environ:
                    cur.execute(
                        'SET GLOBAL HTTP_PROXY_PORT={};'
                        .format(os.environ['SINGLESTORE_HTTP_PORT']),
                    )
                    cur.execute('SET GLOBAL HTTP_API=ON;')
                    cur.execute('RESTART PROXY;')

                # Execute lines in SQL.
                for cmd in infile.read().split(';\n'):
                    cmd = cmd.strip()
                    if cmd:
                        cmd += ';'
                        cur.execute(cmd)

    return dbname


def drop_database(name: str) -> None:
    """Drop a database with the given name."""
    if name:
        with s2.connect(driver='mysql-connector') as conn:
            with conn.cursor() as cur:
                cur.execute(f'DROP DATABASE {name};')
