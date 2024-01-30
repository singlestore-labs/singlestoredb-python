#!/usr/bin/env python
# type: ignore
from __future__ import annotations

import os
import sys
import time
import uuid
from optparse import OptionParser

import singlestoredb as s2


# Handle command-line options
usage = 'usage: %prog [options] sql-file'
parser = OptionParser(usage=usage, add_help_option=False)
parser.add_option(
    '-h', '--host', default='127.0.0.1',
    help='database hostname or IP address',
)
parser.add_option(
    '-P', '--port', type='int', default=3306,
    help='database port',
)
parser.add_option(
    '--password',
    help='user password',
)
parser.add_option(
    '-u', '--user',
    help='username',
)
parser.add_option(
    '-d', '--database',
    help='database name to use',
)
parser.add_option(
    '-H', '--http-port', type='int',
    help='enable HTTP API on given port',
)
parser.add_option(
    '--help',
    help='display usage information',
)

(options, args) = parser.parse_args()

if len(args) != 1 or options.help:
    parser.print_help()
    sys.exit(1)

sql_file = args[0]
if sql_file and not os.path.isfile(sql_file):
    print('ERROR: Could not locate SQL file: {sql_file}', file=sys.stderr)
    sys.exit(1)

database = options.database
if not database:
    database = 'TEMP_{}'.format(uuid.uuid4()).replace('-', '_')

tries = 25
while True:

    try:
        with s2.connect(
            f'mysql://{options.host}:{options.port}',
            user=options.user, password=options.password,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(f'CREATE DATABASE IF NOT EXISTS {database};')
                cur.execute(f'USE {database};')
                if options.http_port:
                    conn.enable_data_api(int(options.http_port))
                with open(sql_file, 'r') as infile:
                    for cmd in infile.read().split(';\n'):
                        cmd = cmd.strip()
                        if cmd:
                            cmd += ';'
                            cur.execute(cmd)
        break

    except Exception as exc:
        print(f'WARNING: {exc}')
        time.sleep(30)
        tries -= 1
        if tries < 0:
            raise
