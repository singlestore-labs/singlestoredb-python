#!/usr/bin/env python
# type: ignore
from __future__ import annotations

import os
import sys
from optparse import OptionParser

import singlestore as s2


# Handle command-line options
usage = 'usage: %prog [options] sql-file'
parser = OptionParser(usage=usage, add_help_option=False)
parser.add_option(
    '-h', '--host',
    help='database hostname or IP address',
)
parser.add_option(
    '-P', '--port',
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
    '-H', '--http-port',
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

with s2.connect(
    f'pymysql://{options.host}:{options.port}',
    user=options.user, password=options.password,
) as conn:
    with conn.cursor() as cur:
        if options.http_port:
            cur.execute(
                'SET GLOBAL HTTP_PROXY_PORT={};'.format(
                    int(options.http_port),
                ),
            )
            cur.execute('SET GLOBAL HTTP_API=ON;')
            cur.execute('RESTART PROXY;')
        with open(sql_file, 'r') as infile:
            for cmd in infile.read().split(';\n'):
                cmd = cmd.strip()
                if cmd:
                    cmd += ';'
                    cur.execute(cmd)
