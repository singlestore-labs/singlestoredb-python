#!/usr/bin/env python
# type: ignore
from __future__ import annotations

import os
import sys
import uuid
from optparse import OptionParser

import singlestoredb as s2


# Handle command-line options
usage = 'usage: %prog [options]'
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
    '--help',
    help='display usage information',
)

(options, args) = parser.parse_args()

if options.help:
    parser.print_help()
    sys.exit(1)

database = options.database.strip()
if not database:
    print('error: database name must be specified', file=sys.stderr)
    sys.exit(1)

with s2.connect(
    f'mysql://{options.host}:{options.port}',
    user=options.user, password=options.password,
) as conn:
    with conn.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS `{database}`;')
