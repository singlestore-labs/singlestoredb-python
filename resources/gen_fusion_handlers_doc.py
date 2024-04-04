#!/usr/bin/env python3
# type: ignore
from __future__ import annotations

import os
from optparse import OptionParser

from singlestoredb.fusion.registry import _handlers

parser = OptionParser()
parser.add_option(
    '-d', '--dir',
    default='_gen_fusion_help',
    help='output directory',
)

(options, args) = parser.parse_args()

os.makedirs(options.dir)

for k, v in _handlers.items():
    out_filename = os.path.join(options.dir, k.replace(' ', '_').lower()) + '.md'
    with open(out_filename, 'w') as f:
        f.write(v.help)
