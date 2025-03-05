#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
hello.py - Example file system for pyfuse3.

This program presents a static file system containing a single file.

Copyright © 2015 Nikolaus Rath <Nikolaus.org>
Copyright © 2015 Gerion Entrup.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
'''

import os
import sys

from argparse import ArgumentParser
import singlestoredb as s2

try:
    import faulthandler
except ImportError:
    pass
else:
    faulthandler.enable()

def parse_args():
    '''Parse command line'''

    parser = ArgumentParser()

    parser.add_argument('mountpoint', type=str,
                        help='Where to mount the file system')
    return parser.parse_args()

def main():
    options = parse_args()

    access_token = os.getenv("API_KEY")
    base_url = os.getenv("API_BASEURL")
    workspaceGroupID = os.getenv("WORKSPACEGROUP_ID")
    if not access_token:
        print("API_KEY not set")
        sys.exit(1)
    if not base_url:
        print("API_BASEURL not set")
        sys.exit(1)
    if not workspaceGroupID:
        print("WORKSPACEGROUP_ID not set")
        sys.exit(1)

    # Mount stage for each workspace group
    wg = s2.manage_workspaces(access_token, base_url=base_url).get_workspace_group(workspaceGroupID)
    wg.stage.mount(options.mountpoint)

if __name__ == '__main__':
    main()
