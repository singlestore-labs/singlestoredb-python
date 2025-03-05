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
import logging
import singlestoredb as s2


try:
    import faulthandler
except ImportError:
    pass
else:
    faulthandler.enable()

log = logging.getLogger(__name__)

# def init_logging(debug=False):
#     formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(threadName)s: '
#                                   '[%(name)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
#     handler = logging.StreamHandler()
#     handler.setFormatter(formatter)
#     root_logger = logging.getLogger()
#     if debug:
#         handler.setLevel(logging.DEBUG)
#         root_logger.setLevel(logging.DEBUG)
#     else:
#         handler.setLevel(logging.INFO)
#         root_logger.setLevel(logging.INFO)
#     root_logger.addHandler(handler)

def daemonize() -> None:
    devnull = os.open(os.devnull, os.O_RDWR)
    os.dup2(devnull, sys.stdin.fileno())
    os.dup2(devnull, sys.stdout.fileno())
    os.dup2(devnull, sys.stderr.fileno())

def parse_args():
    '''Parse command line'''

    parser = ArgumentParser()

    parser.add_argument('mountpoint', type=str,
                        help='Where to mount the file system')
    return parser.parse_args()

def mountStage(access_token, base_url, workspaceGroupID, mountpoint):
    os.makedirs(f"{mountpoint}/stage/{workspaceGroupID}", exist_ok=True)
    workspaceManager = s2.manage_workspaces(access_token, base_url=base_url)
    wg = workspaceManager.get_workspace_group(workspaceGroupID)
    wg.stage.mount(f"{mountpoint}/stage/{workspaceGroupID}")

def main():
    options = parse_args()

    access_token = os.getenv("API_KEY")
    base_url = os.getenv("API_BASEURL")
    if not access_token:
        print("API_KEY not set")
        sys.exit(1)
    if not base_url:
        print("API_BASEURL not set")
        sys.exit(1)

    fileManager = s2.manage_files(access_token, base_url=base_url)

    # Mount personal notebooks
    os.makedirs(f"{options.mountpoint}/personal", exist_ok=True)
    print("Mounting personal")
    if os.fork() == 0:
        fileManager.personal_space.mount(f"{options.mountpoint}/personal")
        os._exit(0)
    
    # Mount shared notebooks
    os.makedirs(f"{options.mountpoint}/shared", exist_ok=True)
    print("Mounting shared")
    if os.fork() == 0:
        fileManager.shared_space.mount(f"{options.mountpoint}/shared")
        os._exit(0)

    # Mount stage for each workspace group
    workspaceManager = s2.manage_workspaces(access_token, base_url=base_url)
    for workspaceGroupID in [wg.id for wg in workspaceManager.workspace_groups]:
        os.makedirs(f"{options.mountpoint}/stage/{workspaceGroupID}", exist_ok=True)
        print(f"Mounting stage/{workspaceGroupID}")
        if os.fork() == 0:
            wg = s2.manage_workspaces(access_token, base_url=base_url).get_workspace_group(workspaceGroupID)
            wg.stage.mount(f"{options.mountpoint}/stage/{wg.id}")
            os._exit(0)

if __name__ == '__main__':
    main()

