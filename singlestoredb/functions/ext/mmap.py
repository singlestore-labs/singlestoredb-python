#!/usr/bin/env python
"""
Module for creating collocated Python UDFs

This module implements the collocated form of external functions for
SingleStoreDB. This mode uses a socket for control communications
and memory mapped files for passing the data to and from the UDF.

The command below is a sample invocation. It exports all functions
within the `myfuncs` Python module that have a `@udf` decorator on
them. The `--db` option specifies a database connection string.
If this exists, the UDF application will connect to the database
and register all functions. The `--replace-existing` option indicates
that existing functions should be replaced::

    python -m singlestoredb.functions.ext.mmap \
        --db=root:@127.0.0.1:9306/cosmeticshop --replace-existing \
        myfuncs

The `myfuncs` package can be any Python package in your Python path.
It must contain functions marked with a `@udf` decorator and the
types must be annotated or specified using the `@udf` decorator
similar to the following::

    from singlestoredb.functions import udf

    @udf
    def print_it(x2: float, x3: str) -> str:
        return int(x2) * x3

    @udf.pandas
    def print_it_pandas(x2: float, x3: str) -> str:
        return x2.astype(np.int64) * x3.astype(str)

With the functions registered, you can now run the UDFs::

    SELECT print_it(3.14, 'my string');
    SELECT print_it_pandas(3.14, 'my string');

"""
import argparse
import array
import asyncio
import io
import logging
import mmap
import multiprocessing
import os
import secrets
import socket
import struct
import sys
import tempfile
import threading
import traceback
import urllib
import zipfile
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from . import asgi
from . import utils
from ... import manage_workspaces
from ...config import get_option


logger = utils.get_logger('singlestoredb.functions.ext.mmap')


def _handle_request(app: Any, connection: Any, client_address: Any) -> None:
    """
    Handle function call request.

    Parameters:
    app : ASGI app
        An ASGI application from the singlestoredb.functions.ext.asgi module
    connection : socket connection
        Socket connection for function control messages
    client_address : string
        Address of connecting client

    """
    logger.info('connection from {}'.format(str(connection).split(', ')[0][-4:]))

    # Receive the request header.  Format:
    #   server version:          uint64
    #   length of function name: uint64
    buf = connection.recv(16)
    version, namelen = struct.unpack('<qq', buf)

    # Python's recvmsg returns a tuple.  We only really care about the first
    # two parts.  The recvmsg call has a weird way of specifying the size for
    # the file descriptor array; basically, we're indicating we want to read
    # two 32-bit ints (for the input and output files).
    fd_model = array.array('i', [0, 0])
    msg, ancdata, flags, addr = connection.recvmsg(
        namelen,
        socket.CMSG_LEN(2 * fd_model.itemsize),
    )
    assert len(ancdata) == 1

    # The function's name will be in the "message" area of the recvmsg response.
    # It will be populated with `namelen` bytes.
    name = msg.decode('utf8')

    # Two file descriptors are transferred to us from the database via the
    # `sendmsg` protocol.  These are for reading the input rows and writing
    # the output rows, respectively.
    fd0, fd1 = struct.unpack('<ii', ancdata[0][2])
    ifile = os.fdopen(fd0, 'rb')
    ofile = os.fdopen(fd1, 'wb')

    # Keep receiving data on this socket until we run out.
    while True:

        # Read in the length of this row, a uint64.  No data means we're done
        # receiving.
        buf = connection.recv(8)
        if not buf:
            break
        length = struct.unpack('<q', buf)[0]
        if not length:
            break

        # Map in the input shared memory segment from the fd we received via
        # recvmsg.
        mem = mmap.mmap(
            ifile.fileno(),
            length,
            mmap.MAP_SHARED,
            mmap.PROT_READ,
        )

        # Read row data
        response_size = 0
        out = io.BytesIO()

        ifile.seek(0)
        try:
            # Run the function
            asyncio.run(
                app.call(
                    name,
                    io.BytesIO(ifile.read(length)),
                    out,
                    data_format='rowdat_1',
                    data_version='1.0',
                ),
            )

            # Write results
            buf = out.getbuffer()
            response_size = len(buf)
            ofile.truncate(max(128*1024, response_size))
            ofile.seek(0)
            ofile.write(buf)
            ofile.flush()

            # Complete the request by send back the status as two uint64s on the
            # socket:
            #     - http status
            #     - size of data in output shared memory
            connection.send(struct.pack('<qq', 200, response_size))

        except Exception as exc:
            errmsg = f'error occurred in executing function `{name}`: {exc}\n'
            logger.error(errmsg.rstrip())
            for line in traceback.format_exception(exc):  # type: ignore
                logger.error(line.rstrip())
            connection.send(
                struct.pack(
                    f'<qq{len(errmsg)}s', 500,
                    len(errmsg), errmsg.encode('utf8'),
                ),
            )
            break

        finally:
            # Close the shared memory object.
            mem.close()

    # Close shared memory files.
    ifile.close()
    ofile.close()

    # Close the connection
    connection.close()


def main(argv: Optional[List[str]] = None) -> None:
    """
    Main program for collocated Python UDFs

    Parameters
    ----------
    argv : List[str], optional
        List of command-line parameters

    """
    tmpdir = None
    functions = []
    defaults: Dict[str, Any] = {}
    for i in range(2):
        parser = argparse.ArgumentParser(
            prog='python -m singlestoredb.functions.ext.mmap',
            description='Run a collacated Python UDF server',
        )
        parser.add_argument(
            '--max-connections', metavar='n', type=int,
            default=get_option('external_function.max_connections'),
            help='maximum number of server connections before refusing them',
        )
        parser.add_argument(
            '--single-thread', action='store_true',
            default=get_option('external_function.single_thread'),
            help='should the server run in single-thread mode?',
        )
        parser.add_argument(
            '--socket-path', metavar='file-path',
            default=(
                get_option('external_function.socket_path') or
                os.path.join(tempfile.gettempdir(), secrets.token_hex(16))
            ),
            help='path to communications socket',
        )
        parser.add_argument(
            '--db', metavar='conn-str',
            default=os.environ.get('SINGLESTOREDB_URL', ''),
            help='connection string to use for registering functions',
        )
        parser.add_argument(
            '--replace-existing', action='store_true',
            help='should existing functions of the same name '
                 'in the database be replaced?',
        )
        parser.add_argument(
            '--log-level', metavar='[info|debug|warning|error]',
            default=get_option('external_function.log_level'),
            help='logging level',
        )
        parser.add_argument(
            '--process-mode', metavar='[thread|subprocess]',
            default=get_option('external_function.process_mode'),
            help='how to handle concurrent handlers',
        )
        parser.add_argument(
            'functions', metavar='module.or.func.path', nargs='*',
            help='functions or modules to export in UDF server',
        )

        args = parser.parse_args(argv)

        logger.setLevel(getattr(logging, args.log_level.upper()))

        if i > 0:
            break

        # Download Stage files as needed
        for i, f in enumerate(args.functions):
            if f.startswith('stage://'):
                url = urllib.parse.urlparse(f)
                if not url.path or url.path == '/':
                    raise ValueError(f'no stage path was specified: {f}')
                if url.path.endswith('/'):
                    raise ValueError(f'an environment file must be specified: {f}')

                mgr = manage_workspaces()
                if url.hostname:
                    wsg = mgr.get_workspace_group(url.hostname)
                elif os.environ.get('SINGLESTOREDB_WORKSPACE_GROUP'):
                    wsg = mgr.get_workspace_group(
                        os.environ['SINGLESTOREDB_WORKSPACE_GROUP'],
                    )
                else:
                    raise ValueError(f'no workspace group specified: {f}')

                if tmpdir is None:
                    tmpdir = tempfile.TemporaryDirectory()

                local_path = os.path.join(tmpdir.name, url.path.split('/')[-1])
                wsg.stage.download_file(url.path, local_path)
                args.functions[i] = local_path

            elif f.startswith('http://') or f.startswith('https://'):
                if tmpdir is None:
                    tmpdir = tempfile.TemporaryDirectory()

                local_path = os.path.join(tmpdir.name, f.split('/')[-1])
                urllib.request.urlretrieve(f, local_path)
                args.functions[i] = local_path

        # See if any of the args are zip files (assume they are environment files)
        modules = [(x, zipfile.is_zipfile(x)) for x in args.functions]
        envs = [x[0] for x in modules if x[1]]
        others = [x[0] for x in modules if not x[1]]

        if envs and len(envs) > 1:
            raise RuntimeError('only one environment file may be specified.')

        if envs and others:
            raise RuntimeError('environment files and other modules can not be mixed.')

        # See if an environment file was specified. If so, use those settings
        # as the defaults and reprocess command line.
        if envs:
            # Add zip file to the Python path
            sys.path.insert(0, envs[0])
            functions = [os.path.splitext(os.path.basename(envs[0]))[0]]

            # Add pyproject.toml variables and redo command-line processing
            defaults = utils.read_config(
                envs[0],
                ['tool.external_function', 'tool.external-function.collocated'],
            )
            if defaults:
                continue

    args.functions = functions or args.functions or None
    args.replace_existing = args.replace_existing \
        or defaults.get('replace_existing') \
        or get_option('external_function.replace_existing')

    if os.path.exists(args.socket_path):
        try:
            os.unlink(args.socket_path)
        except (IOError, OSError):
            raise RuntimeError(
                f'could not remove existing socket path: {args.socket_path}',
            )

    # Create application from functions / module
    app = asgi.create_app(
        functions=args.functions,
        url=args.socket_path,
        data_format='rowdat_1',
        app_mode='collocated',
    )

    funcs = app.get_create_functions(replace=args.replace_existing)
    if not funcs:
        raise RuntimeError('no functions specified')

    for f in funcs:
        logger.info(f'function: {f}')

    # Create the Unix socket server.
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

    # Bind our server to the path.
    server.bind(args.socket_path)

    logger.info(f'using socket path: {args.socket_path}')

    # Listen for incoming connections. Argument is the number of connections to
    # keep in the backlog before we begin refusing them; 32 is plenty for this
    # simple case.
    server.listen(args.max_connections)

    try:
        # Register functions with database
        if args.db:
            logger.info('registering functions with database')
            app.register_functions(args.db, replace=args.replace_existing)

        # Accept connections forever.
        while True:
            # Listen for the next connection on our port.
            connection, client_address = server.accept()

            if args.process_mode == 'thread':
                tcls = threading.Thread
            else:
                tcls = multiprocessing.Process  # type: ignore

            t = tcls(
                target=_handle_request,
                args=(app, connection, client_address),
            )

            t.start()

            # NOTE: The following line forces this process to handle requests
            # serially. This makes it easier to understand what's going on.
            # In real life, though, parallel is much faster. To use parallel
            # handling, just comment out the next line.
            if args.single_thread:
                t.join()

    except KeyboardInterrupt:
        return

    finally:
        if args.db:
            logger.info('dropping functions from database')
            app.drop_functions(args.db)

        # Remove the socket file before we exit.
        try:
            os.unlink(args.socket_path)
        except (IOError, OSError):
            logger.error(f'could not remove socket path: {args.socket_path}')


if __name__ == '__main__':
    try:
        main()
    except RuntimeError as exc:
        logger.error(str(exc))
        sys.exit(1)
    except KeyboardInterrupt:
        pass
