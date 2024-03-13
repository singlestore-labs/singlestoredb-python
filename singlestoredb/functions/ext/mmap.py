#!/usr/bin/env python
'''
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

'''
import argparse
import array
import asyncio
import io
import logging
import mmap
import os
import secrets
import socket
import struct
import sys
import tempfile
import threading
import traceback
from typing import Any

from . import asgi


logger = logging.getLogger('singlestoredb.functions.ext.mmap')
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def _handle_request(app: Any, connection: Any, client_address: Any) -> None:
    '''
    Handle function call request.

    Parameters:
    app : ASGI app
        An ASGI application from the singlestoredb.functions.ext.asgi module
    connection : socket connection
        Socket connection for function control messages
    client_address : string
        Address of connecting client

    '''
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
        ofile.seek(0)
        out = io.BytesIO()

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
            ofile.write(buf)
            ofile.flush()

            # Complete the request by send back the status as two uint64s on the
            # socket:
            #     - http status
            #     - size of data in output shared memory
            connection.send(struct.pack('<qq', 200, response_size))

        except Exception as exc:
            logger.error(f'error occurred in executing function `{name}`: {exc}')
            for line in traceback.format_exception(exc):  # type: ignore
                logger.error(line.rstrip())
            connection.send(struct.pack('<qq', 500, 0))
            break

        finally:
            # Close the shared memory object.
            mem.close()

    # Close shared memory files.
    ifile.close()
    ofile.close()

    # Close the connection
    connection.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='python -m singlestoredb.functions.ext.mmap',
        description='Run a collacated Python UDF server',
    )
    parser.add_argument(
        '--max-connections', metavar='n', type=int, default=32,
        help='maximum number of server connections before refusing them',
    )
    parser.add_argument(
        '--single-thread', default=False, action='store_true',
        help='should the server run in single-thread mode?',
    )
    parser.add_argument(
        '--socket-path', metavar='file-path',
        default=os.path.join(tempfile.gettempdir(), secrets.token_hex(16)),
        help='path to communications socket',
    )
    parser.add_argument(
        '--db', metavar='conn-str', default='',
        help='connection string to use for registering functions',
    )
    parser.add_argument(
        '--replace-existing', action='store_true',
        help='should existing functions of the same name '
             'in the database be replaced?',
    )
    parser.add_argument(
        '--log-level', metavar='[info|debug|warning|error]', default='info',
        help='logging level',
    )
    parser.add_argument(
        'functions', metavar='module.or.func.path', nargs='*',
        help='functions or modules to export in UDF server',
    )
    args = parser.parse_args()

    logger.setLevel(getattr(logging, args.log_level.upper()))

    if os.path.exists(args.socket_path):
        try:
            os.unlink(args.socket_path)
        except (IOError, OSError):
            logger.error(f'could not remove existing socket path: {args.socket_path}')
            sys.exit(1)

    # Create application
    app = asgi.create_app(
        args.functions,
        app_mode='collocated',
        data_format='rowdat_1',
        base_url=args.socket_path,
    )

    funcs = app.show_create_functions(replace=True)  # type: ignore
    if not funcs:
        logger.error('no functions specified')
        sys.exit(1)

    for f in funcs:
        logger.info(f'function: {f}')

    # Register functions with database
    if args.db:
        logger.info('registering functions with database')
        app.register_functions(args.db, replace=args.replace_existing)  # type: ignore

    # Create the Unix socket server.
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

    # Bind our server to the path.
    server.bind(args.socket_path)

    logger.info(f'using socket path: {args.socket_path}')

    # Listen for incoming connections. Argument is the number of connections to
    # keep in the backlog before we begin refusing them; 32 is plenty for this
    # simple case.
    server.listen(args.max_connections)

    # Accept connections forever.
    try:
        while True:
            # Listen for the next connection on our port.
            connection, client_address = server.accept()

            # Handle the connection in a separate thread.
            t = threading.Thread(
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
        sys.exit(0)

    finally:
        # Remove the socket file before we exit.
        try:
            os.unlink(args.socket_path)
        except (IOError, OSError):
            logger.error(f'could not remove socket path: {args.socket_path}')
