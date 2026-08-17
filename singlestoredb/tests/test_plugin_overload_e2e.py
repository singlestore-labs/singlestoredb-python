#!/usr/bin/env python
# type: ignore
"""End-to-end overload tests: real engine, real collocated plugin server.

These exercise the parts of overload dispatch that only the engine can
drive: the ``param_types`` list it puts in the v3 handshake envelope, its
own DDL-level duplicate rejection, ``DROP FUNCTION ... WITH ID``, and how a
server-side failure surfaces to the SQL client.

The whole module skips unless the target server is a build with overloaded
functions available (``fv_overloaded_functions``) — the Docker dev image is
not.
"""
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

import singlestoredb as s2
from . import utils


PLUGIN_MODULE = '''
from singlestoredb.functions import udf


@udf
def ovl_e2e_base(x: int) -> int:
    return x + 1
'''


def _start_plugin_server(tmpdir):
    """Launch the collocated plugin server and wait for its socket."""
    sock_path = os.path.join(tmpdir, 'plugin.sock')
    proc = subprocess.Popen(
        [
            sys.executable, '-m', 'singlestoredb.functions.ext.plugin',
            '--plugin-name', 'ovl_e2e_plugin',
            '--search-path', tmpdir,
            '--socket', sock_path,
            '--process-mode', 'thread',
            '--log-level', 'info',
        ],
        env=dict(os.environ, PYTHONPATH=tmpdir),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.time() + 30
    while time.time() < deadline:
        if os.path.exists(sock_path):
            return proc, sock_path
        if proc.poll() is not None:
            raise RuntimeError(
                f'plugin server exited with code {proc.returncode}',
            )
        time.sleep(0.25)
    proc.terminate()
    raise RuntimeError('plugin server socket never appeared')


class TestPluginOverloadE2E(unittest.TestCase):

    dbname: str = ''
    dbexisted: bool = False
    tmpdir = None
    proc = None
    server_name = 'ovl_e2e_srv'

    @classmethod
    def setUpClass(cls):
        cls.dbname, cls.dbexisted = utils.load_sql(
            os.path.join(os.path.dirname(__file__), 'empty.sql'),
        )
        try:
            with s2.connect(database=cls.dbname) as conn:
                with conn.cursor() as cur:
                    # The feature flag has to be flipped before the option
                    # that depends on it is even recognized.
                    cur.execute('SET GLOBAL fv_overloaded_functions = ON')
                    cur.execute('SET GLOBAL enable_overloaded_functions = ON')
                    cur.execute('SET GLOBAL enable_external_functions = ON')
        except Exception as exc:
            if not cls.dbexisted:
                utils.drop_database(cls.dbname)
            raise unittest.SkipTest(
                f'overloaded functions unavailable on this server: {exc}',
            )

        cls.tmpdir = tempfile.mkdtemp(prefix='ovl_e2e_')
        with open(os.path.join(cls.tmpdir, 'ovl_e2e_plugin.py'), 'w') as f:
            f.write(PLUGIN_MODULE)
        cls.proc, cls.sock_path = _start_plugin_server(cls.tmpdir)

        with s2.connect(database=cls.dbname) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f'CREATE OR REPLACE PLUGIN SERVER {cls.server_name} '
                    f"AS COLLOCATED SERVICE '{cls.sock_path}' "
                    f'FORMAT ROWDAT_1 VERSION 3',
                )

    @classmethod
    def tearDownClass(cls):
        if cls.proc is not None:
            try:
                with s2.connect(database=cls.dbname) as conn:
                    with conn.cursor() as cur:
                        # Cascades a DROP FUNCTION for every plugin function
                        # created against this server.
                        cur.execute(
                            f'DROP PLUGIN SERVER {cls.server_name}',
                        )
            except Exception:
                pass
            cls.proc.terminate()
            cls.proc.wait(timeout=15)
            cls.proc = None
        if cls.tmpdir is not None:
            shutil.rmtree(cls.tmpdir, ignore_errors=True)
            cls.tmpdir = None
        if not cls.dbexisted:
            utils.drop_database(cls.dbname)

    def setUp(self):
        self.conn = s2.connect(database=type(self).dbname)
        self.cur = self.conn.cursor()

    def tearDown(self):
        self.cur.close()
        self.conn.close()

    # -- helpers ---------------------------------------------------------

    def create_variant(self, name, params, returns, body):
        """CREATE PLUGIN FUNCTION for one overload variant."""
        self.cur.execute(
            f'CREATE PLUGIN FUNCTION {name}({params}) RETURNS {returns} '
            f'AS $$ {body} $$ '
            f'USING PLUGIN SERVER {type(self).server_name}',
        )

    def scalar(self, expr):
        self.cur.execute(f'SELECT {expr}')
        return list(self.cur)[0][0]

    def variant_ids(self, name):
        """Map declared-argument string → variant id from SHOW FUNCTIONS."""
        self.cur.execute('SHOW FUNCTIONS')
        return {
            row[1]: row[3] for row in self.cur if row[0] == name
        }

    # -- tests -----------------------------------------------------------

    def test_dispatch_by_declared_signature(self):
        self.create_variant('pp_a', 'a INT', 'TEXT', "return 'int'")
        self.create_variant(
            'pp_a', 'a INT, b INT', 'TEXT', "return 'int,int'",
        )
        self.create_variant(
            'pp_a', 'a VARCHAR(50)', 'TEXT', "return 'varchar'",
        )
        self.create_variant('pp_a', '', 'TEXT', "return 'none'")

        assert self.scalar('pp_a(1 :> INT)') == 'int'
        assert self.scalar('pp_a(1 :> INT, 2 :> INT)') == 'int,int'
        assert self.scalar("pp_a('x' :> VARCHAR(50))") == 'varchar'
        assert self.scalar('pp_a()') == 'none'

    def test_zero_arg_variant_reachable_alongside_one_arg(self):
        # A zero-arg variant arrives with param_types='' rather than no type
        # list at all; conflating the two makes it unreachable.
        self.create_variant('pp_z', '', 'TEXT', "return 'zero'")
        self.create_variant('pp_z', 'a INT', 'TEXT', "return 'one'")

        assert self.scalar('pp_z()') == 'zero'
        assert self.scalar('pp_z(1 :> INT)') == 'one'

    def test_int_bigint_ambiguity_resolved_by_cast(self):
        self.create_variant('pp_i', 'a INT', 'TEXT', "return 'int:%s' % a")
        self.create_variant(
            'pp_i', 'a BIGINT', 'TEXT', "return 'bigint:%s' % a",
        )

        # Both INT and BIGINT accept a TINYINT equally well, so the engine
        # refuses to pick (error 3021).
        with self.assertRaises(s2.OperationalError) as ctx:
            self.scalar('pp_i(7 :> TINYINT)')
        assert 'mbiguous' in str(ctx.exception)

        assert self.scalar('pp_i(7 :> TINYINT :> INT)') == 'int:7'
        assert self.scalar('pp_i(7 :> BIGINT)') == 'bigint:7'

    def test_drop_one_variant_by_id(self):
        self.create_variant('pp_d', 'a INT', 'TEXT', "return 'one'")
        self.create_variant('pp_d', 'a INT, b INT', 'TEXT', "return 'two'")
        assert self.scalar('pp_d(1 :> INT)') == 'one'
        assert self.scalar('pp_d(1 :> INT, 2 :> INT)') == 'two'

        ids = self.variant_ids('pp_d')
        self.cur.execute(f"DROP FUNCTION pp_d WITH ID {ids['INT, INT']}")

        assert self.scalar('pp_d(1 :> INT)') == 'one'
        with self.assertRaises(s2.OperationalError) as ctx:
            self.scalar('pp_d(1 :> INT, 2 :> INT)')
        assert 'expects 1 arguments' in str(ctx.exception)

    def test_engine_rejects_same_group_char_lengths(self):
        # CHAR(20) and CHAR(10) share one overload slot — the length is not
        # part of the key, on either side.
        self.create_variant('pp_c', 'a CHAR(20)', 'TEXT', "return 'c20'")
        with self.assertRaises(s2.OperationalError) as ctx:
            self.create_variant('pp_c', 'a CHAR(10)', 'TEXT', "return 'c10'")
        assert '3020' in str(ctx.exception) or 'overload' in str(ctx.exception)

        assert self.scalar("pp_c('x' :> CHAR(20))") == 'c20'

    def test_engine_rejects_equivalent_int_types(self):
        # The engine treats INT and MEDIUMINT as one type for overloading, so
        # the pair never reaches the plugin server at all.
        self.create_variant('pp_m', 'a INT', 'TEXT', "return 'int'")
        with self.assertRaises(s2.OperationalError) as ctx:
            self.create_variant(
                'pp_m', 'a MEDIUMINT', 'TEXT', "return 'mediumint'",
            )
        assert '3028' in str(ctx.exception) or 'overload' in str(ctx.exception)

        assert self.scalar('pp_m(1 :> INT)') == 'int'

    def test_install_failure_surfaces_as_error_not_reset(self):
        # A body that won't compile fails inside the handshake. The server
        # must still answer the pending request, or the engine reports a
        # bare "connection reset by peer" with no reason.
        self.create_variant('pp_bad', 'a INT', 'TEXT', "return 'x' +")
        with self.assertRaises(s2.OperationalError) as ctx:
            self.scalar('pp_bad(1 :> INT)')
        msg = str(ctx.exception)
        assert 'failed to install function definition' in msg
        assert 'invalid syntax' in msg


if __name__ == '__main__':
    unittest.main()
