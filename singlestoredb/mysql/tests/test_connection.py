# type: ignore
import datetime
import ssl
import time
from unittest import mock

import pytest

import singlestoredb.mysql as sv
from singlestoredb.mysql.constants import CLIENT
from singlestoredb.mysql.tests import base


class TempUser:

    def __init__(self, c, user, db, auth=None, authdata=None, password=None):
        self._c = c
        self._user = user
        self._db = db
        create = 'CREATE USER ' + user
        if password is not None:
            create += " IDENTIFIED BY '%s'" % password
        elif auth is not None:
            create += ' IDENTIFIED WITH %s' % auth
            if authdata is not None:
                create += " AS '%s'" % authdata
        try:
            c.execute(create)
            self._created = True
        except sv.err.InternalError:
            # already exists - TODO need to check the same plugin applies
            self._created = False
        try:
            c.execute('GRANT SELECT ON %s.* TO %s' % (db, user))
            self._grant = True
        except sv.err.InternalError:
            self._grant = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._grant:
            self._c.execute('REVOKE SELECT ON %s.* FROM %s' % (self._db, self._user))
        if self._created:
            self._c.execute('DROP USER %s' % self._user)


class TestAuthentication(base.PyMySQLTestCase):

    socket_auth = False
    socket_found = False
    two_questions_found = False
    three_attempts_found = False
    pam_found = False
    mysql_old_password_found = False
    sha256_password_found = False
    ed25519_found = False

    import os

    osuser = os.environ.get('USER')

#   # socket auth requires the current user and for the connection to be a socket
#   # rest do grants @localhost due to incomplete logic - TODO change to @% then
#   db = base.PyMySQLTestCase.databases[0].copy()

#   socket_auth = db.get('unix_socket') is not None and db.get('host') in (
#       'localhost',
#       '127.0.0.1',
#   )

#   dbname = db['database']

#   cur = sv.connect(**db).cursor()
#   db.pop('user', None)
#   cur.execute('SHOW PLUGINS')
#   for r in cur:
#       if (r[1], r[2]) != ('ACTIVE', 'AUTHENTICATION'):
#           continue
#       if r[3] == 'auth_socket.so' or r[0] == 'unix_socket':
#           socket_plugin_name = r[0]
#           socket_found = True
#       elif r[3] == 'dialog_examples.so':
#           if r[0] == 'two_questions':
#               two_questions_found = True
#           elif r[0] == 'three_attempts':
#               three_attempts_found = True
#       elif r[0] == 'pam':
#           pam_found = True
#           pam_plugin_name = r[3].split('.')[0]
#           if pam_plugin_name == 'auth_pam':
#               pam_plugin_name = 'pam'
#           # MySQL: authentication_pam
#           # https://dev.mysql.com/doc/refman/5.5/en/pam-authentication-plugin.html

#           # MariaDB: pam
#           # https://mariadb.com/kb/en/mariadb/pam-authentication-plugin/

#           # Names differ but functionality is close
#       elif r[0] == 'mysql_old_password':
#           mysql_old_password_found = True
#       elif r[0] == 'sha256_password':
#           sha256_password_found = True
#       elif r[0] == 'ed25519':
#           ed25519_found = True
#       # else:
#       #    print("plugin: %r" % r[0])

    @pytest.mark.skip(reason='not currently supported in SingleStoreDB')
    def test_plugin(self):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(
            "select plugin from mysql.user where concat(user, '@', host)=current_user()",
        )
        for r in cur:
            self.assertIn(conn._auth_plugin_name, (r[0], 'mysql_native_password'))

    @pytest.mark.skip(reason='not currently supported by SingleStoreDB')
    @pytest.mark.skipif(not socket_auth, reason='connection to unix_socket required')
    @pytest.mark.skipif(socket_found, reason='socket plugin already installed')
    def testSocketAuthInstallPlugin(self):
        # needs plugin. lets install it.
        cur = self.connect().cursor()
        try:
            cur.execute("install plugin auth_socket soname 'auth_socket.so'")
            TestAuthentication.socket_found = True
            self.socket_plugin_name = 'auth_socket'
            self.realtestSocketAuth()
        except sv.err.InternalError:
            try:
                cur.execute("install soname 'auth_socket'")
                TestAuthentication.socket_found = True
                self.socket_plugin_name = 'unix_socket'
                self.realtestSocketAuth()
            except sv.err.InternalError:
                TestAuthentication.socket_found = False
                pytest.skip("we couldn't install the socket plugin")
        finally:
            if TestAuthentication.socket_found:
                cur.execute('uninstall plugin %s' % self.socket_plugin_name)

    @pytest.mark.skip(reason='not currently supported by SingleStoreDB')
    @pytest.mark.skipif(not socket_auth, reason='connection to unix_socket required')
    @pytest.mark.skipif(not socket_found, reason='no socket plugin')
    def testSocketAuth(self):
        self.realtestSocketAuth()

    def realtestSocketAuth(self):
        with TempUser(
            self.connect().cursor(),
            TestAuthentication.osuser + '@localhost',
            self.databases[0]['database'],
            self.socket_plugin_name,
        ) as _:
            sv.connect(user=TestAuthentication.osuser, **self.db)

    class Dialog:
        fail = False

        def __init__(self, con):
            self.fail = TestAuthentication.Dialog.fail
            pass

        def prompt(self, echo, prompt):
            if self.fail:
                self.fail = False
                return b'bad guess at a password'
            return self.m.get(prompt)

    class DialogHandler:
        def __init__(self, con):
            self.con = con

        def authenticate(self, pkt):
            while True:
                flag = pkt.read_uint8()
                echo = (flag & 0x06) == 0x02  # noqa: F841
                last = (flag & 0x01) == 0x01
                prompt = pkt.read_all()

                if prompt == b'Password, please:':
                    self.con.write_packet(b'stillnotverysecret\0')
                else:
                    self.con.write_packet(b'no idea what to do with this prompt\0')
                pkt = self.con._read_packet()
                pkt.check_error()
                if pkt.is_ok_packet() or last:
                    break
            return pkt

    class DefectiveHandler:
        def __init__(self, con):
            self.con = con

    @pytest.mark.skip(reason='not currently supported by SingleStoreDB')
    @pytest.mark.skipif(not socket_auth, reason='connection to unix_socket required')
    @pytest.mark.skipif(
        two_questions_found, reason='two_questions plugin already installed',
    )
    def testDialogAuthTwoQuestionsInstallPlugin(self):
        # needs plugin. lets install it.
        cur = self.connect().cursor()
        try:
            cur.execute("install plugin two_questions soname 'dialog_examples.so'")
            TestAuthentication.two_questions_found = True
            self.realTestDialogAuthTwoQuestions()
        except sv.err.InternalError:
            pytest.skip("we couldn't install the two_questions plugin")
        finally:
            if TestAuthentication.two_questions_found:
                cur.execute('uninstall plugin two_questions')

    @pytest.mark.skip(reason='not currently supported by SingleStoreDB')
    @pytest.mark.skipif(not socket_auth, reason='connection to unix_socket required')
    @pytest.mark.skipif(not two_questions_found, reason='no two questions auth plugin')
    def testDialogAuthTwoQuestions(self):
        self.realTestDialogAuthTwoQuestions()

    def realTestDialogAuthTwoQuestions(self):
        TestAuthentication.Dialog.fail = False
        TestAuthentication.Dialog.m = {
            b'Password, please:': b'notverysecret',
            b'Are you sure ?': b'yes, of course',
        }
        with TempUser(
            self.connect().cursor(),
            'singlestoredb_2q@localhost',
            self.databases[0]['database'],
            'two_questions',
            'notverysecret',
        ) as _:
            with self.assertRaises(sv.err.OperationalError):
                sv.connect(user='singlestoredb_2q', **self.db)
            sv.connect(
                user='singlestoredb_2q',
                auth_plugin_map={b'dialog': TestAuthentication.Dialog},
                **self.db,
            )

    @pytest.mark.skip(reason='not currently supported by SingleStoreDB')
    @pytest.mark.skipif(not socket_auth, reason='connection to unix_socket required')
    @pytest.mark.skipif(
        three_attempts_found, reason='three_attempts plugin already installed',
    )
    def testDialogAuthThreeAttemptsQuestionsInstallPlugin(self):
        # needs plugin. lets install it.
        cur = self.connect().cursor()
        try:
            cur.execute("install plugin three_attempts soname 'dialog_examples.so'")
            TestAuthentication.three_attempts_found = True
            self.realTestDialogAuthThreeAttempts()
        except sv.err.InternalError:
            pytest.skip("we couldn't install the three_attempts plugin")
        finally:
            if TestAuthentication.three_attempts_found:
                cur.execute('uninstall plugin three_attempts')

    @pytest.mark.skip(reason='not currently supported by SingleStoreDB')
    @pytest.mark.skipif(not socket_auth, reason='connection to unix_socket required')
    @pytest.mark.skipif(not three_attempts_found, reason='no three attempts plugin')
    def testDialogAuthThreeAttempts(self):
        self.realTestDialogAuthThreeAttempts()

    def realTestDialogAuthThreeAttempts(self):
        TestAuthentication.Dialog.m = {b'Password, please:': b'stillnotverysecret'}
        TestAuthentication.Dialog.fail = (
            True  # fail just once. We've got three attempts after all
        )
        with TempUser(
            self.connect().cursor(),
            'singlestoredb_3a@localhost',
            self.databases[0]['database'],
            'three_attempts',
            'stillnotverysecret',
        ) as _:
            sv.connect(
                user='singlestoredb_3a',
                auth_plugin_map={b'dialog': TestAuthentication.Dialog},
                **self.db,
            )
            sv.connect(
                user='singlestoredb_3a',
                auth_plugin_map={b'dialog': TestAuthentication.DialogHandler},
                **self.db,
            )
            with self.assertRaises(sv.err.OperationalError):
                sv.connect(
                    user='singlestoredb_3a',
                    auth_plugin_map={b'dialog': object},
                    **self.db,
                )

            with self.assertRaises(sv.err.OperationalError):
                sv.connect(
                    user='singlestoredb_3a',
                    auth_plugin_map={b'dialog': TestAuthentication.DefectiveHandler},
                    **self.db,
                )
            with self.assertRaises(sv.err.OperationalError):
                sv.connect(
                    user='singlestoredb_3a',
                    auth_plugin_map={b'notdialogplugin': TestAuthentication.Dialog},
                    **self.db,
                )
            TestAuthentication.Dialog.m = {b'Password, please:': b'I do not know'}
            with self.assertRaises(sv.err.OperationalError):
                sv.connect(
                    user='singlestoredb_3a',
                    auth_plugin_map={b'dialog': TestAuthentication.Dialog},
                    **self.db,
                )
            TestAuthentication.Dialog.m = {b'Password, please:': None}
            with self.assertRaises(sv.err.OperationalError):
                sv.connect(
                    user='singlestoredb_3a',
                    auth_plugin_map={b'dialog': TestAuthentication.Dialog},
                    **self.db,
                )

    @pytest.mark.skipif(not socket_auth, reason='connection to unix_socket required')
    @pytest.mark.skipif(pam_found, reason='pam plugin already installed')
    @pytest.mark.skipif(
        os.environ.get('PASSWORD') is None, reason='PASSWORD env var required',
    )
    @pytest.mark.skipif(
        os.environ.get('PAMSERVICE') is None, reason='PAMSERVICE env var required',
    )
    def testPamAuthInstallPlugin(self):
        # needs plugin. lets install it.
        cur = self.connect().cursor()
        try:
            cur.execute("install plugin pam soname 'auth_pam.so'")
            TestAuthentication.pam_found = True
            self.realTestPamAuth()
        except sv.err.InternalError:
            pytest.skip("we couldn't install the auth_pam plugin")
        finally:
            if TestAuthentication.pam_found:
                cur.execute('uninstall plugin pam')

    @pytest.mark.skipif(not socket_auth, reason='connection to unix_socket required')
    @pytest.mark.skipif(not pam_found, reason='no pam plugin')
    @pytest.mark.skipif(
        os.environ.get('PASSWORD') is None, reason='PASSWORD env var required',
    )
    @pytest.mark.skipif(
        os.environ.get('PAMSERVICE') is None, reason='PAMSERVICE env var required',
    )
    def testPamAuth(self):
        self.realTestPamAuth()

    @pytest.mark.skip(reason='skip PAM tests on SingleStoreDB')
    def realTestPamAuth(self):
        db = self.db.copy()
        import os

        db['password'] = os.environ.get('PASSWORD')
        cur = self.connect().cursor()
        try:
            cur.execute('show grants for ' + TestAuthentication.osuser + '@localhost')
            grants = cur.fetchone()[0]
            cur.execute('drop user ' + TestAuthentication.osuser + '@localhost')
        except sv.OperationalError as e:
            # assuming the user doesn't exist which is ok too
            self.assertEqual(1045, e.args[0])
            grants = None
        with TempUser(
            cur,
            TestAuthentication.osuser + '@localhost',
            self.databases[0]['database'],
            'pam',
            os.environ.get('PAMSERVICE'),
        ) as _:
            try:
                c = sv.connect(user=TestAuthentication.osuser, **db)  # noqa: F841
                db['password'] = 'very bad guess at password'
                with self.assertRaises(sv.err.OperationalError):
                    sv.connect(
                        user=TestAuthentication.osuser,
                        auth_plugin_map={
                            b'mysql_cleartext_password':
                                TestAuthentication.DefectiveHandler,
                        },
                        **self.db,
                    )
            except sv.OperationalError as e:
                self.assertEqual(1045, e.args[0])
                # we had 'bad guess at password' work with pam. Well at least
                # we get a permission denied here
                with self.assertRaises(sv.err.OperationalError):
                    sv.connect(
                        user=TestAuthentication.osuser,
                        auth_plugin_map={
                            b'mysql_cleartext_password':
                                TestAuthentication.DefectiveHandler,
                        },
                        **self.db,
                    )
        if grants:
            # recreate the user
            cur.execute(grants)

    @pytest.mark.skip(reason='not currently supported by SingleStoreDB')
    @pytest.mark.skipif(not socket_auth, reason='connection to unix_socket required')
    @pytest.mark.skipif(
        not sha256_password_found,
        reason='no sha256 password authentication plugin found',
    )
    def testAuthSHA256(self):
        conn = self.connect()
        c = conn.cursor()
        with TempUser(
            c,
            'singlestoredb_sha256@localhost',
            self.databases[0]['database'],
            'sha256_password',
        ) as _:
            c.execute("SET PASSWORD FOR 'singlestoredb_sha256'@'localhost' ='Sh@256Pa33'")
            c.execute('FLUSH PRIVILEGES')
            db = self.db.copy()
            db['password'] = 'Sh@256Pa33'
            # Although SHA256 is supported, need the configuration of public
            # key of the mysql server. Currently will get error by this test.
            with self.assertRaises(sv.err.OperationalError):
                sv.connect(user='singlestoredb_sha256', **db)

    @pytest.mark.skipif(not ed25519_found, reason='no ed25519 authention plugin')
    def testAuthEd25519(self):
        db = self.db.copy()
        db.pop('password', None)
        conn = self.connect()
        c = conn.cursor()
        c.execute("select ed25519_password(''), ed25519_password('ed25519_password')")
        for r in c:
            empty_pass = r[0].decode('ascii')
            non_empty_pass = r[1].decode('ascii')

        with TempUser(
            c,
            'singlestoredb_ed25519',
            self.databases[0]['database'],
            'ed25519',
            empty_pass,
        ) as _:
            sv.connect(user='singlestoredb_ed25519', password='', **db)

        with TempUser(
            c,
            'singlestoredb_ed25519',
            self.databases[0]['database'],
            'ed25519',
            non_empty_pass,
        ) as _:
            sv.connect(user='singlestoredb_ed25519', password='ed25519_password', **db)


class TestConnection(base.PyMySQLTestCase):

    def test_utf8mb4(self):
        """This test requires MySQL >= 5.5."""
        arg = self.databases[0].copy()
        arg['charset'] = 'utf8mb4'
        conn = sv.connect(**arg)  # noqa: F841

    def test_largedata(self):
        """Large query and response (>=16MB)."""
        cur = self.connect().cursor()
        cur.execute('SELECT @@max_allowed_packet')
        if cur.fetchone()[0] < 16 * 1024 * 1024 + 10:
            print('Set max_allowed_packet to bigger than 17MB')
            return
        t = 'a' * (16 * 1024 * 1024)
        cur.execute("SELECT '" + t + "'")
        assert cur.fetchone()[0] == t

    def test_autocommit(self):
        con = self.connect()
        self.assertFalse(con.get_autocommit())

        cur = con.cursor()
        cur.execute('SET AUTOCOMMIT=1')
        self.assertTrue(con.get_autocommit())

        con.autocommit(False)
        self.assertFalse(con.get_autocommit())
        cur.execute('SELECT @@AUTOCOMMIT')
        self.assertEqual(cur.fetchone()[0], 0)

    def test_select_db(self):
        con = self.connect()
        current_db = self.databases[0]['database']
        other_db = self.databases[1]['database']

        cur = con.cursor()
        cur.execute('SELECT database()')
        self.assertEqual(cur.fetchone()[0], current_db)

        con.select_db(other_db)
        cur.execute('SELECT database()')
        self.assertEqual(cur.fetchone()[0], other_db)

    @pytest.mark.skip(reason='wait_timeout= does not work')
    def test_connection_gone_away(self):
        """
        http://dev.mysql.com/doc/refman/5.0/en/gone-away.html
        http://dev.mysql.com/doc/refman/5.0/en/error-messages-client.html#error_cr_server_gone_error

        """
        con = self.connect()
        cur = con.cursor()
        cur.execute('SET wait_timeout=1')
        time.sleep(2)
        with self.assertRaises(sv.OperationalError) as cm:
            cur.execute('SELECT 1+1')
        # error occures while reading, not writing because of socket buffer.
        # self.assertEqual(cm.exception.args[0], 2006)
        self.assertIn(cm.exception.args[0], (2006, 2013))

    def test_init_command(self):
        conn = self.connect(
            init_command='SELECT "bar"; SELECT "baz"',
            client_flag=CLIENT.MULTI_STATEMENTS,
        )
        c = conn.cursor()
        c.execute('select "foobar";')
        self.assertEqual(('foobar',), c.fetchone())
        conn.close()
        with self.assertRaises(sv.err.Error):
            conn.ping(reconnect=False)

    def test_read_default_group(self):
        conn = self.connect(
            read_default_group='client',
        )
        self.assertTrue(conn.open)

    def test_set_charset(self):
        c = self.connect()
        c.set_charset('utf8mb4')
        # TODO validate setting here

    def test_defer_connect(self):
        import socket

        d = self.databases[0].copy()
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect(d['unix_socket'])
        except KeyError:
            sock.close()
            sock = socket.create_connection(
                (d.get('host', 'localhost'), d.get('port', 3306)),
            )
        for k in ['unix_socket', 'host', 'port']:
            try:
                del d[k]
            except KeyError:
                pass

        c = sv.connect(defer_connect=True, **d)
        self.assertFalse(c.open)
        c.connect(sock)
        c.close()
        sock.close()

    @pytest.mark.skip(reason='disable local user tests')
    def test_ssl_connect(self):
        dummy_ssl_context = mock.Mock(options=0)
        with mock.patch(
            'singlestoredb.connections.Connection.connect',
        ) as _, mock.patch(
            'singlestoredb.connections.ssl.create_default_context',
            new=mock.Mock(return_value=dummy_ssl_context),
        ) as create_default_context:
            sv.connect(
                ssl={
                    'ca': 'ca',
                    'cert': 'cert',
                    'key': 'key',
                    'cipher': 'cipher',
                },
            )
            assert create_default_context.called
            assert dummy_ssl_context.check_hostname
            assert dummy_ssl_context.verify_mode == ssl.CERT_REQUIRED
            dummy_ssl_context.load_cert_chain.assert_called_with('cert', keyfile='key')
            dummy_ssl_context.set_ciphers.assert_called_with('cipher')

        dummy_ssl_context = mock.Mock(options=0)
        with mock.patch(
            'singlestoredb.connections.Connection.connect',
        ) as _, mock.patch(
            'singelstoredb.connections.ssl.create_default_context',
            new=mock.Mock(return_value=dummy_ssl_context),
        ) as create_default_context:
            sv.connect(
                ssl={
                    'ca': 'ca',
                    'cert': 'cert',
                    'key': 'key',
                },
            )
            assert create_default_context.called
            assert dummy_ssl_context.check_hostname
            assert dummy_ssl_context.verify_mode == ssl.CERT_REQUIRED
            dummy_ssl_context.load_cert_chain.assert_called_with('cert', keyfile='key')
            dummy_ssl_context.set_ciphers.assert_not_called

        dummy_ssl_context = mock.Mock(options=0)
        with mock.patch(
            'singelstoredb.connections.Connection.connect',
        ) as _, mock.patch(
            'singlestoredb.connections.ssl.create_default_context',
            new=mock.Mock(return_value=dummy_ssl_context),
        ) as create_default_context:
            sv.connect(
                ssl_ca='ca',
            )
            assert create_default_context.called
            assert not dummy_ssl_context.check_hostname
            assert dummy_ssl_context.verify_mode == ssl.CERT_NONE
            dummy_ssl_context.load_cert_chain.assert_not_called
            dummy_ssl_context.set_ciphers.assert_not_called

        dummy_ssl_context = mock.Mock(options=0)
        with mock.patch(
            'singlestoredb.connections.Connection.connect',
        ) as _, mock.patch(
            'singlestoredb.connections.ssl.create_default_context',
            new=mock.Mock(return_value=dummy_ssl_context),
        ) as create_default_context:
            sv.connect(
                ssl_ca='ca',
                ssl_cert='cert',
                ssl_key='key',
            )
            assert create_default_context.called
            assert not dummy_ssl_context.check_hostname
            assert dummy_ssl_context.verify_mode == ssl.CERT_NONE
            dummy_ssl_context.load_cert_chain.assert_called_with('cert', keyfile='key')
            dummy_ssl_context.set_ciphers.assert_not_called

        for ssl_verify_cert in (True, '1', 'yes', 'true'):
            dummy_ssl_context = mock.Mock(options=0)
            with mock.patch(
                'singlestoredb.connections.Connection.connect',
            ) as _, mock.patch(
                'singlestoredb.connections.ssl.create_default_context',
                new=mock.Mock(return_value=dummy_ssl_context),
            ) as create_default_context:
                sv.connect(
                    ssl_cert='cert',
                    ssl_key='key',
                    ssl_verify_cert=ssl_verify_cert,
                )
                assert create_default_context.called
                assert not dummy_ssl_context.check_hostname
                assert dummy_ssl_context.verify_mode == ssl.CERT_REQUIRED
                dummy_ssl_context.load_cert_chain.assert_called_with(
                    'cert', keyfile='key',
                )
                dummy_ssl_context.set_ciphers.assert_not_called

        for ssl_verify_cert in (None, False, '0', 'no', 'false'):
            dummy_ssl_context = mock.Mock(options=0)
            with mock.patch(
                'singlestoredb.connections.Connection.connect',
            ) as _, mock.patch(
                'singlestoredb.connections.ssl.create_default_context',
                new=mock.Mock(return_value=dummy_ssl_context),
            ) as create_default_context:
                sv.connect(
                    ssl_cert='cert',
                    ssl_key='key',
                    ssl_verify_cert=ssl_verify_cert,
                )
                assert create_default_context.called
                assert not dummy_ssl_context.check_hostname
                assert dummy_ssl_context.verify_mode == ssl.CERT_NONE
                dummy_ssl_context.load_cert_chain.assert_called_with(
                    'cert', keyfile='key',
                )
                dummy_ssl_context.set_ciphers.assert_not_called

        for ssl_ca in ('ca', None):
            for ssl_verify_cert in ('foo', 'bar', ''):
                dummy_ssl_context = mock.Mock(options=0)
                with mock.patch(
                    'singlestoredb.connections.Connection.connect',
                ) as _, mock.patch(
                    'singlestoredb.connections.ssl.create_default_context',
                    new=mock.Mock(return_value=dummy_ssl_context),
                ) as create_default_context:
                    sv.connect(
                        ssl_ca=ssl_ca,
                        ssl_cert='cert',
                        ssl_key='key',
                        ssl_verify_cert=ssl_verify_cert,
                    )
                    assert create_default_context.called
                    assert not dummy_ssl_context.check_hostname
                    assert dummy_ssl_context.verify_mode == (
                        ssl.CERT_REQUIRED if ssl_ca is not None else ssl.CERT_NONE
                    ), (ssl_ca, ssl_verify_cert)
                    dummy_ssl_context.load_cert_chain.assert_called_with(
                        'cert', keyfile='key',
                    )
                    dummy_ssl_context.set_ciphers.assert_not_called

        dummy_ssl_context = mock.Mock(options=0)
        with mock.patch(
            'singlestoredb.connections.Connection.connect',
        ) as _, mock.patch(
            'singlestoredb.connections.ssl.create_default_context',
            new=mock.Mock(return_value=dummy_ssl_context),
        ) as create_default_context:
            sv.connect(
                ssl_ca='ca',
                ssl_cert='cert',
                ssl_key='key',
                ssl_verify_identity=True,
            )
            assert create_default_context.called
            assert dummy_ssl_context.check_hostname
            assert dummy_ssl_context.verify_mode == ssl.CERT_NONE
            dummy_ssl_context.load_cert_chain.assert_called_with('cert', keyfile='key')
            dummy_ssl_context.set_ciphers.assert_not_called

        dummy_ssl_context = mock.Mock(options=0)
        with mock.patch(
            'singlestoredb.connections.Connection.connect',
        ) as _, mock.patch(
            'singlestoredb.connections.ssl.create_default_context',
            new=mock.Mock(return_value=dummy_ssl_context),
        ) as create_default_context:
            sv.connect(
                ssl_disabled=True,
                ssl={
                    'ca': 'ca',
                    'cert': 'cert',
                    'key': 'key',
                },
            )
            assert not create_default_context.called

        dummy_ssl_context = mock.Mock(options=0)
        with mock.patch(
            'singlestoredb.connections.Connection.connect',
        ) as _, mock.patch(
            'singlestoredb.connections.ssl.create_default_context',
            new=mock.Mock(return_value=dummy_ssl_context),
        ) as create_default_context:
            sv.connect(
                ssl_disabled=True,
                ssl_ca='ca',
                ssl_cert='cert',
                ssl_key='key',
            )
            assert not create_default_context.called


# A custom type and function to escape it
class Foo:
    value = 'bar'


def escape_foo(x, d):
    return x.value


class TestEscape(base.PyMySQLTestCase):

    def test_escape_string(self):
        con = self.connect()
        cur = con.cursor()  # noqa: F841

        self.assertEqual(con.escape("foo'bar"), "'foo\\'bar'")
#       # added NO_AUTO_CREATE_USER as not including it in 5.7 generates warnings
#       # mysql-8.0 removes the option however
#       if self.mysql_server_is(con, (8, 0, 0)):
#           cur.execute("SET sql_mode='NO_BACKSLASH_ESCAPES'")
#       else:
#           cur.execute("SET sql_mode='NO_BACKSLASH_ESCAPES,NO_AUTO_CREATE_USER'")
#       self.assertEqual(con.escape("foo'bar"), "'foo''bar'")

    def test_escape_builtin_encoders(self):
        con = self.connect()
        cur = con.cursor()  # noqa: F841

        val = datetime.datetime(2012, 3, 4, 5, 6)
        self.assertEqual(con.escape(val, con.encoders), "'2012-03-04 05:06:00'")

    def test_escape_custom_object(self):
        con = self.connect()
        cur = con.cursor()  # noqa: F841

        mapping = {Foo: escape_foo}
        self.assertEqual(con.escape(Foo(), mapping), 'bar')

    def test_escape_fallback_encoder(self):
        con = self.connect()
        cur = con.cursor()  # noqa: F841

        class Custom(str):
            pass

        mapping = {str: sv.converters.escape_string}
        self.assertEqual(con.escape(Custom('foobar'), mapping), "'foobar'")

    def test_escape_no_default(self):
        con = self.connect()
        cur = con.cursor()  # noqa: F841

        self.assertRaises(TypeError, con.escape, 42, {})

    def test_escape_dict_value(self):
        con = self.connect()
        cur = con.cursor()  # noqa: F841

        mapping = con.encoders.copy()
        mapping[Foo] = escape_foo
        self.assertEqual(con.escape({'foo': Foo()}, mapping), {'foo': 'bar'})

    def test_escape_list_item(self):
        con = self.connect()
        cur = con.cursor()  # noqa: F841

        mapping = con.encoders.copy()
        mapping[Foo] = escape_foo
        self.assertEqual(con.escape([Foo()], mapping), '(bar)')

    def test_previous_cursor_not_closed(self):
        con = self.connect(
            init_command='SELECT "bar"; SELECT "baz"',
            client_flag=CLIENT.MULTI_STATEMENTS,
        )
        cur1 = con.cursor()
        cur1.execute('SELECT 1; SELECT 2')
        cur2 = con.cursor()
        cur2.execute('SELECT 3')
        self.assertEqual(cur2.fetchone()[0], 3)

    def test_commit_during_multi_result(self):
        con = self.connect(client_flag=CLIENT.MULTI_STATEMENTS)
        cur = con.cursor()
        cur.execute('SELECT 1; SELECT 2')
        con.commit()
        cur.execute('SELECT 3')
        self.assertEqual(cur.fetchone()[0], 3)
