import unittest

from singlestoredb.functions.ext.function_url import classify_interactive_registration
from singlestoredb.functions.ext.function_url import extract_service_url
from singlestoredb.functions.ext.function_url import is_function_not_defined
from singlestoredb.functions.ext.function_url import is_interactive_service_url
from singlestoredb.mysql.constants import ER


class TestFunctionURL(unittest.TestCase):

    def test_extract_managed_double_quoted(self):
        sql = (
            'CREATE OR REPLACE EXTERNAL FUNCTION `shi_test`(v BIGINT) '
            'RETURNS TEXT AS MANAGED SERVICE '
            '"https://apps.us-east-1.cloud.singlestore.com/pythonudfs/abc/" '
            'FORMAT ROWDAT_1;'
        )
        self.assertEqual(
            extract_service_url(sql),
            'https://apps.us-east-1.cloud.singlestore.com/pythonudfs/abc/',
        )

    def test_extract_service_single_quoted(self):
        sql = (
            "CREATE EXTERNAL FUNCTION foo() RETURNS INT AS REMOTE SERVICE "
            "'https://example.com/invoke' FORMAT ROWDAT_1;"
        )
        self.assertEqual(extract_service_url(sql), 'https://example.com/invoke')

    def test_extract_none(self):
        self.assertIsNone(extract_service_url('CREATE FUNCTION foo() RETURNS INT'))

    def test_classify_missing_create(self):
        self.assertEqual(
            classify_interactive_registration(
                None,
                'https://gw/pythonudfs/sess/interactive/',
            ),
            'create',
        )

    def test_classify_this_session_replace(self):
        this = 'https://gw/pythonudfs/sess/interactive/'
        self.assertEqual(
            classify_interactive_registration(this, this),
            'replace',
        )
        self.assertEqual(
            classify_interactive_registration(this.rstrip('/'), this),
            'replace',
        )

    def test_classify_published_refuses(self):
        with self.assertRaises(ValueError) as ctx:
            classify_interactive_registration(
                'https://gw/pythonudfs/published-id/',
                'https://gw/pythonudfs/sess/interactive/',
            )
        self.assertIn('will not replace', str(ctx.exception))
        self.assertIn('this session is', str(ctx.exception))

    def test_classify_other_session_refuses(self):
        with self.assertRaises(ValueError):
            classify_interactive_registration(
                'https://gw/pythonudfs/other-sess/interactive/',
                'https://gw/pythonudfs/sess/interactive/',
            )

    def test_is_interactive(self):
        self.assertTrue(
            is_interactive_service_url(
                'https://gw/pythonudfs/sess/interactive/',
            ),
        )
        self.assertFalse(
            is_interactive_service_url(
                'https://gw/pythonudfs/published-id/',
            ),
        )

    def test_function_not_defined(self):
        exc = type('E', (Exception,), {'errno': ER.FUNCTION_NOT_DEFINED})()
        self.assertTrue(is_function_not_defined(exc))
        self.assertFalse(is_function_not_defined(ValueError('nope')))

    def test_fake_cursor_ownership(self):
        this_session = 'https://gw/pythonudfs/sess/interactive/'
        published_sql = (
            'CREATE OR REPLACE EXTERNAL FUNCTION `shi_test`(v BIGINT) '
            'RETURNS BIGINT AS MANAGED SERVICE '
            '"https://gw/pythonudfs/published-id/" FORMAT ROWDAT_1;'
        )
        this_sql = (
            'CREATE OR REPLACE EXTERNAL FUNCTION `shi_test`(v BIGINT) '
            'RETURNS BIGINT AS MANAGED SERVICE '
            f'"{this_session}" FORMAT ROWDAT_1;'
        )

        class _Cursor:
            def __init__(self, rows=None, exc=None):
                self._rows = rows or []
                self._exc = exc

            def execute(self, sql):
                if self._exc is not None:
                    raise self._exc

            def __iter__(self):
                return iter(self._rows)

        def show_url(cur):
            try:
                cur.execute('SHOW CREATE FUNCTION `shi_test`')
            except Exception as exc:
                if is_function_not_defined(exc):
                    return None
                raise
            rows = list(cur)
            if not rows:
                return None
            return extract_service_url(rows[0][2])

        missing = type('E', (Exception,), {'errno': ER.FUNCTION_NOT_DEFINED})()
        self.assertEqual(
            classify_interactive_registration(show_url(_Cursor(exc=missing)), this_session),
            'create',
        )
        self.assertEqual(
            classify_interactive_registration(
                show_url(_Cursor(rows=[('shi_test', '', this_sql)])),
                this_session,
            ),
            'replace',
        )
        with self.assertRaises(ValueError):
            classify_interactive_registration(
                show_url(_Cursor(rows=[('shi_test', '', published_sql)])),
                this_session,
            )


if __name__ == '__main__':
    unittest.main()
