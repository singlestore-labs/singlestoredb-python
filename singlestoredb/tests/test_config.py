#!/usr/bin/env python
# type: ignore
# encoding: utf-8
#
# Copyright SAS Institute
#
#  Licensed under the Apache License, Version 2.0 (the License);
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#  This file originally copied from https://github.com/sassoftware/python-swat
#
import unittest

from singlestoredb.config import check_bool
from singlestoredb.config import check_float
from singlestoredb.config import check_int
from singlestoredb.config import check_str
from singlestoredb.config import check_url
from singlestoredb.config import describe_option
from singlestoredb.config import get_default
from singlestoredb.config import get_option
from singlestoredb.config import get_suboptions
from singlestoredb.config import options
from singlestoredb.config import reset_option
from singlestoredb.config import set_option


class TestConfig(unittest.TestCase):

    def setUp(self):
        reset_option()

    def tearDown(self):
        reset_option()

    def test_basic(self):
        self.assertEqual(get_option('local_infile'), False)

        set_option('local_infile', True)

        self.assertEqual(get_option('local_infile'), True)

        with self.assertRaises(ValueError):
            options.local_infile = 'foo'

        options.local_infile = False
        self.assertEqual(options.local_infile, False)

        with self.assertRaises(ValueError):
            options.local_infile = 10

        self.assertEqual(options.local_infile, False)

        self.assertEqual(type(options.results), type(options))

        options.local_infile = False

        reset_option('local_infile')

        self.assertEqual(options.local_infile, False)

        with self.assertRaises(KeyError):
            reset_option('results.foo')

        with self.assertRaises(TypeError):
            reset_option('results')

    def test_shortcut_options(self):
        asize = get_option('results.arraysize')
        token = get_option('management.token')

        self.assertEqual(get_option('arraysize'), asize)
        self.assertEqual(options.arraysize, asize)

        options.arraysize = 20

        self.assertEqual(get_option('results.arraysize'), 20)
        self.assertEqual(options.results.arraysize, 20)
        self.assertEqual(options.arraysize, 20)

        self.assertEqual(get_option('token'), token)
        self.assertEqual(get_option('management.token'), token)
        self.assertEqual(options.token, token)

        options.token = 'Foo'

        self.assertEqual(get_option('token'), 'Foo')
        self.assertEqual(get_option('management.token'), 'Foo')
        self.assertEqual(options.token, 'Foo')

        reset_option('token')

        self.assertEqual(get_option('token'), token)
        self.assertEqual(get_option('management.token'), token)
        self.assertEqual(options.token, token)

    def test_missing_options(self):
        with self.assertRaises(KeyError):
            set_option('results.foo', 10)

        with self.assertRaises(KeyError):
            options.results.foo = 10

        with self.assertRaises(KeyError):
            get_option('results.foo')

        with self.assertRaises(KeyError):
            print(options.results.foo)

        # You can not access a midpoint in the hierarchy with (s|g)et_option
        with self.assertRaises(TypeError):
            set_option('results', 10)

        with self.assertRaises(TypeError):
            get_option('results')

    def test_errors(self):
        with self.assertRaises(ValueError):
            set_option('credential_type', 'foo')

    def test_doc(self):
        out = describe_option('results.arraysize', 'local_infile', _print_desc=False)
        for line in out.split('\n'):
            if not line or line.startswith(' '):
                continue
            self.assertRegex(line, r'^(results\.arraysize|local_infile)')

        # Displays entire option hierarchy
        out = describe_option('management', _print_desc=False)
        for line in out.split('\n'):
            if not line or line.startswith(' '):
                continue
            self.assertRegex(line, r'^management\.')

        with self.assertRaises(KeyError):
            describe_option('management.foo')

        out = describe_option(_print_desc=False)
        self.assertRegex(out, r'\bmanagement\.token :')
        self.assertRegex(out, r'\bhost :')
        self.assertRegex(out, r'\bport :')
        self.assertRegex(out, r'\buser :')
        self.assertRegex(out, r'\bpassword :')
        self.assertRegex(out, r'\bcharset :')

    def test_suboptions(self):
        self.assertEqual(
            list(sorted(get_suboptions('results').keys())),
            ['arraysize', 'type'],
        )

        with self.assertRaises(KeyError):
            get_suboptions('results.foo')

        # This is an option, not a level in the hierarchy
        with self.assertRaises(TypeError):
            get_suboptions('results.arraysize')

    def test_get_default(self):
        self.assertEqual(get_default('results.arraysize'), 1)

        with self.assertRaises(KeyError):
            get_default('results.foo')

        # This is a level in the hierarchy, not an option
        with self.assertRaises(TypeError):
            get_default('results')

    def test_check_int(self):
        self.assertEqual(check_int(10), 10)
        self.assertEqual(check_int(999999999999), 999999999999)
        self.assertEqual(check_int('10'), 10)

        with self.assertRaises(ValueError):
            check_int('foo')

        self.assertEqual(check_int(10, minimum=9), 10)
        self.assertEqual(check_int(10, minimum=10), 10)
        with self.assertRaises(ValueError):
            check_int(10, minimum=11)

        self.assertEqual(check_int(10, minimum=9, exclusive_minimum=True), 10)
        with self.assertRaises(ValueError):
            check_int(10, minimum=10, exclusive_minimum=True)
        with self.assertRaises(ValueError):
            check_int(10, minimum=11, exclusive_minimum=True)

        self.assertEqual(check_int(10, maximum=11), 10)
        self.assertEqual(check_int(10, maximum=10), 10)
        with self.assertRaises(ValueError):
            check_int(10, maximum=9)

        self.assertEqual(check_int(10, maximum=11, exclusive_minimum=True), 10)
        with self.assertRaises(ValueError):
            check_int(10, maximum=10, exclusive_maximum=True)
        with self.assertRaises(ValueError):
            check_int(10, maximum=9, exclusive_maximum=True)

        self.assertEqual(check_int(10, multiple_of=5), 10)
        with self.assertRaises(ValueError):
            check_int(10, multiple_of=3)

    def test_check_float(self):
        self.assertEqual(check_float(123.567), 123.567)
        self.assertEqual(check_float(999999999999.999), 999999999999.999)
        self.assertEqual(check_float('123.567'), 123.567)

        with self.assertRaises(ValueError):
            check_float('foo')

        self.assertEqual(check_float(123.567, minimum=123.566), 123.567)
        self.assertEqual(check_float(123.567, minimum=123.567), 123.567)
        with self.assertRaises(ValueError):
            check_float(123.567, minimum=123.577)

        self.assertEqual(
            check_float(
                123.567, minimum=123.566,
                exclusive_minimum=True,
            ), 123.567,
        )
        with self.assertRaises(ValueError):
            check_float(123.567, minimum=123.567, exclusive_minimum=True)
        with self.assertRaises(ValueError):
            check_float(123.567, minimum=123.568, exclusive_minimum=True)

        self.assertEqual(check_float(123.567, maximum=123.568), 123.567)
        self.assertEqual(check_float(123.567, maximum=123.567), 123.567)
        with self.assertRaises(ValueError):
            check_float(123.567, maximum=123.566)

        self.assertEqual(
            check_float(
                123.567, maximum=123.567,
                exclusive_minimum=True,
            ), 123.567,
        )
        with self.assertRaises(ValueError):
            check_float(123.567, maximum=123.567, exclusive_maximum=True)
        with self.assertRaises(ValueError):
            check_float(123.567, maximum=123.566, exclusive_maximum=True)

        with self.assertRaises(ValueError):
            check_float(123.567, multiple_of=3)

    def test_check_str(self):
        self.assertEqual(check_str('hi there'), 'hi there')
        self.assertTrue(isinstance(check_str('hi there'), str))

        self.assertEqual(check_str('hi there', pattern=r' th'), 'hi there')
        with self.assertRaises(ValueError):
            check_str('hi there', pattern=r' th$')

        self.assertEqual(check_str('hi there', max_length=20), 'hi there')
        self.assertEqual(check_str('hi there', max_length=8), 'hi there')
        with self.assertRaises(ValueError):
            check_str('hi there', max_length=7)

        self.assertEqual(check_str('hi there', min_length=3), 'hi there')
        self.assertEqual(check_str('hi there', min_length=8), 'hi there')
        with self.assertRaises(ValueError):
            check_str('hi there', min_length=9)

        self.assertEqual(
            check_str('hi there', valid_values=['hi there', 'bye now']),
            'hi there',
        )
        with self.assertRaises(ValueError):
            check_str('foo', valid_values=['hi there', 'bye now'])

        # Invalid utf8 data
        with self.assertRaises(ValueError):
            check_str(b'\xff\xfeW[')

    def test_check_url(self):
        self.assertEqual(check_url('hi there'), 'hi there')
        self.assertTrue(isinstance(check_url('hi there'), str))

        # Invalid utf8 data
        with self.assertRaises(ValueError):
            check_url(b'\xff\xfeW[')

    def test_check_bool(self):
        self.assertEqual(check_bool(True), True)
        self.assertEqual(check_bool(False), False)
        self.assertEqual(check_bool(1), True)
        self.assertEqual(check_bool(0), False)
        self.assertEqual(check_bool('yes'), True)
        self.assertEqual(check_bool('no'), False)
        self.assertEqual(check_bool('T'), True)
        self.assertEqual(check_bool('F'), False)
        self.assertEqual(check_bool('true'), True)
        self.assertEqual(check_bool('false'), False)
        self.assertEqual(check_bool('on'), True)
        self.assertEqual(check_bool('off'), False)
        self.assertEqual(check_bool('enabled'), True)
        self.assertEqual(check_bool('disabled'), False)

        with self.assertRaises(ValueError):
            check_bool(2)
        with self.assertRaises(ValueError):
            check_bool('foo')
        with self.assertRaises(ValueError):
            check_bool(1.1)


if __name__ == '__main__':
    import nose2
    nose2.main()
