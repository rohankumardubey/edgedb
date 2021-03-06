#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import os.path

from edb.testbase import server as tb


class DumpTestCaseMixin:

    async def ensure_schema_data_integrity(self):
        tx = self.con.transaction()
        await tx.start()
        try:
            await self._ensure_schema_data_integrity()
        finally:
            await tx.rollback()

    async def _ensure_schema_data_integrity(self):
        await self.assert_query_result(
            r'''
                SELECT A {
                    `s p A m ð¤`: {
                        `ð`,
                        c100,
                        c101 := `ð¯`(`ð` := .`ð` + 1)
                    }
                }
            ''',
            [
                {
                    's p A m ð¤': {
                        'ð': 42,
                        'c100': 58,
                        'c101': 57,
                    }
                }
            ]
        )

        await self.assert_query_result(
            r'''
                SELECT Åukasz {
                    `Åð¤`,
                    `Åð¯`: {
                        @`ððððð`,
                        @`ðÙØ±Ø­Ø¨Ø§ð`,
                        `s p A m ð¤`: {
                            `ð`,
                            c100,
                            c101 := `ð¯`(`ð` := .`ð` + 1)
                        }
                    }
                } ORDER BY .`Åð¯` EMPTY LAST
            ''',
            [
                {
                    'Åð¤': 'simple ð',
                    'Åð¯': {
                        '@ððððð': None,
                        '@ðÙØ±Ø­Ø¨Ø§ð': None,
                        's p A m ð¤': {
                            'ð': 42,
                            'c100': 58,
                            'c101': 57,
                        }
                    }
                },
                {
                    'Åð¤': 'ä½ å¥½ð¤',
                    'Åð¯': None,
                },
            ]
        )

        await self.assert_query_result(
            r'''
                SELECT `ð¯ð¯ð¯`::`ððð`('Åink prop ðÙØ±Ø­Ø¨Ø§ð');
            ''',
            [
                'Åink prop ðÙØ±Ø­Ø¨Ø§ðÅð',
            ]
        )

        # Check that annotation exists
        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT Function {
                    name,
                    annotations: {
                        name,
                        @value
                    },
                } FILTER .name = 'default::ð¯';
            ''',
            [
                {
                    'name': 'default::ð¯',
                    'annotations': [{
                        'name': 'default::ð¿',
                        '@value': 'fun!ð',
                    }]
                }
            ]
        )

        # Check that index exists
        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr,
                    },
                    properties: {
                        name,
                        default,
                    } FILTER .name != 'id',
                } FILTER .name = 'default::Åukasz';
            ''',
            [
                {
                    'name': 'default::Åukasz',
                    'indexes': [{
                        'expr': '.`Åð¤`'
                    }],
                }
            ]
        )

        # Check that scalar types exist
        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT (
                    SELECT ScalarType {
                        name,
                    } FILTER .name LIKE 'default%'
                ).name;
            ''',
            {
                'default::ä½ å¥½',
                'default::ÙØ±Ø­Ø¨Ø§',
                'default::ððð',
            }
        )

        # Check that abstract constraint exists
        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT Constraint {
                    name,
                } FILTER .name LIKE 'default%' AND .abstract;
            ''',
            [
                {'name': 'default::ðð¿'},
            ]
        )

        # Check that abstract constraint was applied properly
        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT Constraint {
                    name,
                    params: {
                        @value
                    } FILTER .num > 0
                }
                FILTER
                    .name = 'default::ðð¿' AND
                    NOT .abstract AND
                    Constraint.<constraints[IS ScalarType].name =
                        'default::ððð';
            ''',
            [
                {
                    'name': 'default::ðð¿',
                    'params': [
                        {'@value': '100'}
                    ]
                },
            ]
        )

        # Check the default value
        await self.con.execute(r'INSERT Åukasz')
        await self.assert_query_result(
            r'''
                SELECT Åukasz {
                    `Åð¤`,
                } FILTER NOT EXISTS .`Åð¯`;
            ''',
            [
                # We had one before and expect one more now.
                {'Åð¤': 'ä½ å¥½ð¤'},
                {'Åð¤': 'ä½ å¥½ð¤'},
            ]
        )

        await self.assert_query_result(
            r'''
                SELECT count(schema::Migration) >= 2
            ''',
            [True],
        )


class TestDump02(tb.StableDumpTestCase, DumpTestCaseMixin):
    DEFAULT_MODULE = 'test'

    SCHEMA_DEFAULT = os.path.join(os.path.dirname(__file__), 'schemas',
                                  'dump02_default.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'dump02_setup.edgeql')

    TEARDOWN = '''
        CONFIGURE CURRENT DATABASE RESET allow_dml_in_functions;
    '''

    @classmethod
    def get_setup_script(cls):
        script = (
            'CONFIGURE CURRENT DATABASE SET allow_dml_in_functions := true;\n'
        )
        return script + super().get_setup_script()

    async def test_dump02_dump_restore(self):
        await self.check_dump_restore(
            DumpTestCaseMixin.ensure_schema_data_integrity)


class TestDump02Compat(
    tb.DumpCompatTestCase,
    DumpTestCaseMixin,
    dump_subdir='dump02',
    check_method=DumpTestCaseMixin.ensure_schema_data_integrity,
):
    @classmethod
    def tearDownClass(cls):
        try:
            cls.loop.run_until_complete(cls.con.execute('''
                CONFIGURE CURRENT DATABASE RESET allow_dml_in_functions;
            '''))
        finally:
            super().tearDownClass()
