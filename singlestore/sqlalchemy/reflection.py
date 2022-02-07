#!/usr/bin/env python
"""SingleStore SQLAlchemy reflection utilities."""
from __future__ import annotations

import re

from sqlalchemy import log
from sqlalchemy.dialects.mysql.reflection import _re_compile
from sqlalchemy.dialects.mysql.reflection import MySQLTableDefinitionParser


@log.class_logger
class SingleStoreTableDefinitionParser(MySQLTableDefinitionParser):
    """Parses the results of a SHOW CREATE TABLE statement."""

    def _prep_regexes(self) -> None:
        """Pre-compile regular expressions."""
        super(SingleStoreTableDefinitionParser, self)._prep_regexes()

        quotes = dict(
            zip(
                ('iq', 'fq', 'esc_fq'),
                [
                    re.escape(s)
                    for s in (
                        self.preparer.initial_quote,
                        self.preparer.final_quote,
                        self.preparer._escape_identifier(self.preparer.final_quote),
                    )
                ],
            ),
        )

        # (PRIMARY|UNIQUE|FULLTEXT|SPATIAL) INDEX `name` (USING (BTREE|HASH))?
        # (`col` (ASC|DESC)?, `col` (ASC|DESC)?)
        # KEY_BLOCK_SIZE size | WITH PARSER name  /*!50100 WITH PARSER name */
        self._re_key = _re_compile(
            r'  '
            r'(?:(?P<type>\S+) )?KEY'
            r'(?: +%(iq)s(?P<name>(?:%(esc_fq)s|[^%(fq)s])+)%(fq)s)?'
            r'(?: +USING +(?P<using_pre>\S+))?'
            r' +\((?P<columns>.*?)\)'
            r'(?: +USING +(?P<using_post>\S+|CLUSTERED +COLUMNSTORE))?'
            r'(?: +KEY_BLOCK_SIZE *[ =]? *(?P<keyblock>\S+))?'
            r'(?: +WITH PARSER +(?P<parser>\S+))?'
            r'(?: +COMMENT +(?P<comment>(\x27\x27|\x27([^\x27])*?\x27)+))?'
            r'(?: +/\*(?P<version_sql>.+)\*/ *)?'
            r',?$' % quotes,
        )
