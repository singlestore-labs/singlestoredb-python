#!/usr/bin/env python
# type: ignore
from __future__ import annotations

import singlestore as s2

commands = r'''
DROP DATABASE IF EXISTS app;

CREATE DATABASE app;

USE app;

CREATE ROWSTORE TABLE IF NOT EXISTS data (
    id VARCHAR(255) NOT NULL,
    value VARCHAR(255) NOT NULL,
    PRIMARY KEY (id) USING HASH
) DEFAULT CHARSET = utf8 COLLATE = utf8_unicode_ci;
'''

with s2.connect() as conn:
    with conn.cursor() as cur:
        for cmd in commands.split(';\n'):
            cmd = cmd.strip()
            if cmd:
                cmd += ';'
                print(cmd)
                cur.execute(cmd)
