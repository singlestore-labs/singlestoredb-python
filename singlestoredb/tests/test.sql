
CREATE ROWSTORE TABLE IF NOT EXISTS data (
    id VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    value BIGINT NOT NULL,
    PRIMARY KEY (id) USING HASH
) DEFAULT CHARSET = utf8 COLLATE = utf8_unicode_ci;

INSERT INTO data SET id='a', name='antelopes', value=2;
INSERT INTO data SET id='b', name='bears', value=2;
INSERT INTO data SET id='c', name='cats', value=5;
INSERT INTO data SET id='d', name='dogs', value=4;
INSERT INTO data SET id='e', name='elephants', value=0;

COMMIT;

CREATE ROWSTORE TABLE IF NOT EXISTS data_with_nulls (
    id VARCHAR(255) NOT NULL,
    name VARCHAR(255),
    value BIGINT,
    PRIMARY KEY (id) USING HASH
) DEFAULT CHARSET = utf8 COLLATE = utf8_unicode_ci;

INSERT INTO data_with_nulls SET id='a', name='antelopes', value=2;
INSERT INTO data_with_nulls SET id='b', name=NULL, value=2;
INSERT INTO data_with_nulls SET id='c', name=NULL, value=5;
INSERT INTO data_with_nulls SET id='d', name='dogs', value=NULL;
INSERT INTO data_with_nulls SET id='e', name='elephants', value=0;

COMMIT;

CREATE OR REPLACE PROCEDURE get_animal(nm VARCHAR(255) NOT NULL COLLATE utf8_unicode_ci) AS
BEGIN
    ECHO SELECT value FROM data WHERE name = nm; --
    ECHO SELECT 1, 2, 3; --
END;

CREATE OR REPLACE PROCEDURE no_args() AS
BEGIN
    ECHO SELECT 4, 5, 6; --
END;

CREATE OR REPLACE PROCEDURE return_int() RETURNS BIGINT AS
BEGIN
    RETURN 1234567890; --
END;

CREATE OR REPLACE PROCEDURE result_set_and_return_int() RETURNS BIGINT AS
BEGIN
    ECHO SELECT value FROM data WHERE name = 'cats'; --
    ECHO SELECT 1, 2, 3; --
    RETURN 1234567890; --
END;

COMMIT;

CREATE TABLE IF NOT EXISTS alltypes (
    `id` INT(11),
    `tinyint` TINYINT,
    `unsigned_tinyint` TINYINT UNSIGNED,
    `bool` BOOL,
    `boolean` BOOLEAN,
    `smallint` SMALLINT,
    `unsigned_smallint` SMALLINT UNSIGNED,
    `mediumint` MEDIUMINT,
    `unsigned_mediumint` MEDIUMINT UNSIGNED,
    `int24` MEDIUMINT,
    `unsigned_int24` MEDIUMINT UNSIGNED,
    `int` INT,
    `unsigned_int` INT UNSIGNED,
    `integer` INTEGER,
    `unsigned_integer` INTEGER UNSIGNED,
    `bigint` BIGINT,
    `unsigned_bigint` BIGINT UNSIGNED,
    `float` FLOAT,
    `double` DOUBLE,
    `real` REAL,
    `decimal` DECIMAL(20,6),
    `dec` DEC(20,6),
    `fixed` FIXED(20,6),
    `numeric` NUMERIC(20,6),
    `date` DATE,
    `time` TIME,
    `time_6` TIME(6),
    `datetime` DATETIME,
    `datetime_6` DATETIME(6),
    `timestamp` TIMESTAMP,
    `timestamp_6` TIMESTAMP(6),
    `year` YEAR,
    `char_100` CHAR(100),
    `binary_100` BINARY(100),
    `varchar_200` VARCHAR(200),
    `varbinary_200` VARBINARY(200),
    `longtext` LONGTEXT,
    `mediumtext` MEDIUMTEXT,
    `text` TEXT,
    `tinytext` TINYTEXT,
    `longblob` LONGBLOB,
    `mediumblob` MEDIUMBLOB,
    `blob` BLOB,
    `tinyblob` TINYBLOB,
    `json` JSON,
--  `geographypoint` GEOGRAPHYPOINT,
--  `geography` GEOGRAPHY,
    `enum` ENUM('one', 'two', 'three'),
    `set` SET('one', 'two', 'three'),
    `bit` BIT
)
COLLATE='utf8_unicode_ci';

INSERT INTO alltypes SET
    `id`=0,
    `tinyint`=80,
    `unsigned_tinyint`=85,
    `bool`=0,
    `boolean`=1,
    `smallint`=-27897,
    `unsigned_smallint`=27897,
    `mediumint`=104729,
    `unsigned_mediumint`=120999,
    `int24`=-200899,
    `unsigned_int24`=407709,
    `int`=-1295369311,
    `unsigned_int`=3872362332,
    `integer`=-1741727421,
    `unsigned_integer`=3198387363,
    `bigint`=-266883847,
    `unsigned_bigint`=980007287362,
    `float`=-146486683.754744,
    `double`=-474646154.719356,
    `real`=-901409776.279346,
    `decimal`=28111097.610822,
    `dec`=389451155.931428,
    `fixed`=-143773416.044092,
    `numeric`=866689461.300046,
    `date`='8524-11-10',
    `time`='00:07:00',
    `time_6`='01:10:00.000002',
    `datetime`='9948-03-11 15:29:22',
    `datetime_6`='1756-10-29 02:02:42.000008',
    `timestamp`='1980-12-31 01:10:23',
    `timestamp_6`='1991-01-02 22:15:10.000006',
    `year`=1923,
    `char_100`='This is a test of a 100 character column.',
    `binary_100`=x'000102030405060708090A0B0C0D0E0F',
    `varchar_200`='This is a test of a variable character column.',
    `varbinary_200`=x'000102030405060708090A0B0C0D0E0F000102030405060708090A0B0C0D0E0F',
    `longtext`='This is a longtext column.',
    `mediumtext`='This is a mediumtext column.',
    `text`='This is a text column.',
    `tinytext`='This is a tinytext column.',
    `longblob`=x'000102030405060708090A0B0C0D0E0F000102030405060708090A0B0C0D0E0F000102030405060708090A0B0C0D0E0F',
    `mediumblob`=x'000102030405060708090A0B0C0D0E0F000102030405060708090A0B0C0D0E0F',
    `blob`=x'000102030405060708090A0B0C0D0E0F',
    `tinyblob`=x'0A0B0C0D0E0F',
    `json`='{"a": 10, "b": 2.75, "c": "hello world"}',
    `enum`='one',
    `set`='two',
    `bit`=128
;

INSERT INTO alltypes SET
    `id`=1,
    `tinyint`=NULL,
    `bool`=NULL,
    `boolean`=NULL,
    `smallint`=NULL,
    `mediumint`=NULL,
    `int24`=NULL,
    `int`=NULL,
    `integer`=NULL,
    `bigint`=NULL,
    `float`=NULL,
    `double`=NULL,
    `real`=NULL,
    `decimal`=NULL,
    `dec`=NULL,
    `fixed`=NULL,
    `numeric`=NULL,
    `date`=NULL,
    `time`=NULL,
    `time_6`=NULL,
    `datetime`=NULL,
    `datetime_6`=NULL,
    `timestamp`=NULL,
    `timestamp_6`=NULL,
    `year`=NULL,
    `char_100`=NULL,
    `binary_100`=NULL,
    `varchar_200`=NULL,
    `longtext`=NULL,
    `mediumtext`=NULL,
    `text`=NULL,
    `tinytext`=NULL,
    `longblob`=NULL,
    `mediumblob`=NULL,
    `blob`=NULL,
    `tinyblob`=NULL,
    `json`=NULL,
    `enum`=NULL,
    `set`=NULL,
    `bit`=NULL
;

-- Minimum values
INSERT INTO alltypes SET
    `id`=2,
    `tinyint`=-128,
    `unsigned_tinyint`=0,
    `bool`=-128,
    `boolean`=-128,
    `smallint`=-32768,
    `unsigned_smallint`=0,
    `mediumint`=-8388608,
    `unsigned_mediumint`=0,
    `int24`=-8388608,
    `unsigned_int24`=0,
    `int`=-2147483648,
    `unsigned_int`=0,
    `integer`=-2147483648,
    `unsigned_integer`=0,
    `bigint`=-9223372036854775808,
    `unsigned_bigint`=0,
    `float`=0,
    `double`=-1.7976931348623158e308,
    `real`=-1.7976931348623158e308,
    `decimal`=-99999999999999.999999,
    `dec`=-99999999999999.999999,
    `fixed`=-99999999999999.999999,
    `numeric`=-99999999999999.999999,
    `date`='1000-01-01',
    `time`='-838:59:59',
    `time_6`='-838:59:59.000000',
    `datetime`='1000-01-01 00:00:00',
    `datetime_6`='1000-01-01 00:00:00.000000',
    `timestamp`='1970-01-01 00:00:01',
    `timestamp_6`='1970-01-01 00:00:01.000000',
    `year`=1901,
    `char_100`='',
    `binary_100`=x'',
    `varchar_200`='',
    `varbinary_200`=x'',
    `longtext`='',
    `mediumtext`='',
    `text`='',
    `tinytext`='',
    `longblob`=x'',
    `mediumblob`=x'',
    `blob`=x'',
    `tinyblob`=x'',
    `json`='{}',
    `enum`='one',
    `set`='two',
    `bit`=0
;

-- Maximum values
INSERT INTO alltypes SET
    `id`=3,
    `tinyint`=127,
    `unsigned_tinyint`=255,
    `bool`=127,
    `boolean`=127,
    `smallint`=32767,
    `unsigned_smallint`=65535,
    `mediumint`=8388607,
    `unsigned_mediumint`=16777215,
    `int24`=8388607,
    `unsigned_int24`=16777215,
    `int`=2147483647,
    `unsigned_int`=4294967295,
    `integer`=2147483647,
    `unsigned_integer`=4294967295,
    `bigint`=9223372036854775807,
    `unsigned_bigint`=18446744073709551615,
    `float`=0,
    `double`=1.7976931348623158e308,
    `real`=1.7976931348623158e308,
    `decimal`=99999999999999.999999,
    `dec`=99999999999999.999999,
    `fixed`=99999999999999.999999,
    `numeric`=99999999999999.999999,
    `date`='9999-12-31',
    `time`='838:59:59',
    `time_6`='838:59:59.999999',
    `datetime`='9999-12-31 23:59:59',
    `datetime_6`='9999-12-31 23:59:59.999999',
    `timestamp`='2038-01-18 21:14:07',
    `timestamp_6`='2038-01-18 21:14:07.999999',
    `year`=2155,
    `char_100`='',
    `binary_100`=x'',
    `varchar_200`='',
    `varbinary_200`=x'',
    `longtext`='',
    `mediumtext`='',
    `text`='',
    `tinytext`='',
    `longblob`=x'',
    `mediumblob`=x'',
    `blob`=x'',
    `tinyblob`=x'',
    `json`='{}',
    `enum`='one',
    `set`='two',
    `bit`=18446744073709551615
;

-- Zero values
--
-- Note that v8 of SingleStoreDB does not allow zero date/times by
-- default, so they are set to NULL here.
--
INSERT INTO alltypes SET
    `id`=4,
    `tinyint`=0,
    `unsigned_tinyint`=0,
    `bool`=0,
    `boolean`=0,
    `smallint`=0,
    `unsigned_smallint`=0,
    `mediumint`=0,
    `unsigned_mediumint`=0,
    `int24`=0,
    `unsigned_int24`=0,
    `int`=0,
    `unsigned_int`=0,
    `integer`=0,
    `unsigned_integer`=0,
    `bigint`=0,
    `unsigned_bigint`=0,
    `float`=0,
    `double`=0.0,
    `real`=0.0,
    `decimal`=0.0,
    `dec`=0.0,
    `fixed`=0.0,
    `numeric`=0.0,
    `date`=NULL,
    `time`='00:00:00',
    `time_6`='00:00:00.000000',
    `datetime`=NULL,
    `datetime_6`=NULL,
    `timestamp`=NULL,
    `timestamp_6`=NULL,
    `year`=NULL,
    `char_100`='',
    `binary_100`=x'',
    `varchar_200`='',
    `varbinary_200`=x'',
    `longtext`='',
    `mediumtext`='',
    `text`='',
    `tinytext`='',
    `longblob`=x'',
    `mediumblob`=x'',
    `blob`=x'',
    `tinyblob`=x'',
    `json`='{}',
    `enum`='one',
    `set`='two',
    `bit`=0
;


--
-- Table of extended data types
--
CREATE ROWSTORE TABLE IF NOT EXISTS `extended_types` (
    `id` INT(11),
    `geography` GEOGRAPHY,
    `geographypoint` GEOGRAPHYPOINT,
    `vectors` BLOB,
    `dt` DATETIME,
    `d` DATE,
    `t` TIME,
    `td` TIME,
    `testkey` LONGTEXT
)
COLLATE='utf8_unicode_ci';


--
-- Invalid utf8 table
--
-- These sequences were breaking during fetch on a customer's machine
-- however, they seem to work fine in our tests.
--
CREATE TABLE IF NOT EXISTS `badutf8` (
    `text` TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci
)
COLLATE='utf8_unicode_ci';


INSERT INTO `badutf8` VALUES ('🥷🧙👻.eth');
INSERT INTO `badutf8` VALUES ('🥒rick.eth');


COMMIT;
