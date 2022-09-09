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

COMMIT;
