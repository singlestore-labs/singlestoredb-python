DROP DATABASE IF EXISTS app;

CREATE DATABASE app;

USE app;

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

CREATE TABLE IF NOT EXISTS alltypes (
    `id` INT(11) NOT NULL,
    `tinyint` TINYINT NOT NULL,
    `bool` BOOL NOT NULL,
    `boolean` BOOLEAN NOT NULL,
    `smallint` SMALLINT NOT NULL,
    `mediumint` MEDIUMINT NOT NULL,
    `int24` MEDIUMINT NOT NULL,
    `int` INT NOT NULL,
    `integer` INTEGER NOT NULL,
    `bigint` BIGINT NOT NULL,
    `float` FLOAT NOT NULL,
    `double` DOUBLE NOT NULL,
    `real` REAL NOT NULL,
    `decimal` DECIMAL(20,6) NOT NULL,
    `dec` DEC(20,6) NOT NULL,
    `fixed` FIXED(20,6) NOT NULL,
    `numeric` NUMERIC(20,6) NOT NULL,
    `date` DATE NOT NULL,
    `time` TIME NOT NULL,
    `time_6` TIME(6) NOT NULL,
    `datetime` DATETIME NOT NULL,
    `datetime_6` DATETIME(6) NOT NULL,
    `timestamp` TIMESTAMP NOT NULL,
    `timestamp_6` TIMESTAMP(6) NOT NULL,
    `year` YEAR NOT NULL,
    `char_100` CHAR(100) NOT NULL,
    `binary_100` BINARY(100) NOT NULL,
    `varchar_200` VARCHAR(200) NOT NULL,
    `varbinary_200` VARBINARY(200) NOT NULL,
    `longtext` LONGTEXT NOT NULL,
    `mediumtext` MEDIUMTEXT NOT NULL,
    `text` TEXT NOT NULL,
    `tinytext` TINYTEXT NOT NULL,
    `longblob` LONGBLOB NOT NULL,
    `mediumblob` MEDIUMBLOB NOT NULL,
    `blob` BLOB NOT NULL,
    `tinyblob` TINYBLOB NOT NULL,
    `json` JSON NOT NULL,
--  `geographypoint` GEOGRAPHYPOINT NOT NULL,
--  `geography` GEOGRAPHY NOT NULL,
    `enum` ENUM('one', 'two', 'three') NOT NULL,
    `set` SET('one', 'two', 'three') NOT NULL,
    `bit` BIT NOT NULL
)
COLLATE='utf8_unicode_ci';

INSERT INTO alltypes SET
    `id`=0,
    `tinyint`=80,
    `bool`=0,
    `boolean`=1,
    `smallint`=-27897,
    `mediumint`=104729,
    `int24`=-200899,
    `int`=-1295369311,
    `integer`=-1741727421,
    `bigint`=-266883847,
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

COMMIT;
