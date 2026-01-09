-- Test data for SingleStore 9.1+ features
-- This file is automatically loaded by utils.py if server version >= 9.1

-- Float16 (half-precision) vectors
CREATE TABLE IF NOT EXISTS `f16_vectors` (
    id INT(11),
    a VECTOR(3, F16)
);
INSERT INTO f16_vectors VALUES(1, '[0.267, 0.535, 0.802]');
INSERT INTO f16_vectors VALUES(2, '[0.371, 0.557, 0.743]');
INSERT INTO f16_vectors VALUES(3, '[-0.424, -0.566, 0.707]');
