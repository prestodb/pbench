-- Create tables
CREATE TABLE t1 (
    id INT,
    a INT
);

CREATE TABLE t2 (
    id INT,
    b INT,
    c INT
);

-- Insert sample data
INSERT INTO t1 (id, a) VALUES
(1, 15),
(2, 25),
(3, 35);

INSERT INTO t2 (id, b, c) VALUES
(1, 5, 10),
(2, 10, 15),
(3, 15, 20);

-- Perform the join
SELECT
    t1.id AS t1_id,
    t1.a AS t1_a,
    t2.id AS t2_id,
    t2.b AS t2_b,
    t2.c AS t2_c
FROM
    t1
JOIN
    t2 ON t1.a + 2 = (t2.b + 1) - 2 * ABS(t2.c);
