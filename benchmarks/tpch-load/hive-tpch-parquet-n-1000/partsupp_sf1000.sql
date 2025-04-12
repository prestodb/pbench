-- Inserting into partsupp
INSERT INTO partsupp
SELECT
    ps_partkey,
    ps_suppkey,
    ps_availqty,
    CAST(ps_supplycost AS DECIMAL(12,2)),
    CAST(ps_comment AS VARCHAR(199))
FROM tpch.sf1000.partsupp;