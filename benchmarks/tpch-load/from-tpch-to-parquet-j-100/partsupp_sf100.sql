-- Inserting into partsupp
INSERT INTO partsupp
SELECT
    partkey,
    suppkey,
    availqty,
    CAST(supplycost AS DECIMAL(12,2)),
    CAST(comment AS VARCHAR(199))
FROM tpch.sf100.partsupp;
