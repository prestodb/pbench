-- Inserting into supplier
INSERT INTO supplier
SELECT
    s_suppkey,
    CAST(s_name AS VARCHAR(25)),
    CAST(s_address AS VARCHAR(40)),
    s_nationkey,
    CAST(s_phone AS VARCHAR(15)),
    CAST(s_acctbal AS DECIMAL(12,2)),
    CAST(s_comment AS VARCHAR(101))
FROM tpch.sf100.supplier;
