-- Inserting into supplier
INSERT INTO supplier
SELECT
    suppkey,
    CAST(name AS VARCHAR(25)),
    CAST(address AS VARCHAR(40)),
    nationkey,
    CAST(phone AS VARCHAR(15)),
    CAST(acctbal AS DECIMAL(12,2)),
    CAST(comment AS VARCHAR(101))
FROM tpch.sf1.supplier;