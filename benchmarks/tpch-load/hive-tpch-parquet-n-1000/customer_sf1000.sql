-- Inserting into customer
INSERT INTO customer
SELECT
    c_custkey,
    CAST(c_name AS VARCHAR(25)),
    CAST(c_address AS VARCHAR(40)),
    c_nationkey,
    CAST(c_phone AS VARCHAR(15)),
    CAST(c_acctbal AS DECIMAL(12,2)),
    CAST(c_mktsegment AS VARCHAR(10)),
    CAST(c_comment AS VARCHAR(117))
FROM tpch.sf1000.customer;