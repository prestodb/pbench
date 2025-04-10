-- Inserting into customer
INSERT INTO customer
SELECT
    custkey,
    CAST(name AS VARCHAR(25)),
    CAST(address AS VARCHAR(40)),
    nationkey,
    CAST(phone AS VARCHAR(15)),
    CAST(acctbal AS DECIMAL(12,2)),
    CAST(mktsegment AS VARCHAR(10)),
    CAST(comment AS VARCHAR(117))
FROM hive.tpch_sf10_parquet.customer;