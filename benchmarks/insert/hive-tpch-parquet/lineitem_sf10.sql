-- Inserting into lineitem
INSERT INTO lineitem
SELECT
    orderkey,
    partkey,
    suppkey,
    linenumber,
    CAST(quantity AS DECIMAL(12,2)),
    CAST(extendedprice AS DECIMAL(12,2)),
    CAST(discount AS DECIMAL(12,2)),
    CAST(tax AS DECIMAL(12,2)),
    CAST(returnflag AS VARCHAR(1)),
    CAST(linestatus AS VARCHAR(1)),
    shipdate,
    commitdate,
    receiptdate,
    CAST(shipinstruct AS VARCHAR(25)),
    CAST(shipmode AS VARCHAR(10)),
    CAST(comment AS VARCHAR(44))
FROM tpch.sf1.lineitem;