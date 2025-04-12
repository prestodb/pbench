-- Inserting into lineitem
INSERT INTO lineitem
SELECT
    l_orderkey,
    l_partkey,
    l_suppkey,
    l_linenumber,
    CAST(l_quantity AS DECIMAL(12,2)),
    CAST(l_extendedprice AS DECIMAL(12,2)),
    CAST(l_discount AS DECIMAL(12,2)),
    CAST(l_tax AS DECIMAL(12,2)),
    CAST(l_returnflag AS VARCHAR(1)),
    CAST(l_linestatus AS VARCHAR(1)),
    l_shipdate,
    l_commitdate,
    l_receiptdate,
    CAST(l_shipinstruct AS VARCHAR(25)),
    CAST(l_shipmode AS VARCHAR(10)),
    CAST(l_comment AS VARCHAR(44))
FROM tpch.sf1000.lineitem;