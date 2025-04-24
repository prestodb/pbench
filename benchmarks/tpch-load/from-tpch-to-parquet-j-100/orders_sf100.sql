-- Inserting into orders
INSERT INTO orders
SELECT
    orderkey,
    custkey,
    CAST(orderstatus AS VARCHAR(1)),
    CAST(totalprice AS DECIMAL(12,2)),
    orderdate,
    CAST(orderpriority AS VARCHAR(15)),
    CAST(clerk AS VARCHAR(15)),
    shippriority,
    CAST(comment AS VARCHAR(79))
FROM tpch.sf100.orders;
