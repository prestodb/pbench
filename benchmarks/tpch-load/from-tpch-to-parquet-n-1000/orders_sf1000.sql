-- Inserting into orders
INSERT INTO orders
SELECT
    o_orderkey,
    o_custkey,
    CAST(o_orderstatus AS VARCHAR(1)),
    CAST(o_totalprice AS DECIMAL(12,2)),
    o_orderdate,
    CAST(o_orderpriority AS VARCHAR(15)),
    CAST(o_clerk AS VARCHAR(15)),
    o_shippriority,
    CAST(o_comment AS VARCHAR(79))
FROM tpch.sf1000.orders;