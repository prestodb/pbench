--TPCH Q15
WITH revenue AS (
    SELECT
        l.suppkey,
        SUM(l.extendedprice * (1 - l.discount)) AS total_revenue
    FROM lineitem l
    WHERE l.shipdate >= DATE '1996-01-01'
      AND l.shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH
    GROUP BY l.suppkey
)
SELECT
    s.suppkey,
    s.name,
    s.address,
    s.phone,
    r.total_revenue
FROM supplier s
JOIN revenue r
    ON s.suppkey = r.suppkey
WHERE r.total_revenue = (
    SELECT MAX(total_revenue)
    FROM revenue
)
ORDER BY s.suppkey;
