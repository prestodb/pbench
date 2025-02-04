--TPCH Q2
SELECT
    s.acctbal,
    s.name as s_name,
    n.name as n_name,
    p.partkey,
    p.mfgr,
    s.address,
    s.phone,
    s.comment
FROM
    part p,
    supplier s,
    partsupp ps,
    nation n,
    region r
WHERE
    p.partkey = ps.partkey
  AND s.suppkey = ps.suppkey
  AND p.size = 15
  AND p.type like '%BRASS'
  AND s.nationkey = n.nationkey
  AND n.regionkey = r.regionkey
  AND r.name = 'EUROPE'
  AND ps.supplycost = (
    SELECT
        min(ps.supplycost)
    FROM
        partsupp ps,
        supplier s,
        nation n,
        region r
    WHERE
        p.partkey = ps.partkey
      AND s.suppkey = ps.suppkey
      AND s.nationkey = n.nationkey
      AND n.regionkey = r.regionkey
      AND r.name = 'EUROPE'
)
ORDER BY
    s.acctbal desc,
    n.name,
    s.name,
    p.partkey
    LIMIT 100;
