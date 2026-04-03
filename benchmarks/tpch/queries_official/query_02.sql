--TPCH Q2
SELECT
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
FROM
    part p,
    supplier s,
    partsupp ps,
    nation n,
    region r
WHERE
    p_partkey = ps_partkey
  AND s_suppkey = ps_suppkey
  AND p_size = 15
  AND p_type like '%BRASS'
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'EUROPE'
  AND ps_supplycost = (
    SELECT
        min(ps_supplycost)
    FROM
        partsupp ps,
        supplier s,
        nation n,
        region r
    WHERE
        p_partkey = ps_partkey
      AND s_suppkey = ps_suppkey
      AND s_nationkey = n_nationkey
      AND n_regionkey = r_regionkey
      AND r_name = 'EUROPE'
)
ORDER BY
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
    LIMIT 100;
