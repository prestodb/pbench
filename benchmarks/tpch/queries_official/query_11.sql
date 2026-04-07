--TPCH Q11
select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    partsupp AS ps,
    supplier AS s,
    nation AS n
where
    ps_suppkey = s_suppkey
  and s_nationkey = n_nationkey
  and n_name = 'GERMANY'
group by
    ps_partkey having
    sum(ps_supplycost * ps_availqty) > (
    select
    sum(ps_supplycost * ps_availqty) * 0.0001
    from
    partsupp AS ps,
    supplier AS s,
    nation AS n
    where
    ps_suppkey = s_suppkey
                  and s_nationkey = n_nationkey
                  and n_name = 'GERMANY'
    )
order by
    value desc;
