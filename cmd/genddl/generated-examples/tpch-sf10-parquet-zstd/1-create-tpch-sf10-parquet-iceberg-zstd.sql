SET SESSION iceberg.compression_codec='ZSTD';
SET SESSION query_max_execution_time='12h';
SET SESSION query_max_run_time='12h';

CREATE SCHEMA IF NOT EXISTS iceberg.tpch_sf10_parquet_iceberg_zstd
WITH (
    location = 's3a://presto-workload-v2/tpch-sf10-parquet-iceberg-zstd/'
);

USE iceberg.tpch_sf10_parquet_iceberg_zstd;

CREATE TABLE IF NOT EXISTS customer (
    c_custkey BIGINT,
    c_name VARCHAR(25),
    c_address VARCHAR(40),
    c_nationkey BIGINT,
    c_phone VARCHAR(15),
    c_acctbal DOUBLE,
    c_mktsegment VARCHAR(10),
    c_comment VARCHAR(117)
)
WITH (
    format = 'PARQUET',
    location = 's3a://presto-workload-v2/tpch-sf10-parquet-iceberg-zstd/customer'
);

CREATE TABLE IF NOT EXISTS lineitem (
    l_orderkey BIGINT,
    l_partkey BIGINT,
    l_suppkey BIGINT,
    l_linenumber INTEGER,
    l_quantity DOUBLE,
    l_extendedprice DOUBLE,
    l_discount DOUBLE,
    l_tax DOUBLE,
    l_returnflag VARCHAR(1),
    l_linestatus VARCHAR(1),
    l_shipdate DATE,
    l_commitdate DATE,
    l_receiptdate DATE,
    l_shipinstruct VARCHAR(25),
    l_shipmode VARCHAR(10),
    l_comment VARCHAR(44)
)
WITH (
    format = 'PARQUET',
    location = 's3a://presto-workload-v2/tpch-sf10-parquet-iceberg-zstd/lineitem'
);

CREATE TABLE IF NOT EXISTS nation (
    n_nationkey BIGINT,
    n_name VARCHAR(25),
    n_regionkey BIGINT,
    n_comment VARCHAR(152)
)
WITH (
    format = 'PARQUET',
    location = 's3a://presto-workload-v2/tpch-sf10-parquet-iceberg-zstd/nation'
);

CREATE TABLE IF NOT EXISTS orders (
    o_orderkey BIGINT,
    o_custkey BIGINT,
    o_orderstatus VARCHAR(1),
    o_totalprice DOUBLE,
    o_orderdate DATE,
    o_orderpriority VARCHAR(15),
    o_clerk VARCHAR(15),
    o_shippriority INTEGER,
    o_comment VARCHAR(79)
)
WITH (
    format = 'PARQUET',
    location = 's3a://presto-workload-v2/tpch-sf10-parquet-iceberg-zstd/orders'
);

CREATE TABLE IF NOT EXISTS part (
    p_partkey BIGINT,
    p_name VARCHAR(55),
    p_mfgr VARCHAR(25),
    p_brand VARCHAR(10),
    p_type VARCHAR(25),
    p_size INTEGER,
    p_container VARCHAR(10),
    p_retailprice DOUBLE,
    p_comment VARCHAR(23)
)
WITH (
    format = 'PARQUET',
    location = 's3a://presto-workload-v2/tpch-sf10-parquet-iceberg-zstd/part'
);

CREATE TABLE IF NOT EXISTS partsupp (
    ps_partkey BIGINT,
    ps_suppkey BIGINT,
    ps_availqty INTEGER,
    ps_supplycost DOUBLE,
    ps_comment VARCHAR(199)
)
WITH (
    format = 'PARQUET',
    location = 's3a://presto-workload-v2/tpch-sf10-parquet-iceberg-zstd/partsupp'
);

CREATE TABLE IF NOT EXISTS region (
    r_regionkey BIGINT,
    r_name VARCHAR(25),
    r_comment VARCHAR(152)
)
WITH (
    format = 'PARQUET',
    location = 's3a://presto-workload-v2/tpch-sf10-parquet-iceberg-zstd/region'
);

CREATE TABLE IF NOT EXISTS supplier (
    s_suppkey BIGINT,
    s_name VARCHAR(25),
    s_address VARCHAR(40),
    s_nationkey BIGINT,
    s_phone VARCHAR(15),
    s_acctbal DOUBLE,
    s_comment VARCHAR(101)
)
WITH (
    format = 'PARQUET',
    location = 's3a://presto-workload-v2/tpch-sf10-parquet-iceberg-zstd/supplier'
);

