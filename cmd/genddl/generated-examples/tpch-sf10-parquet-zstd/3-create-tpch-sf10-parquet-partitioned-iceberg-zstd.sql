SET SESSION iceberg.compression_codec='ZSTD';
SET SESSION query_max_execution_time='12h';
SET SESSION query_max_run_time='12h';

CREATE SCHEMA IF NOT EXISTS iceberg.tpch_sf10_parquet_partitioned_iceberg_zstd
WITH (
    location = 's3a://presto-workload-v2/tpch-sf10-parquet-partitioned-iceberg-zstd/'
);

USE iceberg.tpch_sf10_parquet_partitioned_iceberg_zstd;

CALL iceberg.system.register_table('tpch_sf10_parquet_partitioned_iceberg_zstd', 'customer', 's3a://presto-workload-v2/tpch-sf10-parquet-iceberg-zstd/customer/metadata');
CALL iceberg.system.register_table('tpch_sf10_parquet_partitioned_iceberg_zstd', 'nation', 's3a://presto-workload-v2/tpch-sf10-parquet-iceberg-zstd/nation/metadata');
CALL iceberg.system.register_table('tpch_sf10_parquet_partitioned_iceberg_zstd', 'part', 's3a://presto-workload-v2/tpch-sf10-parquet-iceberg-zstd/part/metadata');
CALL iceberg.system.register_table('tpch_sf10_parquet_partitioned_iceberg_zstd', 'partsupp', 's3a://presto-workload-v2/tpch-sf10-parquet-iceberg-zstd/partsupp/metadata');
CALL iceberg.system.register_table('tpch_sf10_parquet_partitioned_iceberg_zstd', 'region', 's3a://presto-workload-v2/tpch-sf10-parquet-iceberg-zstd/region/metadata');
CALL iceberg.system.register_table('tpch_sf10_parquet_partitioned_iceberg_zstd', 'supplier', 's3a://presto-workload-v2/tpch-sf10-parquet-iceberg-zstd/supplier/metadata');

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
    l_commitdate DATE,
    l_receiptdate DATE,
    l_shipinstruct VARCHAR(25),
    l_shipmode VARCHAR(10),
    l_comment VARCHAR(44),
    l_shipdate DATE
)
WITH (
    format = 'PARQUET',
    partitioning = array['l_shipdate']
);

CREATE TABLE IF NOT EXISTS orders (
    o_orderkey BIGINT,
    o_custkey BIGINT,
    o_orderstatus VARCHAR(1),
    o_totalprice DOUBLE,
    o_orderpriority VARCHAR(15),
    o_clerk VARCHAR(15),
    o_shippriority INTEGER,
    o_comment VARCHAR(79),
    o_orderdate DATE
)
WITH (
    format = 'PARQUET',
    partitioning = array['o_orderdate']
);

