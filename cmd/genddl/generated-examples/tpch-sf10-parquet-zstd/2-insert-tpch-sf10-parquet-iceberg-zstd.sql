SET SESSION iceberg.compression_codec='ZSTD';
SET SESSION query_max_execution_time='12h';
SET SESSION query_max_run_time='12h';

USE iceberg.tpch_sf10_parquet_iceberg_zstd;

INSERT INTO customer
SELECT * FROM iceberg.tpch_sf10_parquet_iceberg.customer;

INSERT INTO lineitem
SELECT * FROM iceberg.tpch_sf10_parquet_iceberg.lineitem;

INSERT INTO nation
SELECT * FROM iceberg.tpch_sf10_parquet_iceberg.nation;

INSERT INTO orders
SELECT * FROM iceberg.tpch_sf10_parquet_iceberg.orders;

INSERT INTO part
SELECT * FROM iceberg.tpch_sf10_parquet_iceberg.part;

INSERT INTO partsupp
SELECT * FROM iceberg.tpch_sf10_parquet_iceberg.partsupp;

INSERT INTO region
SELECT * FROM iceberg.tpch_sf10_parquet_iceberg.region;

INSERT INTO supplier
SELECT * FROM iceberg.tpch_sf10_parquet_iceberg.supplier;

ANALYZE customer;
ANALYZE lineitem;
ANALYZE nation;
ANALYZE orders;
ANALYZE part;
ANALYZE partsupp;
ANALYZE region;
ANALYZE supplier;
