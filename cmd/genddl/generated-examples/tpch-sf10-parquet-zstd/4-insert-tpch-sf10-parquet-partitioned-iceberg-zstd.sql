SET SESSION iceberg.compression_codec='ZSTD';
SET SESSION query_max_execution_time='12h';
SET SESSION query_max_run_time='12h';

USE iceberg.tpch_sf10_parquet_partitioned_iceberg_zstd;

INSERT INTO lineitem
SELECT * FROM iceberg.tpch_sf10_parquet_partitioned_iceberg.lineitem;

INSERT INTO orders
SELECT * FROM iceberg.tpch_sf10_parquet_partitioned_iceberg.orders;

ANALYZE lineitem;
ANALYZE orders;
