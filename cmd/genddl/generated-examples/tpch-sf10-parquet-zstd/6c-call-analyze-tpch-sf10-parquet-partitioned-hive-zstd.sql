USE hive.tpch_sf10_parquet_partitioned_hive_zstd;

CALL system.sync_partition_metadata('tpch_sf10_parquet_partitioned_hive_zstd', 'lineitem', 'FULL');
CALL system.sync_partition_metadata('tpch_sf10_parquet_partitioned_hive_zstd', 'orders', 'FULL');

ANALYZE customer;
ANALYZE lineitem;
ANALYZE nation;
ANALYZE orders;
ANALYZE part;
ANALYZE partsupp;
ANALYZE region;
ANALYZE supplier;
