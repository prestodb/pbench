CALL iceberg.system.rewrite_data_files(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.customer', strategy => 'binpack', options => map('min-input-files', '2'));
CALL iceberg.system.rewrite_data_files(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.lineitem', strategy => 'binpack', options => map('min-input-files', '2'));
CALL iceberg.system.rewrite_data_files(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.orders', strategy => 'binpack', options => map('min-input-files', '2'));
CALL iceberg.system.rewrite_data_files(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.nation', strategy => 'binpack', options => map('min-input-files', '2'));
CALL iceberg.system.rewrite_data_files(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.region', strategy => 'binpack', options => map('min-input-files', '2'));
CALL iceberg.system.rewrite_data_files(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.part', strategy => 'binpack', options => map('min-input-files', '2'));
CALL iceberg.system.rewrite_data_files(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.partsupp', strategy => 'binpack', options => map('min-input-files', '2'));
CALL iceberg.system.rewrite_data_files(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.supplier', strategy => 'binpack', options => map('min-input-files', '2'));
