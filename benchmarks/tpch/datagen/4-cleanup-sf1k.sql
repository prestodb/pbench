CALL iceberg.system.remove_orphan_files(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.customer', dry_run => false);
CALL iceberg.system.remove_orphan_files(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.lineitem', dry_run => false);
CALL iceberg.system.remove_orphan_files(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.orders', dry_run => false);
CALL iceberg.system.remove_orphan_files(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.nation', dry_run => false);
CALL iceberg.system.remove_orphan_files(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.region', dry_run => false);
CALL iceberg.system.remove_orphan_files(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.part', dry_run => false);
CALL iceberg.system.remove_orphan_files(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.supplier', dry_run => false);
CALL iceberg.system.remove_orphan_files(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.partsupp', dry_run => false);

CALL iceberg.system.expire_snapshots(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.customer', older_than => TIMESTAMP '2024-03-16', retain_last => 1);
CALL iceberg.system.expire_snapshots(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.lineitem', older_than => TIMESTAMP '2024-03-16', retain_last => 1);
CALL iceberg.system.expire_snapshots(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.orders', older_than => TIMESTAMP '2024-03-16', retain_last => 1);
CALL iceberg.system.expire_snapshots(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.nation', older_than => TIMESTAMP '2024-03-16', retain_last => 1);
CALL iceberg.system.expire_snapshots(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.region', older_than => TIMESTAMP '2024-03-16', retain_last => 1);
CALL iceberg.system.expire_snapshots(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.part', older_than => TIMESTAMP '2024-03-16', retain_last => 1);
CALL iceberg.system.expire_snapshots(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.supplier', older_than => TIMESTAMP '2024-03-16', retain_last => 1);
CALL iceberg.system.expire_snapshots(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.partsupp', older_than => TIMESTAMP '2024-03-16', retain_last => 1);

CALL iceberg.system.rewrite_manifests(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.customer');
CALL iceberg.system.rewrite_manifests(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.lineitem');
CALL iceberg.system.rewrite_manifests(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.orders');
CALL iceberg.system.rewrite_manifests(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.nation');
CALL iceberg.system.rewrite_manifests(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.region');
CALL iceberg.system.rewrite_manifests(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.part');
CALL iceberg.system.rewrite_manifests(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.supplier');
CALL iceberg.system.rewrite_manifests(table => 'iceberg.tpch_sf1000_parquet_partitioned_iceberg.partsupp');
