CALL dml_hms.system.remove_orphan_files(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.call_center', dry_run => false);
CALL dml_hms.system.remove_orphan_files(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.catalog_page', dry_run => false);
CALL dml_hms.system.remove_orphan_files(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.catalog_returns', dry_run => false);
CALL dml_hms.system.remove_orphan_files(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.catalog_sales', dry_run => false);
CALL dml_hms.system.remove_orphan_files(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.customer', dry_run => false);
CALL dml_hms.system.remove_orphan_files(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.customer_address', dry_run => false);
CALL dml_hms.system.remove_orphan_files(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.customer_demographics', dry_run => false);
CALL dml_hms.system.remove_orphan_files(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.date_dim', dry_run => false);
CALL dml_hms.system.remove_orphan_files(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.household_demographics', dry_run => false);
CALL dml_hms.system.remove_orphan_files(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.income_band', dry_run => false);
CALL dml_hms.system.remove_orphan_files(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.inventory', dry_run => false);
CALL dml_hms.system.remove_orphan_files(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.item', dry_run => false);
CALL dml_hms.system.remove_orphan_files(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.promotion', dry_run => false);
CALL dml_hms.system.remove_orphan_files(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.reason', dry_run => false);
CALL dml_hms.system.remove_orphan_files(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.ship_mode', dry_run => false);
CALL dml_hms.system.remove_orphan_files(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.store', dry_run => false);
CALL dml_hms.system.remove_orphan_files(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.store_returns', dry_run => false);
CALL dml_hms.system.remove_orphan_files(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.store_sales', dry_run => false);
CALL dml_hms.system.remove_orphan_files(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.time_dim', dry_run => false);
CALL dml_hms.system.remove_orphan_files(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.warehouse', dry_run => false);
CALL dml_hms.system.remove_orphan_files(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.web_page', dry_run => false);
CALL dml_hms.system.remove_orphan_files(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.web_returns', dry_run => false);
CALL dml_hms.system.remove_orphan_files(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.web_sales', dry_run => false);
CALL dml_hms.system.remove_orphan_files(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.web_site', dry_run => false);

CALL dml_hms.system.expire_snapshots(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.call_center', older_than => TIMESTAMP '$out_curr_tsp', retain_last => 1);
CALL dml_hms.system.expire_snapshots(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.catalog_page', older_than => TIMESTAMP '$out_curr_tsp', retain_last => 1);
CALL dml_hms.system.expire_snapshots(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.catalog_returns', older_than => TIMESTAMP '$out_curr_tsp', retain_last => 1);
CALL dml_hms.system.expire_snapshots(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.catalog_sales', older_than => TIMESTAMP '$out_curr_tsp', retain_last => 1);
CALL dml_hms.system.expire_snapshots(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.customer', older_than => TIMESTAMP '$out_curr_tsp', retain_last => 1);
CALL dml_hms.system.expire_snapshots(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.customer_address', older_than => TIMESTAMP '$out_curr_tsp', retain_last => 1);
CALL dml_hms.system.expire_snapshots(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.customer_demographics', older_than => TIMESTAMP '$out_curr_tsp', retain_last => 1);
CALL dml_hms.system.expire_snapshots(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.date_dim', older_than => TIMESTAMP '$out_curr_tsp', retain_last => 1);
CALL dml_hms.system.expire_snapshots(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.household_demographics', older_than => TIMESTAMP '$out_curr_tsp', retain_last => 1);
CALL dml_hms.system.expire_snapshots(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.income_band', older_than => TIMESTAMP '$out_curr_tsp', retain_last => 1);
CALL dml_hms.system.expire_snapshots(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.inventory', older_than => TIMESTAMP '$out_curr_tsp', retain_last => 1);
CALL dml_hms.system.expire_snapshots(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.item', older_than => TIMESTAMP '$out_curr_tsp', retain_last => 1);
CALL dml_hms.system.expire_snapshots(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.promotion', older_than => TIMESTAMP '$out_curr_tsp', retain_last => 1);
CALL dml_hms.system.expire_snapshots(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.reason', older_than => TIMESTAMP '$out_curr_tsp', retain_last => 1);
CALL dml_hms.system.expire_snapshots(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.ship_mode', older_than => TIMESTAMP '$out_curr_tsp', retain_last => 1);
CALL dml_hms.system.expire_snapshots(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.store', older_than => TIMESTAMP '$out_curr_tsp', retain_last => 1);
CALL dml_hms.system.expire_snapshots(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.store_returns', older_than => TIMESTAMP '$out_curr_tsp', retain_last => 1);
CALL dml_hms.system.expire_snapshots(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.store_sales', older_than => TIMESTAMP '$out_curr_tsp', retain_last => 1);
CALL dml_hms.system.expire_snapshots(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.time_dim', older_than => TIMESTAMP '$out_curr_tsp', retain_last => 1);
CALL dml_hms.system.expire_snapshots(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.warehouse', older_than => TIMESTAMP '$out_curr_tsp', retain_last => 1);
CALL dml_hms.system.expire_snapshots(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.web_page', older_than => TIMESTAMP '$out_curr_tsp', retain_last => 1);
CALL dml_hms.system.expire_snapshots(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.web_returns', older_than => TIMESTAMP '$out_curr_tsp', retain_last => 1);
CALL dml_hms.system.expire_snapshots(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.web_sales', older_than => TIMESTAMP '$out_curr_tsp', retain_last => 1);
CALL dml_hms.system.expire_snapshots(table => 'dml_hms.tpcds_sf1000_parquet_varchar_opt0222.web_site', older_than => TIMESTAMP '$out_curr_tsp', retain_last => 1);