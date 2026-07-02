
USE iceberg.tpcds_target_textfile;


INSERT INTO iceberg.tpcds_target_textfile.call_center
SELECT * FROM hive.tpcds_source_textfile.call_center;


INSERT INTO iceberg.tpcds_target_textfile.catalog_page
SELECT * FROM hive.tpcds_source_textfile.catalog_page;


INSERT INTO iceberg.tpcds_target_textfile.catalog_returns
SELECT * FROM hive.tpcds_source_textfile.catalog_returns;


INSERT INTO iceberg.tpcds_target_textfile.catalog_sales
SELECT * FROM hive.tpcds_source_textfile.catalog_sales;


INSERT INTO iceberg.tpcds_target_textfile.customer
SELECT * FROM hive.tpcds_source_textfile.customer;


INSERT INTO iceberg.tpcds_target_textfile.customer_address
SELECT * FROM hive.tpcds_source_textfile.customer_address;


INSERT INTO iceberg.tpcds_target_textfile.customer_demographics
SELECT * FROM hive.tpcds_source_textfile.customer_demographics;


INSERT INTO iceberg.tpcds_target_textfile.date_dim
SELECT * FROM hive.tpcds_source_textfile.date_dim;


INSERT INTO iceberg.tpcds_target_textfile.household_demographics
SELECT * FROM hive.tpcds_source_textfile.household_demographics;


INSERT INTO iceberg.tpcds_target_textfile.income_band
SELECT * FROM hive.tpcds_source_textfile.income_band;


INSERT INTO iceberg.tpcds_target_textfile.inventory
SELECT * FROM hive.tpcds_source_textfile.inventory;


INSERT INTO iceberg.tpcds_target_textfile.item
SELECT * FROM hive.tpcds_source_textfile.item;


INSERT INTO iceberg.tpcds_target_textfile.promotion
SELECT * FROM hive.tpcds_source_textfile.promotion;


INSERT INTO iceberg.tpcds_target_textfile.reason
SELECT * FROM hive.tpcds_source_textfile.reason;


INSERT INTO iceberg.tpcds_target_textfile.ship_mode
SELECT * FROM hive.tpcds_source_textfile.ship_mode;


INSERT INTO iceberg.tpcds_target_textfile.store
SELECT * FROM hive.tpcds_source_textfile.store;


INSERT INTO iceberg.tpcds_target_textfile.store_returns
SELECT * FROM hive.tpcds_source_textfile.store_returns;


INSERT INTO iceberg.tpcds_target_textfile.store_sales
SELECT * FROM hive.tpcds_source_textfile.store_sales;


INSERT INTO iceberg.tpcds_target_textfile.time_dim
SELECT * FROM hive.tpcds_source_textfile.time_dim;


INSERT INTO iceberg.tpcds_target_textfile.warehouse
SELECT * FROM hive.tpcds_source_textfile.warehouse;


INSERT INTO iceberg.tpcds_target_textfile.web_page
SELECT * FROM hive.tpcds_source_textfile.web_page;


INSERT INTO iceberg.tpcds_target_textfile.web_returns
SELECT * FROM hive.tpcds_source_textfile.web_returns;


INSERT INTO iceberg.tpcds_target_textfile.web_sales
SELECT * FROM hive.tpcds_source_textfile.web_sales;


INSERT INTO iceberg.tpcds_target_textfile.web_site
SELECT * FROM hive.tpcds_source_textfile.web_site;

ANALYZE call_center;
ANALYZE catalog_page;
ANALYZE catalog_returns;
ANALYZE catalog_sales;
ANALYZE customer;
ANALYZE customer_address;
ANALYZE customer_demographics;
ANALYZE date_dim;
ANALYZE household_demographics;
ANALYZE income_band;
ANALYZE inventory;
ANALYZE item;
ANALYZE promotion;
ANALYZE reason;
ANALYZE ship_mode;
ANALYZE store;
ANALYZE store_returns;
ANALYZE store_sales;
ANALYZE time_dim;
ANALYZE warehouse;
ANALYZE web_page;
ANALYZE web_returns;
ANALYZE web_sales;
ANALYZE web_site;
