SET SESSION iceberg.compression_codec='ZSTD';
SET SESSION query_max_execution_time='12h';
SET SESSION query_max_run_time='12h';

USE iceberg.tpcds_sf1000_parquet_iceberg_zstd;

INSERT INTO call_center
SELECT * FROM iceberg.tpcds_sf1000_parquet_iceberg.call_center;

INSERT INTO catalog_page
SELECT * FROM iceberg.tpcds_sf1000_parquet_iceberg.catalog_page;

INSERT INTO catalog_returns
SELECT * FROM iceberg.tpcds_sf1000_parquet_iceberg.catalog_returns;

INSERT INTO catalog_sales
SELECT * FROM iceberg.tpcds_sf1000_parquet_iceberg.catalog_sales;

INSERT INTO customer
SELECT * FROM iceberg.tpcds_sf1000_parquet_iceberg.customer;

INSERT INTO customer_address
SELECT * FROM iceberg.tpcds_sf1000_parquet_iceberg.customer_address;

INSERT INTO customer_demographics
SELECT * FROM iceberg.tpcds_sf1000_parquet_iceberg.customer_demographics;

INSERT INTO date_dim
SELECT * FROM iceberg.tpcds_sf1000_parquet_iceberg.date_dim;

INSERT INTO household_demographics
SELECT * FROM iceberg.tpcds_sf1000_parquet_iceberg.household_demographics;

INSERT INTO income_band
SELECT * FROM iceberg.tpcds_sf1000_parquet_iceberg.income_band;

INSERT INTO inventory
SELECT * FROM iceberg.tpcds_sf1000_parquet_iceberg.inventory;

INSERT INTO item
SELECT * FROM iceberg.tpcds_sf1000_parquet_iceberg.item;

INSERT INTO promotion
SELECT * FROM iceberg.tpcds_sf1000_parquet_iceberg.promotion;

INSERT INTO reason
SELECT * FROM iceberg.tpcds_sf1000_parquet_iceberg.reason;

INSERT INTO ship_mode
SELECT * FROM iceberg.tpcds_sf1000_parquet_iceberg.ship_mode;

INSERT INTO store
SELECT * FROM iceberg.tpcds_sf1000_parquet_iceberg.store;

INSERT INTO store_returns
SELECT * FROM iceberg.tpcds_sf1000_parquet_iceberg.store_returns;

INSERT INTO store_sales
SELECT * FROM iceberg.tpcds_sf1000_parquet_iceberg.store_sales;

INSERT INTO time_dim
SELECT * FROM iceberg.tpcds_sf1000_parquet_iceberg.time_dim;

INSERT INTO warehouse
SELECT * FROM iceberg.tpcds_sf1000_parquet_iceberg.warehouse;

INSERT INTO web_page
SELECT * FROM iceberg.tpcds_sf1000_parquet_iceberg.web_page;

INSERT INTO web_returns
SELECT * FROM iceberg.tpcds_sf1000_parquet_iceberg.web_returns;

INSERT INTO web_sales
SELECT * FROM iceberg.tpcds_sf1000_parquet_iceberg.web_sales;

INSERT INTO web_site
SELECT * FROM iceberg.tpcds_sf1000_parquet_iceberg.web_site;

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
