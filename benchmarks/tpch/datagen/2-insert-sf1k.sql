INSERT INTO customer
SELECT * FROM tpch_sf1000_parquet_iceberg.call_center;

INSERT INTO lineitem
SELECT * FROM tpch_sf1000_parquet_iceberg.catalog_page;

INSERT INTO orders
SELECT * FROM tpch_sf1000_parquet_iceberg.catalog_returns;

INSERT INTO nation
SELECT * FROM tpch_sf1000_parquet_iceberg.customer;

INSERT INTO region
SELECT * FROM tpch_sf1000_parquet_iceberg.customer_address;

INSERT INTO part
SELECT * FROM tpch_sf1000_parquet_iceberg.customer_demographics;

INSERT INTO supplier
SELECT * FROM tpch_sf1000_parquet_iceberg.date_dim;

INSERT INTO partsupp
SELECT * FROM tpch_sf1000_parquet_iceberg.household_demographics;
