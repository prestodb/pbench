CREATE SCHEMA IF NOT EXISTS tpcds-sf1000-parquet-iceberg-part
WITH (
    location = 's3a://presto-workload-v2/tpcds-sf1000-parquet-iceberg-part/'
);

USE iceberg.tpcds-sf1000-parquet-iceberg-part;

CREATE TABLE IF NOT EXISTS income_band (
    ib_income_band_sk INT,
    ib_lower_bound INT,
    ib_upper_bound INT
)
WITH (
    format = 'PARQUET',
    location = 's3a://presto-workload-v2/tpcds-sf1000-parquet-iceberg-part/income_band'
)

CREATE TABLE IF NOT EXISTS inventory (
    inv_date_sk INT,
    inv_item_sk INT,
    inv_quantity_on_hand INT,
    inv_warehouse_sk INT
)
WITH (
    format = 'PARQUET',
    location = 's3a://presto-workload-v2/tpcds-sf1000-parquet-iceberg-part/inventory'
)

