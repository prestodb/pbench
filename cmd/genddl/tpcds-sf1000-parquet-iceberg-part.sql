CREATE SCHEMA IF NOT EXISTS tpcds-sf1000-parquet-iceberg-part
WITH (
    location = 's3a://presto-workload-v2/tpcds-sf1000-parquet-iceberg-part/'
);
USE iceberg.tpcds-sf1000-parquet-iceberg-part;
CREATE TABLE IF NOT EXISTS inventory (
inv_date_sk INT,
inv_item_sk INT,
)
