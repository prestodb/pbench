CREATE SCHEMA IF NOT EXISTS tpcds-sf1000-parquet-iceberg-part
WITH (
    location = 's3a://presto-workload-v2/tpcds-sf1000-parquet-iceberg-part/'
);

USE iceberg.tpcds-sf1000-parquet-iceberg-part;

CREATE TABLE IF NOT EXISTS date_dim (
    d_date_sk INT,
    d_date_id VARCHAR(16),
    d_date DATE,
    d_month_seq INT,
    d_week_seq INT,
    d_quarter_seq INT,
    d_year INT,
    d_dow INT,
    d_moy INT,
    d_dom INT,
    d_qoy INT,
    d_fy_year INT,
    d_fy_quarter_seq INT,
    d_fy_week_seq INT,
    d_day_name VARCHAR(9),
    d_quarter_name VARCHAR(6),
    d_holiday VARCHAR(1),
    d_weekend VARCHAR(1),
    d_following_holiday VARCHAR(1),
    d_first_dom INT,
    d_last_dom INT,
    d_same_day_ly INT,
    d_same_day_lq INT,
    d_current_day VARCHAR(1),
    d_current_week VARCHAR(1),
    d_current_month VARCHAR(1),
    d_current_quarter VARCHAR(1),
    d_current_year VARCHAR(1)
)
WITH (
    format = 'PARQUET',
    location = 's3a://presto-workload-v2/tpcds-sf1000-parquet-iceberg-part/date_dim'
)

CREATE TABLE IF NOT EXISTS household_demographics (
    hd_demo_sk INT,
    hd_income_band_sk INT,
    hd_buy_potential VARCHAR(15),
    hd_dep_count INT,
    hd_vehicle_count INT
)
WITH (
    format = 'PARQUET',
    location = 's3a://presto-workload-v2/tpcds-sf1000-parquet-iceberg-part/household_demographics'
)

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
    inv_warehouse_sk INT,
    inv_quantity_on_hand INT
)
WITH (
    format = 'PARQUET',
    location = 's3a://presto-workload-v2/tpcds-sf1000-parquet-iceberg-part/inventory'
)

