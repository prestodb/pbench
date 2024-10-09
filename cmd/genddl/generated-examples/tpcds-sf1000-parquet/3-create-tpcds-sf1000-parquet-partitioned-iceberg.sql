SET SESSION iceberg.compression_codec='NONE';
SET SESSION query_max_execution_time='12h';
SET SESSION query_max_run_time='12h';

CREATE SCHEMA IF NOT EXISTS iceberg.tpcds_sf1000_parquet_partitioned_iceberg
WITH (
    location = 's3a://presto-workload-v2/tpcds-sf1000-parquet-partitioned-iceberg/'
);

USE iceberg.tpcds_sf1000_parquet_partitioned_iceberg;

CALL iceberg.system.register_table('tpcds_sf1000_parquet_partitioned_iceberg', 'call_center', 's3a://presto-workload-v2/tpcds-sf1000-parquet-iceberg/call_center/metadata');
CALL iceberg.system.register_table('tpcds_sf1000_parquet_partitioned_iceberg', 'catalog_page', 's3a://presto-workload-v2/tpcds-sf1000-parquet-iceberg/catalog_page/metadata');
CALL iceberg.system.register_table('tpcds_sf1000_parquet_partitioned_iceberg', 'catalog_returns', 's3a://presto-workload-v2/tpcds-sf1000-parquet-iceberg/catalog_returns/metadata');
CALL iceberg.system.register_table('tpcds_sf1000_parquet_partitioned_iceberg', 'customer', 's3a://presto-workload-v2/tpcds-sf1000-parquet-iceberg/customer/metadata');
CALL iceberg.system.register_table('tpcds_sf1000_parquet_partitioned_iceberg', 'customer_address', 's3a://presto-workload-v2/tpcds-sf1000-parquet-iceberg/customer_address/metadata');
CALL iceberg.system.register_table('tpcds_sf1000_parquet_partitioned_iceberg', 'customer_demographics', 's3a://presto-workload-v2/tpcds-sf1000-parquet-iceberg/customer_demographics/metadata');
CALL iceberg.system.register_table('tpcds_sf1000_parquet_partitioned_iceberg', 'date_dim', 's3a://presto-workload-v2/tpcds-sf1000-parquet-iceberg/date_dim/metadata');
CALL iceberg.system.register_table('tpcds_sf1000_parquet_partitioned_iceberg', 'household_demographics', 's3a://presto-workload-v2/tpcds-sf1000-parquet-iceberg/household_demographics/metadata');
CALL iceberg.system.register_table('tpcds_sf1000_parquet_partitioned_iceberg', 'income_band', 's3a://presto-workload-v2/tpcds-sf1000-parquet-iceberg/income_band/metadata');
CALL iceberg.system.register_table('tpcds_sf1000_parquet_partitioned_iceberg', 'item', 's3a://presto-workload-v2/tpcds-sf1000-parquet-iceberg/item/metadata');
CALL iceberg.system.register_table('tpcds_sf1000_parquet_partitioned_iceberg', 'promotion', 's3a://presto-workload-v2/tpcds-sf1000-parquet-iceberg/promotion/metadata');
CALL iceberg.system.register_table('tpcds_sf1000_parquet_partitioned_iceberg', 'reason', 's3a://presto-workload-v2/tpcds-sf1000-parquet-iceberg/reason/metadata');
CALL iceberg.system.register_table('tpcds_sf1000_parquet_partitioned_iceberg', 'ship_mode', 's3a://presto-workload-v2/tpcds-sf1000-parquet-iceberg/ship_mode/metadata');
CALL iceberg.system.register_table('tpcds_sf1000_parquet_partitioned_iceberg', 'store', 's3a://presto-workload-v2/tpcds-sf1000-parquet-iceberg/store/metadata');
CALL iceberg.system.register_table('tpcds_sf1000_parquet_partitioned_iceberg', 'store_returns', 's3a://presto-workload-v2/tpcds-sf1000-parquet-iceberg/store_returns/metadata');
CALL iceberg.system.register_table('tpcds_sf1000_parquet_partitioned_iceberg', 'time_dim', 's3a://presto-workload-v2/tpcds-sf1000-parquet-iceberg/time_dim/metadata');
CALL iceberg.system.register_table('tpcds_sf1000_parquet_partitioned_iceberg', 'warehouse', 's3a://presto-workload-v2/tpcds-sf1000-parquet-iceberg/warehouse/metadata');
CALL iceberg.system.register_table('tpcds_sf1000_parquet_partitioned_iceberg', 'web_page', 's3a://presto-workload-v2/tpcds-sf1000-parquet-iceberg/web_page/metadata');
CALL iceberg.system.register_table('tpcds_sf1000_parquet_partitioned_iceberg', 'web_returns', 's3a://presto-workload-v2/tpcds-sf1000-parquet-iceberg/web_returns/metadata');
CALL iceberg.system.register_table('tpcds_sf1000_parquet_partitioned_iceberg', 'web_site', 's3a://presto-workload-v2/tpcds-sf1000-parquet-iceberg/web_site/metadata');

CREATE TABLE IF NOT EXISTS catalog_sales (
    cs_sold_time_sk INT,
    cs_ship_date_sk INT,
    cs_bill_customer_sk INT,
    cs_bill_cdemo_sk INT,
    cs_bill_hdemo_sk INT,
    cs_bill_addr_sk INT,
    cs_ship_customer_sk INT,
    cs_ship_cdemo_sk INT,
    cs_ship_hdemo_sk INT,
    cs_ship_addr_sk INT,
    cs_call_center_sk INT,
    cs_catalog_page_sk INT,
    cs_ship_mode_sk INT,
    cs_warehouse_sk INT,
    cs_item_sk INT,
    cs_promo_sk INT,
    cs_order_number BIGINT,
    cs_quantity INT,
    cs_wholesale_cost DECIMAL(7,2),
    cs_list_price DECIMAL(7,2),
    cs_sales_price DECIMAL(7,2),
    cs_ext_discount_amt DECIMAL(7,2),
    cs_ext_sales_price DECIMAL(7,2),
    cs_ext_wholesale_cost DECIMAL(7,2),
    cs_ext_list_price DECIMAL(7,2),
    cs_ext_tax DECIMAL(7,2),
    cs_coupon_amt DECIMAL(7,2),
    cs_ext_ship_cost DECIMAL(7,2),
    cs_net_paid DECIMAL(7,2),
    cs_net_paid_inc_tax DECIMAL(7,2),
    cs_net_paid_inc_ship DECIMAL(7,2),
    cs_net_paid_inc_ship_tax DECIMAL(7,2),
    cs_net_profit DECIMAL(7,2),
    cs_sold_date_sk INT
)
WITH (
    format = 'PARQUET',
    partitioning = array['cs_sold_date_sk']
);

CREATE TABLE IF NOT EXISTS inventory (
    inv_item_sk INT,
    inv_warehouse_sk INT,
    inv_quantity_on_hand INT,
    inv_date_sk INT
)
WITH (
    format = 'PARQUET',
    partitioning = array['inv_date_sk']
);

CREATE TABLE IF NOT EXISTS store_sales (
    ss_sold_time_sk INT,
    ss_item_sk INT,
    ss_customer_sk INT,
    ss_cdemo_sk INT,
    ss_hdemo_sk INT,
    ss_addr_sk INT,
    ss_store_sk INT,
    ss_promo_sk INT,
    ss_ticket_number BIGINT,
    ss_quantity INT,
    ss_wholesale_cost DECIMAL(7,2),
    ss_list_price DECIMAL(7,2),
    ss_sales_price DECIMAL(7,2),
    ss_ext_discount_amt DECIMAL(7,2),
    ss_ext_sales_price DECIMAL(7,2),
    ss_ext_wholesale_cost DECIMAL(7,2),
    ss_ext_list_price DECIMAL(7,2),
    ss_ext_tax DECIMAL(7,2),
    ss_coupon_amt DECIMAL(7,2),
    ss_net_paid DECIMAL(7,2),
    ss_net_paid_inc_tax DECIMAL(7,2),
    ss_net_profit DECIMAL(7,2),
    ss_sold_date_sk INT
)
WITH (
    format = 'PARQUET',
    partitioning = array['ss_sold_date_sk']
);

CREATE TABLE IF NOT EXISTS web_sales (
    ws_sold_time_sk INT,
    ws_ship_date_sk INT,
    ws_item_sk INT,
    ws_bill_customer_sk INT,
    ws_bill_cdemo_sk INT,
    ws_bill_hdemo_sk INT,
    ws_bill_addr_sk INT,
    ws_ship_customer_sk INT,
    ws_ship_cdemo_sk INT,
    ws_ship_hdemo_sk INT,
    ws_ship_addr_sk INT,
    ws_web_page_sk INT,
    ws_web_site_sk INT,
    ws_ship_mode_sk INT,
    ws_warehouse_sk INT,
    ws_promo_sk INT,
    ws_order_number BIGINT,
    ws_quantity INT,
    ws_wholesale_cost DECIMAL(7,2),
    ws_list_price DECIMAL(7,2),
    ws_sales_price DECIMAL(7,2),
    ws_ext_discount_amt DECIMAL(7,2),
    ws_ext_sales_price DECIMAL(7,2),
    ws_ext_wholesale_cost DECIMAL(7,2),
    ws_ext_list_price DECIMAL(7,2),
    ws_ext_tax DECIMAL(7,2),
    ws_coupon_amt DECIMAL(7,2),
    ws_ext_ship_cost DECIMAL(7,2),
    ws_net_paid DECIMAL(7,2),
    ws_net_paid_inc_tax DECIMAL(7,2),
    ws_net_paid_inc_ship DECIMAL(7,2),
    ws_net_paid_inc_ship_tax DECIMAL(7,2),
    ws_net_profit DECIMAL(7,2),
    ws_sold_date_sk INT
)
WITH (
    format = 'PARQUET',
    partitioning = array['ws_sold_date_sk']
);

