INSERT INTO call_center
SELECT * FROM tpcds_sf1000_parquet_iceberg.call_center;

INSERT INTO catalog_page
SELECT * FROM tpcds_sf1000_parquet_iceberg.catalog_page;

INSERT INTO catalog_returns
SELECT * FROM tpcds_sf1000_parquet_iceberg.catalog_returns;

INSERT INTO customer
SELECT * FROM tpcds_sf1000_parquet_iceberg.customer;

INSERT INTO customer_address
SELECT * FROM tpcds_sf1000_parquet_iceberg.customer_address;

INSERT INTO customer_demographics
SELECT * FROM tpcds_sf1000_parquet_iceberg.customer_demographics;

INSERT INTO date_dim
SELECT * FROM tpcds_sf1000_parquet_iceberg.date_dim;

INSERT INTO household_demographics
SELECT * FROM tpcds_sf1000_parquet_iceberg.household_demographics;

INSERT INTO item
SELECT * FROM tpcds_sf1000_parquet_iceberg.item;

INSERT INTO ship_mode
SELECT * FROM tpcds_sf1000_parquet_iceberg.ship_mode;

INSERT INTO store
SELECT * FROM tpcds_sf1000_parquet_iceberg.store;

INSERT INTO store_returns
SELECT * FROM tpcds_sf1000_parquet_iceberg.store_returns;

INSERT INTO web_page
SELECT * FROM tpcds_sf1000_parquet_iceberg.web_page;

INSERT INTO web_returns
SELECT * FROM tpcds_sf1000_parquet_iceberg.web_returns;

INSERT INTO web_site
SELECT * FROM tpcds_sf1000_parquet_iceberg.web_site;

INSERT INTO catalog_sales
SELECT
  cs_sold_time_sk,
  cs_ship_date_sk,
  cs_bill_customer_sk,
  cs_bill_cdemo_sk,
  cs_bill_hdemo_sk,
  cs_bill_addr_sk,
  cs_ship_customer_sk,
  cs_ship_cdemo_sk,
  cs_ship_hdemo_sk,
  cs_ship_addr_sk,
  cs_call_center_sk,
  cs_catalog_page_sk,
  cs_ship_mode_sk,
  cs_warehouse_sk,
  cs_item_sk,
  cs_promo_sk,
  cs_order_number,
  cs_quantity,
  cs_wholesale_cost,
  cs_list_price,
  cs_sales_price,
  cs_ext_discount_amt,
  cs_ext_sales_price,
  cs_ext_wholesale_cost,
  cs_ext_list_price,
  cs_ext_tax,
  cs_coupon_amt,
  cs_ext_ship_cost,
  cs_net_paid,
  cs_net_paid_inc_tax,
  cs_net_paid_inc_ship,
  cs_net_paid_inc_ship_tax,
  cs_net_profit,
  cs_sold_date_sk
FROM tpcds_sf1000_parquet_iceberg.catalog_sales;

INSERT INTO store_sales
SELECT
  ss_sold_time_sk,
  ss_item_sk,
  ss_customer_sk,
  ss_cdemo_sk,
  ss_hdemo_sk,
  ss_addr_sk,
  ss_store_sk,
  ss_promo_sk,
  ss_ticket_number,
  ss_quantity,
  ss_wholesale_cost,
  ss_list_price,
  ss_sales_price,
  ss_ext_discount_amt,
  ss_ext_sales_price,
  ss_ext_wholesale_cost,
  ss_ext_list_price,
  ss_ext_tax,
  ss_coupon_amt,
  ss_net_paid,
  ss_net_paid_inc_tax,
  ss_net_profit,
  ss_sold_date_sk
FROM tpcds_sf1000_parquet_iceberg.store_sales;

INSERT INTO web_sales
SELECT
  ws_sold_time_sk,
  ws_ship_date_sk,
  ws_item_sk,
  ws_bill_customer_sk,
  ws_bill_cdemo_sk,
  ws_bill_hdemo_sk,
  ws_bill_addr_sk,
  ws_ship_customer_sk,
  ws_ship_cdemo_sk,
  ws_ship_hdemo_sk,
  ws_ship_addr_sk,
  ws_web_page_sk,
  ws_web_site_sk,
  ws_ship_mode_sk,
  ws_warehouse_sk,
  ws_promo_sk,
  ws_order_number,
  ws_quantity,
  ws_wholesale_cost,
  ws_list_price,
  ws_sales_price,
  ws_ext_discount_amt,
  ws_ext_sales_price,
  ws_ext_wholesale_cost,
  ws_ext_list_price,
  ws_ext_tax,
  ws_coupon_amt,
  ws_ext_ship_cost,
  ws_net_paid,
  ws_net_paid_inc_tax,
  ws_net_paid_inc_ship,
  ws_net_paid_inc_ship_tax,
  ws_net_profit,
  ws_sold_date_sk
FROM tpcds_sf1000_parquet_iceberg.web_sales;

INSERT INTO income_band
SELECT * FROM tpcds_sf1000_parquet_iceberg.income_band;

INSERT INTO promotion
SELECT * FROM tpcds_sf1000_parquet_iceberg.promotion;

INSERT INTO reason
SELECT * FROM tpcds_sf1000_parquet_iceberg.reason;

INSERT INTO time_dim
SELECT * FROM tpcds_sf1000_parquet_iceberg.time_dim;

INSERT INTO warehouse
SELECT * FROM tpcds_sf1000_parquet_iceberg.warehouse;

INSERT INTO inventory
SELECT
  inv_item_sk,
  inv_warehouse_sk,
  inv_quantity_on_hand,
  inv_date_sk
FROM tpcds_sf1000_parquet_iceberg.inventory;
