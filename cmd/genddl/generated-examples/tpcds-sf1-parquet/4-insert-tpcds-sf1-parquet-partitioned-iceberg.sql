SET SESSION iceberg.compression_codec='NONE';
SET SESSION query_max_execution_time='12h';
SET SESSION query_max_run_time='12h';

USE iceberg.tpcds_sf1_parquet_partitioned_iceberg;

INSERT INTO catalog_sales
SELECT
    cast(cs_sold_time_sk as INT),
    cast(cs_ship_date_sk as INT),
    cast(cs_bill_customer_sk as INT),
    cast(cs_bill_cdemo_sk as INT),
    cast(cs_bill_hdemo_sk as INT),
    cast(cs_bill_addr_sk as INT),
    cast(cs_ship_customer_sk as INT),
    cast(cs_ship_cdemo_sk as INT),
    cast(cs_ship_hdemo_sk as INT),
    cast(cs_ship_addr_sk as INT),
    cast(cs_call_center_sk as INT),
    cast(cs_catalog_page_sk as INT),
    cast(cs_ship_mode_sk as INT),
    cast(cs_warehouse_sk as INT),
    cast(cs_item_sk as INT),
    cast(cs_promo_sk as INT),
    cast(cs_order_number as BIGINT),
    cast(cs_quantity as INT),
    cast(cs_wholesale_cost as DECIMAL(7,2)),
    cast(cs_list_price as DECIMAL(7,2)),
    cast(cs_sales_price as DECIMAL(7,2)),
    cast(cs_ext_discount_amt as DECIMAL(7,2)),
    cast(cs_ext_sales_price as DECIMAL(7,2)),
    cast(cs_ext_wholesale_cost as DECIMAL(7,2)),
    cast(cs_ext_list_price as DECIMAL(7,2)),
    cast(cs_ext_tax as DECIMAL(7,2)),
    cast(cs_coupon_amt as DECIMAL(7,2)),
    cast(cs_ext_ship_cost as DECIMAL(7,2)),
    cast(cs_net_paid as DECIMAL(7,2)),
    cast(cs_net_paid_inc_tax as DECIMAL(7,2)),
    cast(cs_net_paid_inc_ship as DECIMAL(7,2)),
    cast(cs_net_paid_inc_ship_tax as DECIMAL(7,2)),
    cast(cs_net_profit as DECIMAL(7,2)),
    cast(cs_sold_date_sk as INT)
FROM tpcds.sf1.catalog_sales;

INSERT INTO inventory
SELECT
    cast(inv_item_sk as INT),
    cast(inv_warehouse_sk as INT),
    cast(inv_quantity_on_hand as INT),
    cast(inv_date_sk as INT)
FROM tpcds.sf1.inventory;

INSERT INTO store_sales
SELECT
    cast(ss_sold_time_sk as INT),
    cast(ss_item_sk as INT),
    cast(ss_customer_sk as INT),
    cast(ss_cdemo_sk as INT),
    cast(ss_hdemo_sk as INT),
    cast(ss_addr_sk as INT),
    cast(ss_store_sk as INT),
    cast(ss_promo_sk as INT),
    cast(ss_ticket_number as BIGINT),
    cast(ss_quantity as INT),
    cast(ss_wholesale_cost as DECIMAL(7,2)),
    cast(ss_list_price as DECIMAL(7,2)),
    cast(ss_sales_price as DECIMAL(7,2)),
    cast(ss_ext_discount_amt as DECIMAL(7,2)),
    cast(ss_ext_sales_price as DECIMAL(7,2)),
    cast(ss_ext_wholesale_cost as DECIMAL(7,2)),
    cast(ss_ext_list_price as DECIMAL(7,2)),
    cast(ss_ext_tax as DECIMAL(7,2)),
    cast(ss_coupon_amt as DECIMAL(7,2)),
    cast(ss_net_paid as DECIMAL(7,2)),
    cast(ss_net_paid_inc_tax as DECIMAL(7,2)),
    cast(ss_net_profit as DECIMAL(7,2)),
    cast(ss_sold_date_sk as INT)
FROM tpcds.sf1.store_sales;

INSERT INTO web_sales
SELECT
    cast(ws_sold_time_sk as INT),
    cast(ws_ship_date_sk as INT),
    cast(ws_item_sk as INT),
    cast(ws_bill_customer_sk as INT),
    cast(ws_bill_cdemo_sk as INT),
    cast(ws_bill_hdemo_sk as INT),
    cast(ws_bill_addr_sk as INT),
    cast(ws_ship_customer_sk as INT),
    cast(ws_ship_cdemo_sk as INT),
    cast(ws_ship_hdemo_sk as INT),
    cast(ws_ship_addr_sk as INT),
    cast(ws_web_page_sk as INT),
    cast(ws_web_site_sk as INT),
    cast(ws_ship_mode_sk as INT),
    cast(ws_warehouse_sk as INT),
    cast(ws_promo_sk as INT),
    cast(ws_order_number as BIGINT),
    cast(ws_quantity as INT),
    cast(ws_wholesale_cost as DECIMAL(7,2)),
    cast(ws_list_price as DECIMAL(7,2)),
    cast(ws_sales_price as DECIMAL(7,2)),
    cast(ws_ext_discount_amt as DECIMAL(7,2)),
    cast(ws_ext_sales_price as DECIMAL(7,2)),
    cast(ws_ext_wholesale_cost as DECIMAL(7,2)),
    cast(ws_ext_list_price as DECIMAL(7,2)),
    cast(ws_ext_tax as DECIMAL(7,2)),
    cast(ws_coupon_amt as DECIMAL(7,2)),
    cast(ws_ext_ship_cost as DECIMAL(7,2)),
    cast(ws_net_paid as DECIMAL(7,2)),
    cast(ws_net_paid_inc_tax as DECIMAL(7,2)),
    cast(ws_net_paid_inc_ship as DECIMAL(7,2)),
    cast(ws_net_paid_inc_ship_tax as DECIMAL(7,2)),
    cast(ws_net_profit as DECIMAL(7,2)),
    cast(ws_sold_date_sk as INT)
FROM tpcds.sf1.web_sales;

ANALYZE catalog_sales;
ANALYZE inventory;
ANALYZE store_sales;
ANALYZE web_sales;
