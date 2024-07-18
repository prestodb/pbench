-- ==============================================
--    Fetch two rows randomly from csv:
-- ==============================================
with csc_random as
         (select
              cs_sold_date_sk,
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
              cs_net_profit
          from csv
          where cs_sold_time_sk >= (select floor( max(cs_sold_time_sk) * rand()) from csv )
          order by cs_sold_time_sk limit 2)

-- ========================================================
--    Verify the row can be selected from catalog_sales:
-- ========================================================

select
    catalog_sales.cs_sold_date_sk,
    catalog_sales.cs_sold_time_sk,
    catalog_sales.cs_ship_date_sk,
    catalog_sales.cs_bill_customer_sk,
    catalog_sales.cs_bill_cdemo_sk,
    catalog_sales.cs_bill_hdemo_sk,
    catalog_sales.cs_bill_addr_sk,
    catalog_sales.cs_ship_customer_sk,
    catalog_sales.cs_ship_cdemo_sk,
    catalog_sales.cs_ship_hdemo_sk,
    catalog_sales.cs_ship_addr_sk,
    catalog_sales.cs_call_center_sk,
    catalog_sales.cs_catalog_page_sk,
    catalog_sales.cs_ship_mode_sk,
    catalog_sales.cs_warehouse_sk,
    catalog_sales.cs_item_sk,
    catalog_sales.cs_promo_sk,
    catalog_sales.cs_order_number,
    catalog_sales.cs_quantity,
    catalog_sales.cs_wholesale_cost,
    catalog_sales.cs_list_price,
    catalog_sales.cs_sales_price,
    catalog_sales.cs_ext_discount_amt,
    catalog_sales.cs_ext_sales_price,
    catalog_sales.cs_ext_wholesale_cost,
    catalog_sales.cs_ext_list_price,
    catalog_sales.cs_ext_tax,
    catalog_sales.cs_coupon_amt,
    catalog_sales.cs_ext_ship_cost,
    catalog_sales.cs_net_paid,
    catalog_sales.cs_net_paid_inc_tax,
    catalog_sales.cs_net_paid_inc_ship,
    catalog_sales.cs_net_paid_inc_ship_tax,
    catalog_sales.cs_net_profit
from catalog_sales, csc_random
where
        catalog_sales.cs_sold_date_sk = csc_random.cs_sold_date_sk and
        catalog_sales.cs_sold_time_sk = csc_random.cs_sold_time_sk and
        catalog_sales.cs_ship_date_sk = csc_random.cs_ship_date_sk and
        catalog_sales.cs_bill_customer_sk = csc_random.cs_bill_customer_sk and
        catalog_sales.cs_bill_cdemo_sk = csc_random.cs_bill_cdemo_sk and
        catalog_sales.cs_bill_hdemo_sk = csc_random.cs_bill_hdemo_sk and
        catalog_sales.cs_bill_addr_sk = csc_random.cs_bill_addr_sk and
        catalog_sales.cs_ship_customer_sk = csc_random.cs_ship_customer_sk and
        catalog_sales.cs_ship_cdemo_sk = csc_random.cs_ship_cdemo_sk and
        catalog_sales.cs_ship_hdemo_sk = csc_random.cs_ship_hdemo_sk and
        catalog_sales.cs_ship_addr_sk = csc_random.cs_ship_addr_sk and
        catalog_sales.cs_call_center_sk = csc_random.cs_call_center_sk and
        catalog_sales.cs_catalog_page_sk = csc_random.cs_catalog_page_sk and
        catalog_sales.cs_ship_mode_sk = csc_random.cs_ship_mode_sk and
        catalog_sales.cs_warehouse_sk = csc_random.cs_warehouse_sk and
        catalog_sales.cs_item_sk = csc_random.cs_item_sk and
        catalog_sales.cs_promo_sk = csc_random.cs_promo_sk;