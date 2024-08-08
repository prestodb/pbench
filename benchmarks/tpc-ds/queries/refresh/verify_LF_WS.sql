-- ==============================================
--    Fetch two rows randomly from wsv:
-- ==============================================
with ws_random as
         (select
              ws_sold_date_sk,
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
              ws_net_profit
          from wsv
          where ws_sold_time_sk >= (select floor( max(ws_sold_time_sk) * rand()) from wsv )
          order by ws_sold_time_sk limit 2)


-- ========================================================
--    Verify the row can be selected from web_sales:
-- ========================================================

select
    ws.ws_sold_date_sk,
    ws.ws_sold_time_sk,
    ws.ws_ship_date_sk,
    ws.ws_item_sk,
    ws.ws_bill_customer_sk,
    ws.ws_bill_cdemo_sk,
    ws.ws_bill_hdemo_sk,
    ws.ws_bill_addr_sk,
    ws.ws_ship_customer_sk,
    ws.ws_ship_cdemo_sk,
    ws.ws_ship_hdemo_sk,
    ws.ws_ship_addr_sk,
    ws.ws_web_page_sk,
    ws.ws_web_site_sk,
    ws.ws_ship_mode_sk,
    ws.ws_warehouse_sk,
    ws.ws_promo_sk,
    ws.ws_order_number,
    ws.ws_quantity,
    ws.ws_wholesale_cost,
    ws.ws_list_price,
    ws.ws_sales_price,
    ws.ws_ext_discount_amt,
    ws.ws_ext_sales_price,
    ws.ws_ext_wholesale_cost,
    ws.ws_ext_list_price,
    ws.ws_ext_tax,
    ws.ws_coupon_amt,
    ws.ws_ext_ship_cost,
    ws.ws_net_paid,
    ws.ws_net_paid_inc_tax,
    ws.ws_net_paid_inc_ship,
    ws.ws_net_paid_inc_ship_tax,
    ws.ws_net_profit
from web_sales ws, ws_random
where
        ws.ws_sold_date_sk = ws_random.ws_sold_date_sk and
        ws.ws_sold_time_sk = ws_random.ws_sold_time_sk and
        ws.ws_ship_date_sk = ws_random.ws_ship_date_sk and
        ws.ws_item_sk = ws_random.ws_item_sk and
        ws.ws_bill_customer_sk = ws_random.ws_bill_customer_sk and
        ws.ws_bill_cdemo_sk = ws_random.ws_bill_cdemo_sk and
        ws.ws_bill_hdemo_sk = ws_random.ws_bill_hdemo_sk and
        ws.ws_bill_addr_sk = ws_random.ws_bill_addr_sk and
        ws.ws_ship_customer_sk = ws_random.ws_ship_customer_sk and
        ws.ws_ship_cdemo_sk = ws_random.ws_ship_cdemo_sk and
        ws.ws_ship_hdemo_sk = ws_random.ws_ship_hdemo_sk and
        ws.ws_ship_addr_sk = ws_random.ws_ship_addr_sk and
        ws.ws_web_page_sk = ws_random.ws_web_page_sk and
        ws.ws_web_site_sk = ws_random.ws_web_site_sk and
        ws.ws_ship_mode_sk = ws_random.ws_ship_mode_sk and
        ws.ws_warehouse_sk = ws_random.ws_warehouse_sk and
        ws.ws_promo_sk = ws_random.ws_promo_sk;
