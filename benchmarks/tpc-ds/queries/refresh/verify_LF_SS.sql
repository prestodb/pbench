-- ==============================================
--    Fetch two rows randomly from ssv:
-- ==============================================
with ss_random as
         (select
              ss_sold_date_sk,
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
              ss_net_profit
          from ssv
          where ss_sold_time_sk >= (select floor( max(ss_sold_time_sk) * rand()) from ssv )
          order by ss_sold_time_sk limit 2)

-- ========================================================
--    Verify the row can be selected from store_sales:
-- ========================================================
select
    ss.ss_sold_date_sk,
    ss.ss_sold_time_sk,
    ss.ss_item_sk,
    ss.ss_customer_sk,
    ss.ss_cdemo_sk,
    ss.ss_hdemo_sk,
    ss.ss_addr_sk,
    ss.ss_store_sk,
    ss.ss_promo_sk,
    ss.ss_ticket_number,
    ss.ss_quantity,
    ss.ss_wholesale_cost,
    ss.ss_list_price,
    ss.ss_sales_price,
    ss.ss_ext_discount_amt,
    ss.ss_ext_sales_price,
    ss.ss_ext_wholesale_cost,
    ss.ss_ext_list_price,
    ss.ss_ext_tax,
    ss.ss_coupon_amt,
    ss.ss_net_paid,
    ss.ss_net_paid_inc_tax,
    ss.ss_net_profit
from store_sales ss, ss_random
where
        ss.ss_sold_date_sk = ss_random.ss_sold_date_sk and
        ss.ss_sold_time_sk = ss_random.ss_sold_time_sk and
        ss.ss_item_sk = ss_random.ss_item_sk and
        ss.ss_customer_sk = ss_random.ss_customer_sk and
        ss.ss_cdemo_sk = ss_random.ss_cdemo_sk and
        ss.ss_hdemo_sk = ss_random.ss_hdemo_sk and
        ss.ss_addr_sk = ss_random.ss_addr_sk and
        ss.ss_store_sk = ss_random.ss_store_sk and
        ss.ss_promo_sk = ss_random.ss_promo_sk;
