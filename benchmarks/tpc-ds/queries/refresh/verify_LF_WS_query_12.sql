-- ==============================================
-- 28 ec 0b 0b ac 1b fb 1b
-- ==============================================
with column_checksums as (
    select array[
               checksum(ws_sold_date_sk),
           checksum(ws_sold_time_sk),
           checksum(ws_ship_date_sk),
           checksum(ws_item_sk),
           checksum(ws_bill_customer_sk),
           checksum(ws_bill_cdemo_sk),
           checksum(ws_bill_hdemo_sk),
           checksum(ws_bill_addr_sk),
           checksum(ws_ship_customer_sk),
           checksum(ws_ship_cdemo_sk),
           checksum(ws_ship_hdemo_sk),
           checksum(ws_ship_addr_sk),
           checksum(ws_web_page_sk),
           checksum(ws_web_site_sk),
           checksum(ws_ship_mode_sk),
           checksum(ws_warehouse_sk),
           checksum(ws_promo_sk),
           checksum(ws_order_number),
           checksum(ws_quantity),
           checksum(ws_wholesale_cost),
           checksum(ws_list_price),
           checksum(ws_sales_price),
           checksum(ws_ext_discount_amt),
           checksum(ws_ext_sales_price),
           checksum(ws_ext_wholesale_cost),
           checksum(ws_ext_list_price),
           checksum(ws_ext_tax),
           checksum(ws_coupon_amt),
           checksum(ws_ext_ship_cost),
           checksum(ws_net_paid),
           checksum(ws_net_paid_inc_tax),
           checksum(ws_net_paid_inc_ship),
           checksum(ws_net_paid_inc_ship_tax),
           checksum(ws_net_profit)
    ] checksums
from web_sales
    )
select checksum(cs) as table_checksum
from column_checksums
         cross join unnest(column_checksums.checksums) as x(cs);
