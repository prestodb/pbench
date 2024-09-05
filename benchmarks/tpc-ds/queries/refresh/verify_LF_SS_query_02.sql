-- ==============================================
-- 91 7f 5e 5c 0b 2d 03 72
-- ==============================================
with column_checksums as (
    select array[
               checksum(ss_sold_date_sk),
           checksum(ss_sold_time_sk),
           checksum(ss_item_sk),
           checksum(ss_customer_sk),
           checksum(ss_cdemo_sk),
           checksum(ss_hdemo_sk),
           checksum(ss_addr_sk),
           checksum(ss_store_sk),
           checksum(ss_promo_sk),
           checksum(ss_ticket_number),
           checksum(ss_quantity),
           checksum(ss_wholesale_cost),
           checksum(ss_list_price),
           checksum(ss_sales_price),
           checksum(ss_ext_discount_amt),
           checksum(ss_ext_sales_price),
           checksum(ss_ext_wholesale_cost),
           checksum(ss_ext_list_price),
           checksum(ss_ext_tax),
           checksum(ss_coupon_amt),
           checksum(ss_net_paid),
           checksum(ss_net_paid_inc_tax),
           checksum(ss_net_profit)
    ] checksums
from store_sales
    )
select checksum(cs) as table_checksum
from column_checksums
         cross join unnest(column_checksums.checksums) as x(cs);
