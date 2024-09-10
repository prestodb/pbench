-- ==============================================
-- fd 54 dc 73 48 5d da 70
-- ==============================================
with column_checksums as (
    select array[
               checksum(cs_sold_date_sk),
           checksum(cs_sold_time_sk),
           checksum(cs_ship_date_sk),
           checksum(cs_bill_customer_sk),
           checksum(cs_bill_cdemo_sk),
           checksum(cs_bill_hdemo_sk),
           checksum(cs_bill_addr_sk),
           checksum(cs_ship_customer_sk),
           checksum(cs_ship_cdemo_sk),
           checksum(cs_ship_hdemo_sk),
           checksum(cs_ship_addr_sk),
           checksum(cs_call_center_sk),
           checksum(cs_catalog_page_sk),
           checksum(cs_ship_mode_sk),
           checksum(cs_warehouse_sk),
           checksum(cs_item_sk),
           checksum(cs_promo_sk),
           checksum(cs_order_number),
           checksum(cs_quantity),
           checksum(cs_wholesale_cost),
           checksum(cs_list_price),
           checksum(cs_sales_price),
           checksum(cs_ext_discount_amt),
           checksum(cs_ext_sales_price),
           checksum(cs_ext_wholesale_cost),
           checksum(cs_ext_list_price),
           checksum(cs_ext_tax),
           checksum(cs_coupon_amt),
           checksum(cs_ext_ship_cost),
           checksum(cs_net_paid),
           checksum(cs_net_paid_inc_tax),
           checksum(cs_net_paid_inc_ship),
           checksum(cs_net_paid_inc_ship_tax),
           checksum(cs_net_profit)
    ] checksums
from catalog_sales
    )
select checksum(cs) as table_checksum
from column_checksums
         cross join unnest(column_checksums.checksums) as x(cs);
