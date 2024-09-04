-- ==============================================
-- 144662
-- 08 39 4f 4a 5b b6 95 4e
-- ==============================================
select count(*) from catalog_returns;

with column_checksums as (
    select array[
               checksum(cr_returned_date_sk),
           checksum(cr_returned_time_sk),
           checksum(cr_item_sk),
           checksum(cr_refunded_customer_sk),
           checksum(cr_refunded_cdemo_sk),
           checksum(cr_refunded_hdemo_sk),
           checksum(cr_refunded_addr_sk),
           checksum(cr_returning_customer_sk),
           checksum(cr_returning_cdemo_sk),
           checksum(cr_returning_hdemo_sk),
           checksum(cr_returning_addr_sk),
           checksum(cr_call_center_sk),
           checksum(cr_catalog_page_sk),
           checksum(cr_ship_mode_sk),
           checksum(cr_warehouse_sk),
           checksum(cr_reason_sk),
           checksum(cr_order_number),
           checksum(cr_return_quantity),
           checksum(cr_return_amount),
           checksum(cr_return_tax),
           checksum(cr_return_amt_inc_tax),
           checksum(cr_fee),
           checksum(cr_return_ship_cost),
           checksum(cr_refunded_cash),
           checksum(cr_reversed_charge),
           checksum(cr_store_credit),
           checksum(cr_net_loss)
    ] checksums
from catalog_returns
    )
select checksum(cs) as table_checksum
from column_checksums
         cross join unnest(column_checksums.checksums) as x(cs);
