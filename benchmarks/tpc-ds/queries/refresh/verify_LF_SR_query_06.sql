-- ==============================================
-- be ab 61 0d ae 90 25 86
-- ==============================================
with column_checksums as (
    select array[
               checksum(sr_returned_date_sk),
           checksum(sr_return_time_sk),
           checksum(sr_item_sk),
           checksum(sr_customer_sk),
           checksum(sr_cdemo_sk),
           checksum(sr_hdemo_sk),
           checksum(sr_addr_sk),
           checksum(sr_store_sk),
           checksum(sr_reason_sk),
           checksum(sr_ticket_number),
           checksum(sr_return_quantity),
           checksum(sr_return_amt),
           checksum(sr_return_tax),
           checksum(sr_return_amt_inc_tax),
           checksum(sr_fee),
           checksum(sr_return_ship_cost),
           checksum(sr_refunded_cash),
           checksum(sr_reversed_charge),
           checksum(sr_store_credit),
           checksum(sr_net_loss)
    ] checksums
from store_returns
    )
select checksum(cs) as table_checksum
from column_checksums
         cross join unnest(column_checksums.checksums) as x(cs);
