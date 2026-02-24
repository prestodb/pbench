-- ==============================================
-- ec 74 b5 fe ad 2c 44 57
-- ==============================================
with column_checksums as (
    select array[
               checksum(wr_returned_date_sk),
           checksum(wr_returned_time_sk),
           checksum(wr_item_sk),
           checksum(wr_refunded_customer_sk),
           checksum(wr_refunded_cdemo_sk),
           checksum(wr_refunded_hdemo_sk),
           checksum(wr_refunded_addr_sk),
           checksum(wr_returning_customer_sk),
           checksum(wr_returning_cdemo_sk),
           checksum(wr_returning_hdemo_sk),
           checksum(wr_returning_addr_sk),
           checksum(wr_web_page_sk),
           checksum(wr_reason_sk),
           checksum(wr_order_number),
           checksum(wr_return_quantity),
           checksum(wr_return_amt),
           checksum(wr_return_tax),
           checksum(wr_return_amt_inc_tax),
           checksum(wr_fee),
           checksum(wr_return_ship_cost),
           checksum(wr_refunded_cash),
           checksum(wr_reversed_charge),
           checksum(wr_account_credit),
           checksum(wr_net_loss)
    ] checksums
from web_returns
    )
select checksum(cs) as table_checksum
from column_checksums
         cross join unnest(column_checksums.checksums) as x(cs);
