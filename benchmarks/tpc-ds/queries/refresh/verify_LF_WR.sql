-- ==============================================
--    Fetch two rows randomly from wrv:
-- ==============================================
with wr_random as (
    select
        wr_return_date_sk,
        wr_return_time_sk,
        wr_item_sk,
        wr_refunded_customer_sk,
        wr_refunded_cdemo_sk,
        wr_refunded_hdemo_sk,
        wr_refunded_addr_sk,
        wr_returning_customer_sk,
        wr_returning_cdemo_sk,
        wr_returning_hdemo_sk,
        wr_returning_addr_sk,
        wr_web_page_sk,
        wr_reason_sk,
        wr_order_number,
        wr_return_quantity,
        wr_return_amt,
        wr_return_tax,
        wr_return_amt_inc_tax,
        wr_fee,
        wr_return_ship_cost,
        wr_refunded_cash,
        wr_reversed_charge,
        wr_account_credit,
        wr_net_loss
    from wrv
    where wr_return_time_sk >= (select floor( max(wr_return_time_sk) * rand()) from wrv )
    order by wr_return_time_sk limit 2)

-- ========================================================
--    Verify the row can be selected from web_returns:
-- ========================================================

select
    wr.wr_returned_date_sk,
    wr.wr_returned_time_sk,
    wr.wr_item_sk,
    wr.wr_refunded_customer_sk,
    wr.wr_refunded_cdemo_sk,
    wr.wr_refunded_hdemo_sk,
    wr.wr_refunded_addr_sk,
    wr.wr_returning_customer_sk,
    wr.wr_returning_cdemo_sk,
    wr.wr_returning_hdemo_sk,
    wr.wr_returning_addr_sk,
    wr.wr_web_page_sk,
    wr.wr_reason_sk,
    wr.wr_order_number,
    wr.wr_return_quantity,
    wr.wr_return_amt,
    wr.wr_return_tax,
    wr.wr_return_amt_inc_tax,
    wr.wr_fee,
    wr.wr_return_ship_cost,
    wr.wr_refunded_cash,
    wr.wr_reversed_charge,
    wr.wr_account_credit,
    wr.wr_net_loss
from web_returns wr, wr_random
where
        wr.wr_returned_date_sk = wr_random.wr_return_date_sk and
        wr.wr_returned_time_sk = wr_random.wr_return_time_sk and
        wr.wr_item_sk = wr_random.wr_item_sk and
        wr.wr_refunded_customer_sk = wr_random.wr_refunded_customer_sk and
        wr.wr_refunded_cdemo_sk = wr_random.wr_refunded_cdemo_sk and
        wr.wr_refunded_hdemo_sk = wr_random.wr_refunded_hdemo_sk and
        wr.wr_refunded_addr_sk = wr_random.wr_refunded_addr_sk and
        wr.wr_returning_customer_sk = wr_random.wr_returning_customer_sk and
        wr.wr_returning_cdemo_sk = wr_random.wr_returning_cdemo_sk and
        wr.wr_returning_hdemo_sk = wr_random.wr_returning_hdemo_sk and
        wr.wr_returning_addr_sk = wr_random.wr_returning_addr_sk and
        wr.wr_web_page_sk = wr_random.wr_web_page_sk and
        wr.wr_reason_sk = wr_random.wr_reason_sk;
