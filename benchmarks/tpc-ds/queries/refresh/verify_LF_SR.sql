with sr_random as
         (select
              sr_returned_date_sk,
              sr_return_time_sk,
              sr_item_sk,
              sr_customer_sk,
              sr_cdemo_sk,
              sr_hdemo_sk,
              sr_addr_sk,
              sr_store_sk,
              sr_reason_sk,
              sr_ticket_number,
              sr_return_quantity,
              sr_return_amt,
              sr_return_tax,
              sr_return_amt_inc_tax,
              sr_fee,
              sr_return_ship_cost,
              sr_refunded_cash,
              sr_reversed_charge,
              sr_store_credit,
              sr_net_loss
          from srv
          where sr_return_time_sk >= (select floor( max(sr_return_time_sk) * rand()) from srv )
          order by sr_return_time_sk limit 2)
-- ========================================================
--    Verify the row can be selected from store_returns:
-- ========================================================

select
    sr.sr_returned_date_sk,
    sr.sr_return_time_sk,
    sr.sr_item_sk,
    sr.sr_customer_sk,
    sr.sr_cdemo_sk,
    sr.sr_hdemo_sk,
    sr.sr_addr_sk,
    sr.sr_store_sk,
    sr.sr_reason_sk,
    sr.sr_ticket_number,
    sr.sr_return_quantity,
    sr.sr_return_amt,
    sr.sr_return_tax,
    sr.sr_return_amt_inc_tax,
    sr.sr_fee,
    sr.sr_return_ship_cost,
    sr.sr_refunded_cash,
    sr.sr_reversed_charge,
    sr.sr_store_credit,
    sr.sr_net_loss
from store_returns sr, sr_random
where
        sr.sr_returned_date_sk =     sr_random.sr_returned_date_sk  and
        sr.sr_return_time_sk = sr_random.sr_return_time_sk and
        sr.sr_item_sk = sr_random.sr_item_sk and
        sr.sr_customer_sk = sr_random.sr_customer_sk and
        sr.sr_cdemo_sk = sr_random.sr_cdemo_sk and
        sr.sr_hdemo_sk  = sr_random.sr_hdemo_sk and
        sr.sr_addr_sk = sr_random.sr_addr_sk and
        sr.sr_store_sk = sr_random.sr_store_sk and
        sr.sr_reason_sk = sr_random.sr_reason_sk;
