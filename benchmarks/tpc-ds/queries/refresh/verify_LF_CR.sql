-- ==============================================
--    Fetch two rows randomly from crv:
-- ==============================================
with crv_random as (
    select
        cr_return_date_sk,
        cr_return_time_sk,
        cr_item_sk,
        cr_refunded_customer_sk,
        cr_refunded_cdemo_sk,
        cr_refunded_hdemo_sk,
        cr_refunded_addr_sk,
        cr_returning_customer_sk,
        cr_returning_cdemo_sk,
        cr_returning_hdemo_sk,
        cr_returning_addr_sk,
        cr_call_center_sk,
        cr_catalog_page_sk,
        cr_ship_mode_sk,
        cr_warehouse_sk,
        cr_reason_sk,
        cr_order_number,
        cr_return_quantity,
        cr_return_amount,
        cr_return_tax,
        cr_return_amt_inc_tax,
        cr_fee,
        cr_return_ship_cost,
        cr_refunded_cash,
        cr_reversed_charge,
        cr_merchant_credit,
        cr_net_loss
    from crv
    where cr_return_time_sk >= (select floor( max(cr_return_time_sk) * rand()) from crv )
    order by cr_return_time_sk limit 2)
-- ========================================================
--    Verify the row can be selected from catalog_returns:
-- ========================================================
select
    cr.cr_returned_date_sk,
    cr.cr_returned_time_sk,
    cr.cr_item_sk,
    cr.cr_refunded_customer_sk,
    cr.cr_refunded_cdemo_sk,
    cr.cr_refunded_hdemo_sk,
    cr.cr_refunded_addr_sk,
    cr.cr_returning_customer_sk,
    cr.cr_returning_cdemo_sk,
    cr.cr_returning_hdemo_sk,
    cr.cr_returning_addr_sk,
    cr.cr_call_center_sk,
    cr.cr_catalog_page_sk,
    cr.cr_ship_mode_sk,
    cr.cr_warehouse_sk,
    cr.cr_reason_sk,
    cr.cr_order_number,
    cr.cr_return_quantity,
    cr.cr_return_amount,
    cr.cr_return_tax,
    cr.cr_return_amt_inc_tax,
    cr.cr_fee,
    cr.cr_return_ship_cost,
    cr.cr_refunded_cash,
    cr.cr_reversed_charge,
    cr.cr_store_credit,
    cr.cr_net_loss
from catalog_returns cr, crv_random
where
        cr.cr_returned_date_sk = crv_random.cr_return_date_sk and
        cr.cr_returned_time_sk = crv_random.cr_return_time_sk and
        cr.cr_item_sk = crv_random.cr_item_sk and
        cr.cr_refunded_customer_sk = crv_random.cr_refunded_customer_sk and
        cr.cr_refunded_cdemo_sk = crv_random.cr_refunded_cdemo_sk and
        cr.cr_refunded_hdemo_sk = crv_random.cr_refunded_hdemo_sk and
        cr.cr_refunded_addr_sk = crv_random.cr_refunded_addr_sk and
        cr.cr_returning_customer_sk = crv_random.cr_returning_customer_sk and
        cr.cr_returning_cdemo_sk = crv_random.cr_returning_cdemo_sk and
        cr.cr_returning_hdemo_sk  = crv_random.cr_returning_hdemo_sk and
        cr.cr_returning_addr_sk = crv_random.cr_returning_addr_sk and
        cr.cr_call_center_sk = crv_random.cr_call_center_sk and
        cr.cr_catalog_page_sk = crv_random.cr_catalog_page_sk and
        cr.cr_ship_mode_sk = crv_random.cr_ship_mode_sk and
        cr.cr_warehouse_sk = crv_random.cr_warehouse_sk and
        cr.cr_reason_sk = crv_random.cr_reason_sk;