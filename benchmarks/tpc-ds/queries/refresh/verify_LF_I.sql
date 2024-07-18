-- ==============================================
--    Fetch two rows randomly from iv:
-- ==============================================
with inv_random as (select
                        inv_date_sk,
                        inv_item_sk,
                        inv_warehouse_sk,
                        inv_quantity_on_hand
                    from iv
                    where inv_item_sk >= (select floor( max(inv_item_sk) * rand()) from iv )
                    order by inv_item_sk limit 2)

-- ========================================================
--    Verify the row can be selected from inventory:
-- ========================================================

select
    inv.inv_date_sk,
    inv.inv_item_sk,
    inv.inv_warehouse_sk,
    inv.inv_quantity_on_hand
from inventory inv, inv_random
where
        inv.inv_date_sk = inv_random.inv_date_sk and
        inv.inv_item_sk = inv_random.inv_item_sk and
        inv.inv_warehouse_sk = inv_random.inv_warehouse_sk;
