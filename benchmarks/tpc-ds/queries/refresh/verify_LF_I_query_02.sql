-- ==============================================
-- a1 56 0d 3e 9a f7 5f 9d
-- ==============================================
with column_checksums as (
    select array[
               checksum(inv_date_sk),
           checksum(inv_item_sk),
           checksum(inv_warehouse_sk),
           checksum(inv_quantity_on_hand)
    ] checksums
from inventory
    )
select checksum(cs) as table_checksum
from column_checksums
         cross join unnest(column_checksums.checksums) as x(cs);
