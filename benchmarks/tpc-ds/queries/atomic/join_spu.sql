--#BGBLK 5

 --set current schema bdinsights; 
-- JOIN-SPU Both tables distributed on join key.
SELECT AVG(ss_ext_sales_price)
  FROM store_sales ss
  INNER JOIN store_returns sr ON (
    ss.ss_ticket_number=sr.sr_ticket_number
    and ss.ss_item_sk=sr.sr_item_sk
  )
  WHERE MOD(ss_ticket_number, 15)=1;

--#EOBLK
