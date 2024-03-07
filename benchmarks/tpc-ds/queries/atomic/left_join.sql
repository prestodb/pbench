--#BGBLK 100

 --set current schema bdinsights; 
-- All customers who bought an item from the catalog and not a store
-- for a given year and quarter
SELECT COUNT(*) FROM (
  SELECT d_date, ss_ticket_number, ss_item_sk, ss_customer_sk, ss_sales_price, sr_returned_date_sk 
  FROM store_sales ss
  JOIN date_dim ON (ss_sold_date_sk=d_date_sk)
  LEFT JOIN store_returns sr ON (ss_ticket_number=sr_ticket_number AND ss_item_sk=sr_item_sk)
  WHERE d_year=1999
   -- AND d_qoy IN (3,4)
    AND ss_sales_price>=10.00
) foo;

--#EOBLK
