--#BGBLK 10

 --set current schema bdinsights; 
-- All customers who bought an item from the catalog and not a store
-- for a given year and quarter
SELECT COUNT(*) FROM (
  SELECT d_date, ss_ticket_number, ss_item_sk, ss_customer_sk, ss_sales_price
  FROM store_sales ss
  JOIN date_dim ON (ss_sold_date_sk=d_date_sk)
  WHERE d_year BETWEEN 1999 AND 2002 --AND d_qoy IN (3,4)
    AND ss_customer_sk IN (SELECT c_customer_sk FROM customer WHERE c_birth_year>1970)
) foo;

--#EOBLK
