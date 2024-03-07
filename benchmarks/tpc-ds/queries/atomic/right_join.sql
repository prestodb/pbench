--#BGBLK 2

 --set current schema bdinsights; 
-- All customers who bought an item from the catalog and not a store
-- for a given year and quarter
SELECT COUNT(*), MAX(ws_item_sk), MIN(wr_returned_date_sk) FROM (
  SELECT d_date, ws_order_number, ws_item_sk, ws_bill_customer_sk, ws_sales_price, wr_returned_date_sk 
  FROM web_returns wr
  JOIN date_dim dr ON (wr_returned_date_sk=dr.d_date_sk AND dr.d_year <= 1920)
  RIGHT JOIN web_sales ws ON (ws_order_number=wr_order_number AND ws_item_sk=wr_item_sk 
    AND ws_sales_price>=10.00 AND ws_sold_date_sk BETWEEN 2451820 AND 2451821)
) foo;

--#EOBLK
