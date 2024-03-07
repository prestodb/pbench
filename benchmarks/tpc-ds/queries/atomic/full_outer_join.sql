--#BGBLK 30

 --set current schema bdinsights; 
  SELECT SUM(sr_return_amt), SUM(ss_sales_price)
  FROM store_sales ss
  JOIN date_dim ON (ss_sold_date_sk=d_date_sk)
  FULL OUTER JOIN store_returns sr ON (ss_ticket_number=sr_ticket_number AND ss_item_sk=sr_item_sk)
  WHERE d_year=1999 AND d_qoy <= 2 --AND ss_sales_price>=10.00;

--#EOBLK
