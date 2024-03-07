--#BGBLK 100

--set current schema blu2;
set current query optimization 5;

--SELECT COUNT(DISTINCT cs_bill_customer_sk)
--  FROM catalog_sales;
SELECT COUNT(DISTINCT ss_ticket_number)
  FROM store_sales;

--#EOBLK
