--#BGBLK 10

 --set current schema bdinsights; 
-- All customers who bought a given item from both the catalog and a store
-- for a given year and quarter
SELECT COUNT(*) FROM (
  SELECT cs_sold_date_sk, cs_bill_customer_sk, cs_item_sk
  FROM catalog_sales
  JOIN date_dim ON (cs_sold_date_sk=d_date_sk)
  WHERE d_year=1999 AND d_dom <= 4
  INTERSECT 
  SELECT ss_sold_date_sk, ss_customer_sk, ss_item_sk
  FROM store_sales
  JOIN date_dim ON (ss_sold_date_sk=d_date_sk)
  WHERE d_year=1999 AND d_dom <= 4
) foo;



--#EOBLK
