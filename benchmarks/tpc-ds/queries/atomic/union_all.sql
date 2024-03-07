--#BGBLK 100

 --set current schema bdinsights; 
-- almost exactly 100% single core CPU equivalent, spread over multipe cores
--  PID USER      PR  NI  VIRT  RES  SHR S %CPU %MEM    TIME+  COMMAND
--22558 nz        25   0 1904m 359m 358m R 100.2  0.4   3:32.88 dbos
-- All customers who bought an item from either the catalog or a store or both
-- for a given year and quarter
SELECT COUNT(*) FROM (
  SELECT cs_sold_date_sk, cs_bill_customer_sk, cs_item_sk
  FROM catalog_sales
  JOIN date_dim ON (cs_sold_date_sk=d_date_sk)
  WHERE d_year=1999 AND d_dom <= 4
  UNION ALL
  SELECT ss_sold_date_sk, ss_customer_sk, ss_item_sk
  FROM store_sales
  JOIN date_dim ON (ss_sold_date_sk=d_date_sk)
  WHERE d_year=1999 AND d_dom <= 4
) foo;



--#EOBLK
