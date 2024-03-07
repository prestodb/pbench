--#BGBLK 150

 --set current schema bdinsights; 
SELECT COUNT(*)
FROM web_sales c1 
INNER JOIN date_dim ON (c1.ws_sold_date_sk=d_date_sk)
INNER JOIN warehouse c2 ON FLOAT(c1.ws_warehouse_sk)=FLOAT(c2.w_warehouse_sk)
WHERE d_year <= 2001;

--#EOBLK
