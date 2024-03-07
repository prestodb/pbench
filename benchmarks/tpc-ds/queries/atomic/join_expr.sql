--#BGBLK 100

 --set current schema bdinsights; 
-- No longer running this query as it does a merge join, not an expression join
SELECT COUNT(*)
FROM web_sales c1 
INNER JOIN web_returns c2 ON c1.ws_order_number+1=c2.wr_order_number+1;

--#EOBLK
