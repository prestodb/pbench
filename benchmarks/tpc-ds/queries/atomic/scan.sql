--#BGBLK 50

 --set current schema bdinsights; 
-- SCAN Single rows returned
SELECT count(*) FROM store_sales WHERE ss_net_profit=0.0;

--#EOBLK
