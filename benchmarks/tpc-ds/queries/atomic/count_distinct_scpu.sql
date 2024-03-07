--#BGBLK 1

 --set current schema bdinsights;

SELECT COUNT(DISTINCT ws_sold_date_sk||ws_sold_time_sk||ws_ship_date_sk) FROM web_sales;

--#EOBLK
