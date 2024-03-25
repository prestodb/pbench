--#BGBLK 1

 --set current schema bdinsights;

SELECT COUNT(DISTINCT cast(ws_sold_date_sk as varchar) || cast(ws_sold_time_sk as varchar)||cast(ws_ship_date_sk as varchar)) FROM web_sales;

--#EOBLK
