--#BGBLK 20

 --set current schema bdinsights; 
SELECT sum( cast (c_customer_sk as dec(15,4)))/count_big(c_customer_sk), MAX(c_customer_id), --MAX(c_first_name), MAX(c_last_name), 
  sum(cast (ss_item_sk as dec(15,4)) )/count_big(ss_item_sk),
  sum(cast (ss_net_profit as dec(15,4)))/count_big(ss_net_profit) 
FROM customer 
INNER JOIN store_sales ON c_customer_sk=ss_customer_sk
WHERE ss_sold_date_sk<=2450816+10;

--#EOBLK
