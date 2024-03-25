--#BGBLK 10

 --set current schema bdinsights; 
SELECT COUNT(*),MAX(ss_sold_date_sk), MIN(ss_sold_time_sk) FROM store_sales WHERE EXISTS (SELECT d_date_sk FROM date_dim WHERE d_year <= 2002 AND d_date_sk=ss_sold_date_sk);

--#EOBLK
