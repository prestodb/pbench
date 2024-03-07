--#BGBLK 3

 --set current schema bdinsights; 
SELECT COUNT_big(*)
FROM web_returns c1, date_dim
WHERE d_date_sk<2415022+1000; 

--#EOBLK
