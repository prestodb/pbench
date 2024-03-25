--#BGBLK 1

 --set current schema bdinsights; 
SELECT COUNT(*), MAX(wr_returned_date_sk), MIN(wr_returned_time_sk)
FROM web_returns c1
CROSS JOIN date_dim
WHERE d_date_sk<2415822;

--#EOBLK
