--#BGBLK 20

 --set current schema bdinsights; 
-- Near 100% host CPU on 2 cores
--  PID USER      PR  NI  VIRT  RES  SHR S %CPU %MEM    TIME+  COMMAND
-- 1702 nz        18   0 18936 1984 1584 R 90.3  0.0 101:34.10 nzsql
-- 1703 nz        15   0 1593m  38m  36m R 86.9  0.0  97:43.98 postmaster
-- Project, restrict, response to host
--SELECT cs_item_sk FROM catalog_sales WHERE cs_order_number%25=1;
SELECT ss_item_sk
FROM store_sales
JOIN date_dim dr ON (ss_sold_date_sk=dr.d_date_sk)
WHERE dr.d_year = 1998 and d_moy = 1 and d_dom <= 8
fetch first  1727688150 rows only;

--#EOBLK
