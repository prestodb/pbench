--#BGBLK 10

 --set current schema bdinsights; 
SELECT sum( cast (ss.ss_customer_sk as decimal(15,4)))/count(ss.ss_customer_sk), sum(cast(ss.ss_ticket_number as decimal(15,4)))/count(ss.ss_ticket_number), 
   sum( cast(ss.ticket_sale as decimal(15,4)))/count(ss.ticket_sale), sum(cast (ss2.tot_sales as decimal(15,4)))/count(ss2.tot_sales), 
   sum( cast(CASE ss2.tot_sales WHEN 0 THEN 0 ELSE ss.ticket_sale/ss2.tot_sales END  as decimal(15,4))) AS "PCT_OF_TOT_SALES"
  FROM  (SELECT ss_ticket_number, ss_customer_sk, SUM(cast (ss_ext_sales_price as decimal(15,4))) ticket_sale FROM store_sales GROUP BY ss_ticket_number, ss_customer_sk) ss
  INNER JOIN (SELECT ss_customer_sk, SUM(cast(ss_ext_sales_price as decimal(15,4)))  tot_sales FROM store_sales GROUP BY ss_customer_sk) ss2 
     ON (ss.ss_customer_sk=ss2.ss_customer_sk)
  WHERE MOD(ss_ticket_number, 15) = 1;

--#EOBLK
