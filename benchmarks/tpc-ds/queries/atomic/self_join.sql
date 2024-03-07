--#BGBLK 4

 --set current schema bdinsights; 
SELECT ss.ss_customer_sk, ss.ss_ticket_number, ss.ticket_sale, ss2.tot_sales, 
   CASE ss2.tot_sales WHEN 0 THEN 0 ELSE ss.ticket_sale/ss2.tot_sales END AS "PCT_OF_TOT_SALES"
  FROM  (SELECT ss_ticket_number, ss_customer_sk, SUM(ss_ext_sales_price) ticket_sale FROM store_sales GROUP BY ss_ticket_number, ss_customer_sk) ss
  INNER JOIN (SELECT ss_customer_sk, SUM(ss_ext_sales_price) tot_sales FROM store_sales GROUP BY ss_customer_sk) ss2 
     ON (ss.ss_customer_sk=ss2.ss_customer_sk)
  WHERE MOD(ss_ticket_number, 2) = 1 
  ;

--#EOBLK
