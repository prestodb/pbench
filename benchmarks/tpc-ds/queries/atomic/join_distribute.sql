--#BGBLK 10

 --set current schema bdinsights; 
-- JOIN-SPU-Distribute
SELECT MIN(cs_net_paid), MIN(cs_list_price), MIN(c_birth_year), MIN(c_last_name)
  FROM customer
  INNER JOIN catalog_sales  ON (cs_bill_customer_sk=c_customer_sk)
  WHERE cs_ext_sales_price < 10000.0;

--#EOBLK
