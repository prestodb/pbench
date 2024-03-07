--#BGBLK 6

---- set current schema tpcds;
 -- JOIN-Host-Broadcast
 SELECT max(c_birth_year)
 FROM catalog_sales
 left outer JOIN customer  ON (cs_bill_customer_sk=c_customer_sk);

--#EOBLK
