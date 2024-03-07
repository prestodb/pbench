--#BGBLK 6

 --set current schema bdinsights; 
-- JOIN-Host-Broadcast
SELECT MIN(cs_net_paid), MIN(cs_list_price), MIN(c_birth_year)
FROM customer 
INNER JOIN catalog_sales  ON (cs_bill_customer_sk=c_customer_sk)
WHERE c_current_addr_sk>=(SELECT MIN(ca_address_sk) FROM customer_address)
  AND c_current_cdemo_sk>=(SELECT MIN(cd_demo_sk) FROM customer_demographics)
  AND c_current_hdemo_sk>=(SELECT MIN(hd_demo_sk) FROM household_demographics)
  AND c_first_shipto_date_sk>=(SELECT MIN(d_date_sk) FROM date_dim)
  AND c_first_sales_date_sk>=(SELECT MIN(d_date_sk) FROM date_dim)
;

--#EOBLK
