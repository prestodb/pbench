--#BGBLK 20

 --set current schema bdinsights; 
-- All sales from customers from Burlington who purchased store items
SELECT COUNT(*) FROM (
  SELECT ss.ss_ticket_number, ss_item_sk, c_first_name, ca_zip
  FROM store_sales ss
--  JOIN date_dim ON (d_date_sk=ss_sold_date_sk)
  JOIN customer ON (ss_customer_sk=c_customer_sk)
  JOIN customer_address ON (ca_address_sk=c_current_addr_sk)
  WHERE ca_city='Burlington'
  -- AND d_year=1999 AND d_qoy=4
) foo;

--#EOBLK
