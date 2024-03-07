--#BGBLK 50

 --set current schema bdinsights; 
-- JOIN-MULTI-HASH Compute-intensive, co-located, multi-hash star join.
SELECT AVG(cs1.cs_ext_sales_price)
FROM catalog_sales cs1
  INNER JOIN catalog_sales cs2 ON (
        cs1.cs_order_number = cs2.cs_order_number
    AND cs1.cs_item_sk = cs2.cs_item_sk
    AND cs1.cs_bill_customer_sk != cs2.cs_ship_customer_sk
  )
  INNER JOIN catalog_returns ON (
        cs1.cs_order_number = cr_order_number
    AND cs1.cs_item_sk=cr_item_sk
  )
  INNER JOIN date_dim ON (cr_returned_date_sk = d_date_sk)
WHERE d_date BETWEEN '1999-01-01' and '1999-02-01'
;

--#EOBLK
