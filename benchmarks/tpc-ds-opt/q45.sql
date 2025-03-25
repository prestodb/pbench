SELECT
  a3.ca_zip AS `CA_ZIP`,
  a3.ca_city AS `CA_CITY`,
  SUM(a0.ws_sales_price)
FROM (
  (
    (
      web_sales AS a0
        INNER JOIN date_dim AS a1
          ON (
            (
              a0.ws_sold_date_sk = a1.d_date_sk
            )
            AND (
              (
                a0.ws_sold_date_sk <= 2452092
              ) AND (
                a0.ws_sold_date_sk >= 2452002
              )
            )
            AND (
              a1.d_qoy = 2
            )
            AND (
              a1.d_year = 2001
            )
          )
    )
    INNER JOIN (
      customer AS a2
        INNER JOIN customer_address AS a3
          ON (
            a2.c_current_addr_sk = a3.ca_address_sk
          )
    )
      ON (
        a0.ws_bill_customer_sk = a2.c_customer_sk
      )
  )
  INNER JOIN item AS a4
    ON (
      a0.ws_item_sk = a4.i_item_sk
    )
    AND (
      SUBSTRING(a3.ca_zip, 1, 5) IN ('85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792')
      OR a4.i_item_id IN (
        SELECT
          a5.i_item_id
        FROM item AS a5
        WHERE a5.i_item_sk IN (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)
      )
    )
)
GROUP BY
  a3.ca_zip,
  a3.ca_city
ORDER BY
  1 ASC NULLS LAST,
  2 ASC NULLS LAST
LIMIT 100;
