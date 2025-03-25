SELECT
  a3.ca_zip AS CA_ZIP,
  SUM(a0.cs_sales_price)
FROM (
  (
    catalog_sales AS a0
      INNER JOIN date_dim AS a1
        ON (
          (
            a0.cs_sold_date_sk = a1.d_date_sk
          )
          AND (
            (
              a0.cs_sold_date_sk <= 2452092
            ) AND (
              a0.cs_sold_date_sk >= 2452002
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
      a0.cs_bill_customer_sk = a2.c_customer_sk
    )
    AND (
      (
        (
          SUBSTRING(a3.ca_zip, 1, 5) IN ('85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792')
        )
        OR (
          a3.ca_state IN ('CA', 'WA', 'GA')
        )
      )
      OR (
        a0.cs_sales_price > 500
      )
    )
)
GROUP BY
  a3.ca_zip
ORDER BY
  1 ASC NULLS LAST
LIMIT 100