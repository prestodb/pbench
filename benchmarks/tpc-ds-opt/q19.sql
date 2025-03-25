SELECT
  a4.i_brand_id AS BRAND_ID,
  a4.i_brand AS BRAND,
  a4.i_manufact_id AS I_MANUFACT_ID,
  a4.i_manufact AS I_MANUFACT,
  SUM(a2.ss_ext_sales_price) AS EXT_PRICE
FROM (
  customer_address AS a0
    INNER JOIN (
      (
        customer AS a1
          INNER JOIN (
            (
              store_sales AS a2
                INNER JOIN date_dim AS a3
                  ON (
                    (
                      a3.d_date_sk = a2.ss_sold_date_sk
                    )
                    AND (
                      (
                        a2.ss_sold_date_sk <= 2451148
                      ) AND (
                        a2.ss_sold_date_sk >= 2451119
                      )
                    )
                    AND (
                      (
                        a2.ss_item_sk <= 401979
                      ) AND (
                        a2.ss_item_sk >= 9
                      )
                    )
                    AND (
                      a3.d_moy = 11
                    )
                    AND (
                      a3.d_year = 1998
                    )
                  )
            )
            INNER JOIN item AS a4
              ON (
                (
                  a2.ss_item_sk = a4.i_item_sk
                ) AND (
                  a4.i_manager_id = 8
                )
              )
          )
            ON (
              a2.ss_customer_sk = a1.c_customer_sk
            )
      )
      INNER JOIN store AS a5
        ON (
          a2.ss_store_sk = a5.s_store_sk
        )
    )
      ON (
        a1.c_current_addr_sk = a0.ca_address_sk
      )
      AND (
        SUBSTRING(a0.ca_zip, 1, 5) <> SUBSTRING(a5.s_zip, 1, 5)
      )
)
GROUP BY
  a4.i_brand,
  a4.i_brand_id,
  a4.i_manufact_id,
  a4.i_manufact
ORDER BY
  5 DESC,
  2 ASC NULLS LAST,
  1 ASC NULLS LAST,
  3 ASC NULLS LAST,
  4 ASC NULLS LAST
LIMIT 100