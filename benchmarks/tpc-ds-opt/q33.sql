SELECT
  a0.c0 AS I_MANUFACT_ID,
  SUM(a0.c1) AS TOTAL_SALES
FROM (
  (
    SELECT
      a4.i_manufact_id AS c0,
      SUM(a1.ss_ext_sales_price) AS c1
    FROM (
      (
        (
          store_sales AS a1
            INNER JOIN date_dim AS a2
              ON (
                (
                  a1.ss_sold_date_sk = a2.d_date_sk
                )
                AND (
                  (
                    a1.ss_addr_sk <= 32500000
                  ) AND (
                    a1.ss_addr_sk >= 2
                  )
                )
                AND (
                  (
                    a1.ss_sold_date_sk <= 2450965
                  ) AND (
                    a1.ss_sold_date_sk >= 2450935
                  )
                )
                AND (
                  a2.d_year = 1998
                )
                AND (
                  a2.d_moy = 5
                )
              )
        )
        INNER JOIN customer_address AS a3
          ON (
            (
              a1.ss_addr_sk = a3.ca_address_sk
            ) AND (
              a3.ca_gmt_offset = -005.00
            )
          )
      )
      INNER JOIN (
        item AS a4
          INNER JOIN (
            SELECT DISTINCT
              a6.i_manufact_id AS c0
            FROM item AS a6
            WHERE
              (
                a6.i_category = 'Electronics'
              )
          ) AS a5
            ON (
              a4.i_manufact_id = a5.c0
            )
      )
        ON (
          a1.ss_item_sk = a4.i_item_sk
        )
    )
    GROUP BY
      a4.i_manufact_id
  )
  UNION ALL
  (
    SELECT
      a10.i_manufact_id AS c0,
      SUM(a7.cs_ext_sales_price) AS c1
    FROM (
      (
        (
          catalog_sales AS a7
            INNER JOIN date_dim AS a8
              ON (
                (
                  a7.cs_sold_date_sk = a8.d_date_sk
                )
                AND (
                  (
                    a7.cs_bill_addr_sk <= 32500000
                  ) AND (
                    a7.cs_bill_addr_sk >= 2
                  )
                )
                AND (
                  (
                    a7.cs_sold_date_sk <= 2450965
                  ) AND (
                    a7.cs_sold_date_sk >= 2450935
                  )
                )
                AND (
                  a8.d_year = 1998
                )
                AND (
                  a8.d_moy = 5
                )
              )
        )
        INNER JOIN customer_address AS a9
          ON (
            (
              a7.cs_bill_addr_sk = a9.ca_address_sk
            ) AND (
              a9.ca_gmt_offset = -005.00
            )
          )
      )
      INNER JOIN (
        item AS a10
          INNER JOIN (
            SELECT DISTINCT
              a12.i_manufact_id AS c0
            FROM item AS a12
            WHERE
              (
                a12.i_category = 'Electronics'
              )
          ) AS a11
            ON (
              a10.i_manufact_id = a11.c0
            )
      )
        ON (
          a7.cs_item_sk = a10.i_item_sk
        )
    )
    GROUP BY
      a10.i_manufact_id
  )
  UNION ALL
  (
    SELECT
      a16.i_manufact_id AS c0,
      SUM(a13.ws_ext_sales_price) AS c1
    FROM (
      (
        (
          web_sales AS a13
            INNER JOIN date_dim AS a14
              ON (
                (
                  a13.ws_sold_date_sk = a14.d_date_sk
                )
                AND (
                  (
                    a13.ws_bill_addr_sk <= 32500000
                  ) AND (
                    a13.ws_bill_addr_sk >= 2
                  )
                )
                AND (
                  (
                    a13.ws_sold_date_sk <= 2450965
                  ) AND (
                    a13.ws_sold_date_sk >= 2450935
                  )
                )
                AND (
                  a14.d_year = 1998
                )
                AND (
                  a14.d_moy = 5
                )
              )
        )
        INNER JOIN customer_address AS a15
          ON (
            (
              a13.ws_bill_addr_sk = a15.ca_address_sk
            )
            AND (
              a15.ca_gmt_offset = -005.00
            )
          )
      )
      INNER JOIN (
        item AS a16
          INNER JOIN (
            SELECT DISTINCT
              a18.i_manufact_id AS c0
            FROM item AS a18
            WHERE
              (
                a18.i_category = 'Electronics'
              )
          ) AS a17
            ON (
              a16.i_manufact_id = a17.c0
            )
      )
        ON (
          a13.ws_item_sk = a16.i_item_sk
        )
    )
    GROUP BY
      a16.i_manufact_id
  )
) AS a0
GROUP BY
  a0.c0
ORDER BY
  2 ASC NULLS LAST
LIMIT 100