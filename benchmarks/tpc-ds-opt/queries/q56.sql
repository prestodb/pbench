SELECT
  a0.c0 AS I_ITEM_ID,
  SUM(a0.c1) AS TOTAL_SALES
FROM (
  (
    SELECT
      a1.c0 AS c0,
      SUM(a1.c1) AS c1
    FROM (
      SELECT DISTINCT
        a4.i_item_id AS c0,
        a2.ss_ext_sales_price AS c1,
        a4.i_item_sk AS c2,
        a6.ca_address_sk AS c3,
        a3.d_date_sk AS c4,
        a2.ss_item_sk AS c5,
        a2.ss_ticket_number AS c6
      FROM (
        (
          (
            store_sales AS a2
              INNER JOIN date_dim AS a3
                ON (
                  (
                    a2.ss_sold_date_sk = a3.d_date_sk
                  )
                  AND (
                    (
                      a2.ss_addr_sk <= 32500000
                    ) AND (
                      a2.ss_addr_sk >= 2
                    )
                  )
                  AND (
                    (
                      a2.ss_sold_date_sk <= 2451969
                    ) AND (
                      a2.ss_sold_date_sk >= 2451942
                    )
                  )
                  AND (
                    a3.d_year = 2001
                  )
                  AND (
                    a3.d_moy = 2
                  )
                )
          )
          INNER JOIN (
            item AS a4
              INNER JOIN item AS a5
                ON (
                  (
                    a4.i_item_id = a5.i_item_id
                  )
                  AND (
                    a5.i_color IN ('slate', 'blanched', 'burnished')
                  )
                )
          )
            ON (
              a2.ss_item_sk = a4.i_item_sk
            )
        )
        INNER JOIN customer_address AS a6
          ON (
            (
              a2.ss_addr_sk = a6.ca_address_sk
            ) AND (
              a6.ca_gmt_offset = -005.00
            )
          )
      )
    ) AS a1
    GROUP BY
      a1.c0
  )
  UNION ALL
  (
    SELECT
      a7.c0 AS c0,
      SUM(a7.c1) AS c1
    FROM (
      SELECT DISTINCT
        a10.i_item_id AS c0,
        a8.cs_ext_sales_price AS c1,
        a10.i_item_sk AS c2,
        a12.ca_address_sk AS c3,
        a9.d_date_sk AS c4,
        a8.cs_item_sk AS c5,
        a8.cs_order_number AS c6
      FROM (
        (
          (
            catalog_sales AS a8
              INNER JOIN date_dim AS a9
                ON (
                  (
                    a8.cs_sold_date_sk = a9.d_date_sk
                  )
                  AND (
                    (
                      a8.cs_bill_addr_sk <= 32500000
                    ) AND (
                      a8.cs_bill_addr_sk >= 2
                    )
                  )
                  AND (
                    (
                      a8.cs_sold_date_sk <= 2451969
                    ) AND (
                      a8.cs_sold_date_sk >= 2451942
                    )
                  )
                  AND (
                    a9.d_year = 2001
                  )
                  AND (
                    a9.d_moy = 2
                  )
                )
          )
          INNER JOIN (
            item AS a10
              INNER JOIN item AS a11
                ON (
                  (
                    a10.i_item_id = a11.i_item_id
                  )
                  AND (
                    a11.i_color IN ('slate', 'blanched', 'burnished')
                  )
                )
          )
            ON (
              a8.cs_item_sk = a10.i_item_sk
            )
        )
        INNER JOIN customer_address AS a12
          ON (
            (
              a8.cs_bill_addr_sk = a12.ca_address_sk
            )
            AND (
              a12.ca_gmt_offset = -005.00
            )
          )
      )
    ) AS a7
    GROUP BY
      a7.c0
  )
  UNION ALL
  (
    SELECT
      a13.c0 AS c0,
      SUM(a13.c1) AS c1
    FROM (
      SELECT DISTINCT
        a16.i_item_id AS c0,
        a14.ws_ext_sales_price AS c1,
        a16.i_item_sk AS c2,
        a18.ca_address_sk AS c3,
        a15.d_date_sk AS c4,
        a14.ws_item_sk AS c5,
        a14.ws_order_number AS c6
      FROM (
        (
          (
            web_sales AS a14
              INNER JOIN date_dim AS a15
                ON (
                  (
                    a14.ws_sold_date_sk = a15.d_date_sk
                  )
                  AND (
                    (
                      a14.ws_bill_addr_sk <= 32500000
                    ) AND (
                      a14.ws_bill_addr_sk >= 2
                    )
                  )
                  AND (
                    (
                      a14.ws_sold_date_sk <= 2451969
                    ) AND (
                      a14.ws_sold_date_sk >= 2451942
                    )
                  )
                  AND (
                    a15.d_year = 2001
                  )
                  AND (
                    a15.d_moy = 2
                  )
                )
          )
          INNER JOIN (
            item AS a16
              INNER JOIN item AS a17
                ON (
                  (
                    a16.i_item_id = a17.i_item_id
                  )
                  AND (
                    a17.i_color IN ('slate', 'blanched', 'burnished')
                  )
                )
          )
            ON (
              a14.ws_item_sk = a16.i_item_sk
            )
        )
        INNER JOIN customer_address AS a18
          ON (
            (
              a14.ws_bill_addr_sk = a18.ca_address_sk
            )
            AND (
              a18.ca_gmt_offset = -005.00
            )
          )
      )
    ) AS a13
    GROUP BY
      a13.c0
  )
) AS a0
GROUP BY
  a0.c0
ORDER BY
  2 ASC NULLS LAST
LIMIT 100