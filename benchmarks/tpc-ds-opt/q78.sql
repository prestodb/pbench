SELECT
  a0.c5 AS SS_SOLD_YEAR,
  a0.c4 AS SS_ITEM_SK,
  a0.c3 AS SS_CUSTOMER_SK,
  ROUND((
    a0.c2 / (
      COALESCE(a0.c8, 0) + COALESCE(a11.c3, 0)
    )
  ), 2) AS RATIO,
  a0.c2 AS STORE_QTY,
  a0.c1 AS STORE_WHOLESALE_COST,
  a0.c0 AS STORE_SALES_PRICE,
  (
    COALESCE(a0.c8, 0) + COALESCE(a11.c3, 0)
  ) AS OTHER_CHAN_QTY,
  (
    COALESCE(a0.c7, 00000000000000000000000000000.00) + COALESCE(a11.c4, 00000000000000000000000000000.00)
  ) AS OTHER_CHAN_WHOLESALE_COST,
  (
    COALESCE(a0.c6, 00000000000000000000000000000.00) + COALESCE(a11.c5, 00000000000000000000000000000.00)
  ) AS OTHER_CHAN_SALES_PRICE
FROM (
  (
    SELECT
      a1.c5 AS c0,
      a1.c4 AS c1,
      a1.c3 AS c2,
      a1.c2 AS c3,
      a1.c1 AS c4,
      a1.c0 AS c5,
      a6.c5 AS c6,
      a6.c4 AS c7,
      a6.c3 AS c8
    FROM (
      (
        SELECT
          a2.c0 AS c0,
          a2.c5 AS c1,
          a2.c4 AS c2,
          SUM(a2.c3) AS c3,
          SUM(a2.c2) AS c4,
          SUM(a2.c1) AS c5
        FROM (
          (
            SELECT
              a4.d_year AS c0,
              a3.ss_sales_price AS c1,
              a3.ss_wholesale_cost AS c2,
              a3.ss_quantity AS c3,
              a3.ss_customer_sk AS c4,
              a3.ss_item_sk AS c5,
              a3.ss_ticket_number AS c6
            FROM (
              store_sales AS a3
                INNER JOIN date_dim AS a4
                  ON (
                    (
                      a3.ss_sold_date_sk = a4.d_date_sk
                    )
                    AND (
                      (
                        a3.ss_sold_date_sk <= 2451910
                      ) AND (
                        a3.ss_sold_date_sk >= 2451545
                      )
                    )
                    AND (
                      a4.d_year = 2000
                    )
                  )
            )
          ) AS a2
          LEFT OUTER JOIN store_returns AS a5
            ON (
              a5.sr_ticket_number = a2.c6
            ) AND (
              a2.c5 = a5.sr_item_sk
            )
        )
        WHERE
          (
            a5.sr_ticket_number IS NULL
          )
        GROUP BY
          a2.c5,
          a2.c4,
          a2.c0
      ) AS a1
      LEFT OUTER JOIN (
        SELECT
          a7.c0 AS c0,
          a7.c5 AS c1,
          a7.c4 AS c2,
          SUM(a7.c3) AS c3,
          SUM(a7.c2) AS c4,
          SUM(a7.c1) AS c5
        FROM (
          (
            SELECT
              a9.d_year AS c0,
              a8.ws_sales_price AS c1,
              a8.ws_wholesale_cost AS c2,
              a8.ws_quantity AS c3,
              a8.ws_bill_customer_sk AS c4,
              a8.ws_item_sk AS c5,
              a8.ws_order_number AS c6
            FROM (
              web_sales AS a8
                INNER JOIN date_dim AS a9
                  ON (
                    (
                      a8.ws_sold_date_sk = a9.d_date_sk
                    )
                    AND (
                      (
                        a8.ws_sold_date_sk <= 2451910
                      ) AND (
                        a8.ws_sold_date_sk >= 2451545
                      )
                    )
                    AND (
                      a9.d_year = 2000
                    )
                  )
            )
          ) AS a7
          LEFT OUTER JOIN web_returns AS a10
            ON (
              a10.wr_order_number = a7.c6
            ) AND (
              a7.c5 = a10.wr_item_sk
            )
        )
        WHERE
          (
            a10.wr_order_number IS NULL
          )
        GROUP BY
          a7.c5,
          a7.c4,
          a7.c0
      ) AS a6
        ON (
          a6.c0 = a1.c0
        ) AND (
          a6.c1 = a1.c1
        ) AND (
          a6.c2 = a1.c2
        )
    )
  ) AS a0
  LEFT OUTER JOIN (
    SELECT
      a12.c0 AS c0,
      a12.c5 AS c1,
      a12.c4 AS c2,
      SUM(a12.c3) AS c3,
      SUM(a12.c2) AS c4,
      SUM(a12.c1) AS c5
    FROM (
      (
        SELECT
          a14.d_year AS c0,
          a13.cs_sales_price AS c1,
          a13.cs_wholesale_cost AS c2,
          a13.cs_quantity AS c3,
          a13.cs_bill_customer_sk AS c4,
          a13.cs_item_sk AS c5,
          a13.cs_order_number AS c6
        FROM (
          catalog_sales AS a13
            INNER JOIN date_dim AS a14
              ON (
                (
                  a13.cs_sold_date_sk = a14.d_date_sk
                )
                AND (
                  (
                    a13.cs_sold_date_sk <= 2451910
                  ) AND (
                    a13.cs_sold_date_sk >= 2451545
                  )
                )
                AND (
                  a14.d_year = 2000
                )
              )
        )
      ) AS a12
      LEFT OUTER JOIN catalog_returns AS a15
        ON (
          a15.cr_order_number = a12.c6
        ) AND (
          a12.c5 = a15.cr_item_sk
        )
    )
    WHERE
      (
        a15.cr_order_number IS NULL
      )
    GROUP BY
      a12.c5,
      a12.c4,
      a12.c0
  ) AS a11
    ON (
      a11.c0 = a0.c5
    ) AND (
      a11.c1 = a0.c4
    ) AND (
      a11.c2 = a0.c3
    )
)
WHERE
  (
    (
      COALESCE(a0.c8, 0) > 0
    ) OR (
      COALESCE(a11.c3, 0) > 0
    )
  )
ORDER BY
  2 ASC NULLS LAST,
  3 ASC NULLS LAST,
  5 DESC,
  6 DESC,
  7 DESC,
  8 ASC NULLS LAST,
  9 ASC NULLS LAST,
  10 ASC NULLS LAST,
  ROUND((
    a0.c2 / COALESCE((
      a0.c8 + a11.c3
    ), 1)
  ), 2) ASC NULLS LAST
LIMIT 100