SELECT DISTINCT
  a0.c0 AS CHANNEL,
  a0.c1 AS ITEM,
  a0.c2 AS RETURN_RATIO,
  a0.c3 AS RETURN_RANK,
  a0.c4 AS CURRENCY_RANK
FROM (
  (
    SELECT
      CAST('web' AS STRING) AS c0,
      a1.c0 AS c1,
      a1.c1 AS c2,
      a1.c2 AS c3,
      a1.c3 AS c4
    FROM (
      SELECT
        a2.c0 AS c0,
        a2.c1 AS c1,
        RANK() OVER (ORDER BY a2.c1 ASC NULLS LAST) AS c2,
        a2.c2 AS c3
      FROM (
        SELECT
          a3.c0 AS c0,
          a3.c1 AS c1,
          RANK() OVER (ORDER BY a3.c2 ASC NULLS LAST) AS c2
        FROM (
          SELECT
            a4.c0 AS c0,
            (
              CAST(a4.c1 AS DECIMAL(15, 4)) / CAST(a4.c2 AS DECIMAL(15, 4))
            ) AS c1,
            (
              CAST(a4.c3 AS DECIMAL(15, 4)) / CAST(a4.c4 AS DECIMAL(15, 4))
            ) AS c2
          FROM (
            SELECT
              a6.ws_item_sk AS c0,
              SUM(COALESCE(a5.wr_return_quantity, 0)) AS c1,
              SUM(COALESCE(a6.ws_quantity, 0)) AS c2,
              SUM(COALESCE(CAST(a5.wr_return_amt AS DECIMAL(13, 2)), 00000000000.00)) AS c3,
              SUM(COALESCE(CAST(a6.ws_net_paid AS DECIMAL(13, 2)), 00000000000.00)) AS c4
            FROM (
              web_returns AS a5
                INNER JOIN (
                  web_sales AS a6
                    INNER JOIN date_dim AS a7
                      ON (
                        (
                          a7.d_date_sk = a6.ws_sold_date_sk
                        )
                        AND (
                          1 < a6.ws_net_profit
                        )
                        AND (
                          0 < a6.ws_net_paid
                        )
                        AND (
                          0 < a6.ws_quantity
                        )
                        AND (
                          (
                            a6.ws_sold_date_sk <= 2452275
                          ) AND (
                            a6.ws_sold_date_sk >= 2452245
                          )
                        )
                        AND (
                          2001 = a7.d_year
                        )
                        AND (
                          12 = a7.d_moy
                        )
                      )
                )
                  ON (
                    (
                      a6.ws_item_sk = a5.wr_item_sk
                    )
                    AND (
                      a6.ws_order_number = a5.wr_order_number
                    )
                    AND (
                      10000 < a5.wr_return_amt
                    )
                  )
            )
            GROUP BY
              a6.ws_item_sk
          ) AS a4
        ) AS a3
      ) AS a2
    ) AS a1
    WHERE
      (
        (
          a1.c2 <= 10
        ) OR (
          a1.c3 <= 10
        )
      )
  )
  UNION ALL
  (
    SELECT
      'catalog' AS c0,
      a8.c0 AS c1,
      a8.c1 AS c2,
      a8.c2 AS c3,
      a8.c3 AS c4
    FROM (
      SELECT
        a9.c0 AS c0,
        a9.c1 AS c1,
        RANK() OVER (ORDER BY a9.c1 ASC NULLS LAST) AS c2,
        a9.c2 AS c3
      FROM (
        SELECT
          a10.c0 AS c0,
          a10.c1 AS c1,
          RANK() OVER (ORDER BY a10.c2 ASC NULLS LAST) AS c2
        FROM (
          SELECT
            a11.c0 AS c0,
            (
              CAST(a11.c1 AS DECIMAL(15, 4)) / CAST(a11.c2 AS DECIMAL(15, 4))
            ) AS c1,
            (
              CAST(a11.c3 AS DECIMAL(15, 4)) / CAST(a11.c4 AS DECIMAL(15, 4))
            ) AS c2
          FROM (
            SELECT
              a13.cs_item_sk AS c0,
              SUM(COALESCE(a12.cr_return_quantity, 0)) AS c1,
              SUM(COALESCE(a13.cs_quantity, 0)) AS c2,
              SUM(COALESCE(CAST(a12.cr_return_amount AS DECIMAL(13, 2)), 00000000000.00)) AS c3,
              SUM(COALESCE(CAST(a13.cs_net_paid AS DECIMAL(13, 2)), 00000000000.00)) AS c4
            FROM (
              catalog_returns AS a12
                INNER JOIN (
                  catalog_sales AS a13
                    INNER JOIN date_dim AS a14
                      ON (
                        (
                          a14.d_date_sk = a13.cs_sold_date_sk
                        )
                        AND (
                          1 < a13.cs_net_profit
                        )
                        AND (
                          0 < a13.cs_net_paid
                        )
                        AND (
                          0 < a13.cs_quantity
                        )
                        AND (
                          (
                            a13.cs_sold_date_sk <= 2452275
                          ) AND (
                            a13.cs_sold_date_sk >= 2452245
                          )
                        )
                        AND (
                          2001 = a14.d_year
                        )
                        AND (
                          12 = a14.d_moy
                        )
                      )
                )
                  ON (
                    (
                      a13.cs_item_sk = a12.cr_item_sk
                    )
                    AND (
                      a13.cs_order_number = a12.cr_order_number
                    )
                    AND (
                      10000 < a12.cr_return_amount
                    )
                  )
            )
            GROUP BY
              a13.cs_item_sk
          ) AS a11
        ) AS a10
      ) AS a9
    ) AS a8
    WHERE
      (
        (
          a8.c2 <= 10
        ) OR (
          a8.c3 <= 10
        )
      )
  )
  UNION ALL
  (
    SELECT
      CAST('store' AS STRING) AS c0,
      a15.c0 AS c1,
      a15.c1 AS c2,
      a15.c2 AS c3,
      a15.c3 AS c4
    FROM (
      SELECT
        a16.c0 AS c0,
        a16.c1 AS c1,
        RANK() OVER (ORDER BY a16.c1 ASC NULLS LAST) AS c2,
        a16.c2 AS c3
      FROM (
        SELECT
          a17.c0 AS c0,
          a17.c1 AS c1,
          RANK() OVER (ORDER BY a17.c2 ASC NULLS LAST) AS c2
        FROM (
          SELECT
            a18.c0 AS c0,
            (
              CAST(a18.c1 AS DECIMAL(15, 4)) / CAST(a18.c2 AS DECIMAL(15, 4))
            ) AS c1,
            (
              CAST(a18.c3 AS DECIMAL(15, 4)) / CAST(a18.c4 AS DECIMAL(15, 4))
            ) AS c2
          FROM (
            SELECT
              a20.ss_item_sk AS c0,
              SUM(COALESCE(a19.sr_return_quantity, 0)) AS c1,
              SUM(COALESCE(a20.ss_quantity, 0)) AS c2,
              SUM(COALESCE(CAST(a19.sr_return_amt AS DECIMAL(13, 2)), 00000000000.00)) AS c3,
              SUM(COALESCE(CAST(a20.ss_net_paid AS DECIMAL(13, 2)), 00000000000.00)) AS c4
            FROM (
              store_returns AS a19
                INNER JOIN (
                  store_sales AS a20
                    INNER JOIN date_dim AS a21
                      ON (
                        (
                          a21.d_date_sk = a20.ss_sold_date_sk
                        )
                        AND (
                          1 < a20.ss_net_profit
                        )
                        AND (
                          0 < a20.ss_net_paid
                        )
                        AND (
                          0 < a20.ss_quantity
                        )
                        AND (
                          (
                            a20.ss_sold_date_sk <= 2452275
                          ) AND (
                            a20.ss_sold_date_sk >= 2452245
                          )
                        )
                        AND (
                          2001 = a21.d_year
                        )
                        AND (
                          12 = a21.d_moy
                        )
                      )
                )
                  ON (
                    (
                      a20.ss_item_sk = a19.sr_item_sk
                    )
                    AND (
                      a20.ss_ticket_number = a19.sr_ticket_number
                    )
                    AND (
                      10000 < a19.sr_return_amt
                    )
                  )
            )
            GROUP BY
              a20.ss_item_sk
          ) AS a18
        ) AS a17
      ) AS a16
    ) AS a15
    WHERE
      (
        (
          a15.c2 <= 10
        ) OR (
          a15.c3 <= 10
        )
      )
  )
) AS a0
ORDER BY
  1 ASC NULLS LAST,
  4 ASC NULLS LAST,
  5 ASC NULLS LAST,
  3 ASC NULLS LAST,
  2 ASC NULLS LAST
LIMIT 100