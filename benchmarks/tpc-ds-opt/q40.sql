SELECT
  a0.c4 AS W_STATE,
  a0.c0 AS I_ITEM_ID,
  SUM(
    CASE
      WHEN (
        a0.c5 < CAST('2000-03-11' AS DATE)
      )
      THEN (
        a0.c1 - COALESCE(CAST(a5.c2 AS DECIMAL(13, 2)), 00000000000.00)
      )
      ELSE 000000000000.00
    END
  ) AS SALES_BEFORE,
  SUM(
    CASE
      WHEN (
        a0.c5 >= CAST('2000-03-11' AS DATE)
      )
      THEN (
        a0.c1 - COALESCE(CAST(a5.c2 AS DECIMAL(13, 2)), 00000000000.00)
      )
      ELSE 000000000000.00
    END
  ) AS SALES_AFTER
FROM (
  (
    SELECT
      a4.i_item_id AS c0,
      a1.cs_sales_price AS c1,
      a1.cs_item_sk AS c2,
      a1.cs_order_number AS c3,
      a3.w_state AS c4,
      a2.d_date AS c5
    FROM (
      (
        (
          catalog_sales AS a1
            INNER JOIN date_dim AS a2
              ON (
                (
                  a2.d_date_sk = a1.cs_sold_date_sk
                )
                AND (
                  (
                    a1.cs_sold_date_sk <= 2451645
                  ) AND (
                    a1.cs_sold_date_sk >= 2451585
                  )
                )
                AND (
                  (
                    a1.cs_item_sk <= 401987
                  ) AND (
                    a1.cs_item_sk >= 37
                  )
                )
                AND (
                  CAST('2000-02-10' AS DATE) <= a2.d_date
                )
                AND (
                  a2.d_date <= CAST('2000-04-10' AS DATE)
                )
              )
        )
        INNER JOIN warehouse AS a3
          ON (
            a3.w_warehouse_sk = a1.cs_warehouse_sk
          )
      )
      INNER JOIN item AS a4
        ON (
          (
            a1.cs_item_sk = a4.i_item_sk
          )
          AND (
            0.99 <= a4.i_current_price
          )
          AND (
            a4.i_current_price <= 1.49
          )
        )
    )
  ) AS a0
  LEFT OUTER JOIN (
    SELECT
      a6.cr_item_sk AS c0,
      a6.cr_order_number AS c1,
      a6.cr_refunded_cash AS c2
    FROM (
      catalog_returns AS a6
        INNER JOIN item AS a7
          ON (
            (
              a7.i_item_sk = a6.cr_item_sk
            )
            AND (
              (
                a6.cr_item_sk <= 401987
              ) AND (
                a6.cr_item_sk >= 37
              )
            )
            AND (
              a7.i_current_price <= 1.49
            )
            AND (
              0.99 <= a7.i_current_price
            )
          )
    )
  ) AS a5
    ON (
      a0.c3 = a5.c1
    ) AND (
      a0.c2 = a5.c0
    )
)
GROUP BY
  a0.c4,
  a0.c0
ORDER BY
  1 ASC NULLS LAST,
  2 ASC NULLS LAST
LIMIT 100