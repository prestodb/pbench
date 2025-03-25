WITH a0 AS (
  SELECT
    a1.ss_store_sk AS c0,
    SUM(a1.ss_sales_price) AS c1,
    a1.ss_item_sk AS c2
  FROM (
    store_sales AS a1
      INNER JOIN date_dim AS a2
        ON (
          (
            a1.ss_sold_date_sk = a2.d_date_sk
          )
          AND (
            (
              a1.ss_sold_date_sk <= 2451179
            ) AND (
              a1.ss_sold_date_sk >= 2450815
            )
          )
          AND (
            1176 <= a2.d_month_seq
          )
          AND (
            a2.d_month_seq <= 1187
          )
        )
  )
  GROUP BY
    a1.ss_store_sk,
    a1.ss_item_sk
)
SELECT
  a4.s_store_name AS S_STORE_NAME,
  a7.i_item_desc AS I_ITEM_DESC,
  A3.c1 AS REVENUE,
  a7.i_current_price AS I_CURRENT_PRICE,
  a7.i_wholesale_cost AS I_WHOLESALE_COST,
  a7.i_brand AS I_BRAND
FROM (
  (
    a0 AS A3
      INNER JOIN (
        store AS a4
          INNER JOIN (
            SELECT
              A6.c0 AS c0,
              SUM(A6.c1) AS c1,
              COUNT(A6.c1) AS c2
            FROM a0 AS A6
            GROUP BY
              A6.c0
          ) AS a5
            ON (
              a5.c0 = a4.s_store_sk
            )
      )
        ON (
          a4.s_store_sk = A3.c0
        )
        AND (
          A3.c1 <= (
            0.1 * CAST((
              a5.c1 / a5.c2
            ) AS DECIMAL(31, 2))
          )
        )
  )
  INNER JOIN item AS a7
    ON (
      a7.i_item_sk = A3.c2
    )
)
ORDER BY
  1 ASC NULLS LAST,
  2 ASC NULLS LAST
LIMIT 100