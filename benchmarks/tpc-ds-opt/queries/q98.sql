SELECT
  a0.c0 AS I_ITEM_DESC,
  a0.c1 AS I_CATEGORY,
  a0.c2 AS I_CLASS,
  a0.c3 AS I_CURRENT_PRICE,
  a0.c4 AS ITEMREVENUE,
  (
    (
      a0.c4 * 100
    ) / a0.c6
  ) AS REVENUERATIO
FROM (
  SELECT
    a1.c0 AS c0,
    a1.c1 AS c1,
    a1.c2 AS c2,
    a1.c3 AS c3,
    a1.c4 AS c4,
    a1.c5 AS c5,
    SUM(a1.c4) OVER (PARTITION BY a1.c2) AS c6
  FROM (
    SELECT
      a4.i_item_desc AS c0,
      a4.i_category AS c1,
      a4.i_class AS c2,
      a4.i_current_price AS c3,
      SUM(a2.ss_ext_sales_price) AS c4,
      a4.i_item_id AS c5
    FROM (
      (
        store_sales AS a2
          INNER JOIN date_dim AS a3
            ON (
              (
                a2.ss_sold_date_sk = a3.d_date_sk
              )
              AND (
                (
                  a2.ss_sold_date_sk <= 2451262
                ) AND (
                  a2.ss_sold_date_sk >= 2451232
                )
              )
              AND (
                (
                  a2.ss_item_sk <= 402000
                ) AND (
                  a2.ss_item_sk >= 1
                )
              )
              AND (
                CAST('1999-02-22' AS DATE) <= a3.d_date
              )
              AND (
                a3.d_date <= CAST('1999-03-24' AS DATE)
              )
            )
      )
      INNER JOIN item AS a4
        ON (
          (
            a2.ss_item_sk = a4.i_item_sk
          )
          AND (
            a4.i_category IN ('Sports', 'Books', 'Home')
          )
        )
    )
    GROUP BY
      a4.i_item_id,
      a4.i_item_desc,
      a4.i_category,
      a4.i_class,
      a4.i_current_price
  ) AS a1
) AS a0
ORDER BY
  2 ASC NULLS LAST,
  3 ASC NULLS LAST,
  a0.c5 ASC NULLS LAST,
  1 ASC NULLS LAST,
  6 ASC NULLS LAST