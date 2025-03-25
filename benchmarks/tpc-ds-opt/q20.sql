SELECT
  a0.c0 AS I_ITEM_ID,
  a0.c1 AS I_ITEM_DESC,
  a0.c2 AS I_CATEGORY,
  a0.c3 AS I_CLASS,
  a0.c4 AS I_CURRENT_PRICE,
  a0.c5 AS ITEMREVENUE,
  (
    (
      a0.c5 * 100
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
    SUM(a1.c5) OVER (PARTITION BY a1.c3) AS c6
  FROM (
    SELECT
      a4.i_item_id AS c0,
      a4.i_item_desc AS c1,
      a4.i_category AS c2,
      a4.i_class AS c3,
      a4.i_current_price AS c4,
      SUM(a2.cs_ext_sales_price) AS c5
    FROM (
      (
        catalog_sales AS a2
          INNER JOIN date_dim AS a3
            ON (
              (
                a2.cs_sold_date_sk = a3.d_date_sk
              )
              AND (
                (
                  a2.cs_sold_date_sk <= 2451262
                ) AND (
                  a2.cs_sold_date_sk >= 2451232
                )
              )
              AND (
                (
                  a2.cs_item_sk <= 402000
                ) AND (
                  a2.cs_item_sk >= 1
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
            a2.cs_item_sk = a4.i_item_sk
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
  3 ASC NULLS LAST,
  4 ASC NULLS LAST,
  1 ASC NULLS LAST,
  2 ASC NULLS LAST,
  7 ASC NULLS LAST
LIMIT 100