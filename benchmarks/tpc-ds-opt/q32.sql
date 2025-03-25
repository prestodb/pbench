SELECT
  SUM(a0.c0) AS excess_discount_amount
FROM (
  SELECT
    a1.cs_ext_discount_amt AS c0,
    COUNT(a1.cs_ext_discount_amt) OVER (PARTITION BY a1.cs_item_sk) AS c1,
    SUM(a1.cs_ext_discount_amt) OVER (PARTITION BY a1.cs_item_sk) AS c2
  FROM (
    (
      catalog_sales AS a1
        INNER JOIN item AS a2
          ON (
            (
              a1.cs_item_sk = a2.i_item_sk
            )
            AND (
              (
                a1.cs_item_sk <= 398799
              ) AND (
                a1.cs_item_sk >= 15489
              )
            )
            AND (
              (
                a1.cs_sold_date_sk <= 2451661
              ) AND (
                a1.cs_sold_date_sk >= 2451571
              )
            )
            AND (
              a2.i_manufact_id = 977
            )
          )
    )
    INNER JOIN date_dim AS a3
      ON (
        (
          a3.d_date_sk = a1.cs_sold_date_sk
        )
        AND (
          CAST('2000-01-27' AS DATE) <= a3.d_date
        )
        AND (
          a3.d_date <= CAST('2000-04-26' AS DATE)
        )
      )
  )
) AS a0
WHERE
  (
    (
      1.3 * CAST((
        a0.c2 / a0.c1
      ) AS DECIMAL(7, 2))
    ) < a0.c0
  )
LIMIT 100