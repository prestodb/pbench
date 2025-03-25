WITH a2 AS (
  SELECT
    a3.c0 AS c0,
    CAST((
      a3.c1 / a3.c2
    ) AS DECIMAL(7, 2)) AS c1
  FROM (
    SELECT
      a4.ss_item_sk AS c0,
      SUM(a4.ss_net_profit) AS c1,
      COUNT(a4.ss_net_profit) AS c2
    FROM store_sales AS a4
    WHERE
      (
        a4.ss_store_sk = 4
      )
    GROUP BY
      a4.ss_item_sk
  ) AS a3
  WHERE
    (
      (
        0.9 * (
          SELECT
            CAST((
              a5.c0 / a5.c1
            ) AS DECIMAL(7, 2))
          FROM (
            SELECT
              SUM(a6.ss_net_profit) AS c0,
              COUNT(a6.ss_net_profit) AS c1
            FROM store_sales AS a6
            WHERE
              (
                a6.ss_store_sk = 4
              ) AND (
                a6.ss_addr_sk IS NULL
              )
            GROUP BY
              a6.ss_store_sk
          ) AS a5
        )
      ) < CAST((
        a3.c1 / a3.c2
      ) AS DECIMAL(7, 2))
    )
)
SELECT
  a1.c1 AS RNK,
  a0.i_product_name AS BEST_PERFORMING,
  a8.i_product_name AS WORST_PERFORMING
FROM (
  (
    item AS a0
      INNER JOIN (
        SELECT
          A7.c0 AS c0,
          RANK() OVER (ORDER BY A7.c1 ASC NULLS LAST) AS c1
        FROM a2 AS A7
      ) AS a1
        ON (
          (
            a0.i_item_sk = a1.c0
          ) AND (
            a1.c1 < 11
          )
        )
  )
  INNER JOIN (
    item AS a8
      INNER JOIN (
        SELECT
          A10.c0 AS c0,
          RANK() OVER (ORDER BY A10.c1 DESC) AS c1
        FROM a2 AS A10
      ) AS a9
        ON (
          (
            a8.i_item_sk = a9.c0
          ) AND (
            a9.c1 < 11
          )
        )
  )
    ON (
      a1.c1 = a9.c1
    )
)
ORDER BY
  1 ASC NULLS LAST
LIMIT 100