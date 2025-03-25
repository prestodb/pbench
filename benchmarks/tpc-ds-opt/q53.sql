SELECT
  a0.c0 AS I_MANUFACT_ID,
  a0.c1 AS SUM_SALES,
  CAST((
    a0.c2 / a0.c3
  ) AS DECIMAL(31, 2)) AS AVG_QUARTERLY_SALES
FROM (
  SELECT
    a1.c0 AS c0,
    a1.c1 AS c1,
    SUM(a1.c1) OVER (PARTITION BY a1.c0) AS c2,
    COUNT(a1.c1) OVER (PARTITION BY a1.c0) AS c3
  FROM (
    SELECT
      a3.i_manufact_id AS c0,
      SUM(a2.ss_sales_price) AS c1
    FROM (
      (
        store_sales AS a2
          INNER JOIN item AS a3
            ON (
              (
                a2.ss_item_sk = a3.i_item_sk
              )
              AND (
                NOT a2.ss_store_sk IS NULL
              )
              AND (
                (
                  a2.ss_sold_date_sk <= 2451910
                ) AND (
                  a2.ss_sold_date_sk >= 2451545
                )
              )
              AND (
                (
                  a2.ss_item_sk <= 401988
                ) AND (
                  a2.ss_item_sk >= 4
                )
              )
              AND (
                (
                  (
                    (
                      a3.i_category IN ('Books', 'Children', 'Electronics')
                    )
                    AND (
                      a3.i_class IN ('personal', 'portable', 'reference', 'self-help')
                    )
                  )
                  AND (
                    a3.i_brand IN ('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9')
                  )
                )
                OR (
                  (
                    (
                      a3.i_category IN ('Women', 'Music', 'Men')
                    )
                    AND (
                      a3.i_class IN ('accessories', 'classical', 'fragrances', 'pants')
                    )
                  )
                  AND (
                    a3.i_brand IN ('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1')
                  )
                )
              )
            )
      )
      INNER JOIN date_dim AS a4
        ON (
          (
            a2.ss_sold_date_sk = a4.d_date_sk
          )
          AND (
            a4.d_month_seq IN (1200, 1201, 1202, 1203, 1204, 1205, 1206, 1207, 1208, 1209, 1210, 1211)
          )
        )
    )
    GROUP BY
      a3.i_manufact_id,
      a4.d_qoy
  ) AS a1
) AS a0
WHERE
  (
    0.1 < CASE
      WHEN (
        CAST((
          a0.c2 / a0.c3
        ) AS DECIMAL(31, 2)) > 0
      )
      THEN (
        ABS((
          a0.c1 - CAST((
            a0.c2 / a0.c3
          ) AS DECIMAL(31, 2))
        )) / CAST((
          a0.c2 / a0.c3
        ) AS DECIMAL(31, 2))
      )
      ELSE NULL
    END
  )
ORDER BY
  3 ASC NULLS LAST,
  2 ASC NULLS LAST,
  1 ASC NULLS LAST
LIMIT 100