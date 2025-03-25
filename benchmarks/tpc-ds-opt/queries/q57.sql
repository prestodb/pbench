WITH a0 AS (
  SELECT
    a1.c0 AS c0,
    a1.c1 AS c1,
    a1.c2 AS c2,
    a1.c3 AS c3,
    a1.c4 AS c4,
    a1.c5 AS c5,
    RANK() OVER (PARTITION BY a1.c0, a1.c1, a1.c2 ORDER BY a1.c3 ASC NULLS LAST, a1.c4 ASC NULLS LAST) AS c6,
    SUM(a1.c5) OVER (PARTITION BY a1.c0, a1.c1, a1.c2, a1.c3) AS c7,
    COUNT(a1.c5) OVER (PARTITION BY a1.c0, a1.c1, a1.c2, a1.c3) AS c8
  FROM (
    SELECT
      a5.i_category AS c0,
      a5.i_brand AS c1,
      a4.cc_name AS c2,
      a3.d_year AS c3,
      a3.d_moy AS c4,
      SUM(a2.cs_sales_price) AS c5
    FROM (
      (
        (
          catalog_sales AS a2
            INNER JOIN date_dim AS a3
              ON (
                (
                  a2.cs_sold_date_sk = a3.d_date_sk
                )
                AND (
                  (
                    a2.cs_sold_date_sk <= 2451575
                  ) AND (
                    a2.cs_sold_date_sk >= 2451149
                  )
                )
                AND (
                  (
                    (
                      a3.d_year = 1999
                    )
                    OR (
                      (
                        a3.d_year = 1998
                      ) AND (
                        a3.d_moy = 12
                      )
                    )
                  )
                  OR (
                    (
                      a3.d_year = 2000
                    ) AND (
                      a3.d_moy = 1
                    )
                  )
                )
              )
        )
        INNER JOIN call_center AS a4
          ON (
            a4.cc_call_center_sk = a2.cs_call_center_sk
          )
      )
      INNER JOIN item AS a5
        ON (
          a2.cs_item_sk = a5.i_item_sk
        )
    )
    GROUP BY
      a5.i_category,
      a5.i_brand,
      a4.cc_name,
      a3.d_year,
      a3.d_moy
  ) AS a1
)
SELECT
  A8.c0 AS I_CATEGORY,
  A8.c1 AS I_BRAND,
  A8.c2 AS CC_NAME,
  A8.c3 AS D_YEAR,
  A8.c4 AS D_MOY,
  CAST((
    A8.c7 / A8.c8
  ) AS DECIMAL(31, 2)) AS AVG_MONTHLY_SALES,
  A8.c5 AS SUM_SALES,
  A7.c5 AS PSUM,
  A6.c5 AS NSUM
FROM (
  a0 AS A6
    INNER JOIN (
      a0 AS A7
        INNER JOIN a0 AS A8
          ON (
            (
              A8.c6 = (
                A7.c6 + 1
              )
            )
            AND (
              A8.c2 = A7.c2
            )
            AND (
              A8.c1 = A7.c1
            )
            AND (
              A8.c0 = A7.c0
            )
            AND (
              0.1 < CASE
                WHEN (
                  CAST((
                    A8.c7 / A8.c8
                  ) AS DECIMAL(31, 2)) > 0
                )
                THEN (
                  ABS((
                    A8.c5 - CAST((
                      A8.c7 / A8.c8
                    ) AS DECIMAL(31, 2))
                  )) / CAST((
                    A8.c7 / A8.c8
                  ) AS DECIMAL(31, 2))
                )
                ELSE NULL
              END
            )
            AND (
              0 < CAST((
                A8.c7 / A8.c8
              ) AS DECIMAL(31, 2))
            )
            AND (
              A8.c3 = 1999
            )
          )
    )
      ON (
        A8.c6 = (
          A6.c6 - 1
        )
      )
      AND (
        A6.c0 = A8.c0
      )
      AND (
        A6.c1 = A8.c1
      )
      AND (
        A6.c2 = A8.c2
      )
)
ORDER BY
  (
    A8.c5 - CAST((
      A8.c7 / A8.c8
    ) AS DECIMAL(31, 2))
  ) ASC NULLS LAST,
  3 ASC NULLS LAST
LIMIT 100