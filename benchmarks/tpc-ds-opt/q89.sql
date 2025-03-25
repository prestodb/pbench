SELECT
  a0.c0 AS I_CATEGORY,
  a0.c1 AS I_CLASS,
  a0.c2 AS I_BRAND,
  a0.c3 AS S_STORE_NAME,
  a0.c4 AS S_COMPANY_NAME,
  a0.c5 AS D_MOY,
  a0.c6 AS SUM_SALES,
  CAST((
    a0.c7 / a0.c8
  ) AS DECIMAL(31, 2)) AS AVG_MONTHLY_SALES
FROM (
  SELECT
    a1.c0 AS c0,
    a1.c1 AS c1,
    a1.c2 AS c2,
    a1.c3 AS c3,
    a1.c4 AS c4,
    a1.c5 AS c5,
    a1.c6 AS c6,
    SUM(a1.c6) OVER (PARTITION BY a1.c0, a1.c2, a1.c3, a1.c4) AS c7,
    COUNT(a1.c6) OVER (PARTITION BY a1.c0, a1.c2, a1.c3, a1.c4) AS c8
  FROM (
    SELECT
      a3.i_category AS c0,
      a3.i_class AS c1,
      a3.i_brand AS c2,
      a5.s_store_name AS c3,
      a5.s_company_name AS c4,
      a4.d_moy AS c5,
      SUM(a2.ss_sales_price) AS c6
    FROM (
      (
        (
          store_sales AS a2
            INNER JOIN item AS a3
              ON (
                (
                  a2.ss_item_sk = a3.i_item_sk
                )
                AND (
                  (
                    a2.ss_sold_date_sk <= 2451544
                  ) AND (
                    a2.ss_sold_date_sk >= 2451180
                  )
                )
                AND (
                  (
                    a2.ss_item_sk <= 401996
                  ) AND (
                    a2.ss_item_sk >= 10
                  )
                )
                AND (
                  (
                    (
                      a3.i_category IN ('Books', 'Electronics', 'Sports')
                    )
                    AND (
                      a3.i_class IN ('computers', 'stereo', 'football')
                    )
                  )
                  OR (
                    (
                      a3.i_category IN ('Men', 'Jewelry', 'Women')
                    )
                    AND (
                      a3.i_class IN ('shirts', 'birdal', 'dresses')
                    )
                  )
                )
              )
        )
        INNER JOIN date_dim AS a4
          ON (
            (
              a2.ss_sold_date_sk = a4.d_date_sk
            ) AND (
              a4.d_year = 1999
            )
          )
      )
      INNER JOIN store AS a5
        ON (
          a2.ss_store_sk = a5.s_store_sk
        )
    )
    GROUP BY
      a3.i_category,
      a3.i_class,
      a3.i_brand,
      a5.s_store_name,
      a5.s_company_name,
      a4.d_moy
  ) AS a1
) AS a0
WHERE
  (
    0.1 < CASE
      WHEN (
        CAST((
          a0.c7 / a0.c8
        ) AS DECIMAL(31, 2)) <> 00000000000000000000000000000.00
      )
      THEN (
        ABS((
          a0.c6 - CAST((
            a0.c7 / a0.c8
          ) AS DECIMAL(31, 2))
        )) / CAST((
          a0.c7 / a0.c8
        ) AS DECIMAL(31, 2))
      )
      ELSE NULL
    END
  )
ORDER BY
  (
    a0.c6 - CAST((
      a0.c7 / a0.c8
    ) AS DECIMAL(31, 2))
  ) ASC NULLS LAST,
  4 ASC NULLS LAST
LIMIT 100