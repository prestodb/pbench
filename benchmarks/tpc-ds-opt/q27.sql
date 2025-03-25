SELECT
  a0.c0 AS I_ITEM_ID,
  a0.c1 AS S_STATE,
  CASE
    WHEN (
      NOT a0.c10 IS NULL
    )
    THEN CASE
      WHEN (
        a0.c10 IS NULL
      )
      THEN NULL
      ELSE CASE WHEN (
        a0.c10 < 2
      ) THEN 0 ELSE 1 END
    END
    ELSE 1
  END AS G_STATE,
  (
    a0.c8 / CASE
      WHEN (
        NOT a0.c10 IS NULL
      )
      THEN a0.c9
      ELSE 0000000000000000000000000000000.
    END
  ) AS AGG1,
  CAST((
    a0.c6 / CASE
      WHEN (
        NOT a0.c10 IS NULL
      )
      THEN a0.c7
      ELSE 0000000000000000000000000000000.
    END
  ) AS DECIMAL(7, 2)) AS AGG2,
  CAST((
    a0.c4 / CASE
      WHEN (
        NOT a0.c10 IS NULL
      )
      THEN a0.c5
      ELSE 0000000000000000000000000000000.
    END
  ) AS DECIMAL(7, 2)) AS AGG3,
  CAST((
    a0.c2 / CASE
      WHEN (
        NOT a0.c10 IS NULL
      )
      THEN a0.c3
      ELSE 0000000000000000000000000000000.
    END
  ) AS DECIMAL(7, 2)) AS AGG4
FROM (
  (
    SELECT
      a1.c0 AS c0,
      a1.c1 AS c1,
      SUM(a1.c9) AS c2,
      SUM(a1.c8) AS c3,
      SUM(a1.c7) AS c4,
      SUM(a1.c6) AS c5,
      SUM(a1.c5) AS c6,
      SUM(a1.c4) AS c7,
      SUM(a1.c3) AS c8,
      SUM(a1.c2) AS c9,
      a1.c10 AS c10
    FROM (
      SELECT
        CASE WHEN (
          a9.c0 < 3
        ) THEN a2.c0 ELSE NULL END AS c0,
        CASE WHEN (
          a9.c0 < 2
        ) THEN a2.c1 ELSE NULL END AS c1,
        a2.c2 AS c2,
        a2.c3 AS c3,
        a2.c4 AS c4,
        a2.c5 AS c5,
        a2.c6 AS c6,
        a2.c7 AS c7,
        a2.c8 AS c8,
        a2.c9 AS c9,
        a9.c0 AS c10
      FROM (
        (
          SELECT
            a8.i_item_id AS c0,
            a7.c9 AS c1,
            SUM(a7.c7) AS c2,
            SUM(a7.c6) AS c3,
            SUM(a7.c5) AS c4,
            SUM(a7.c4) AS c5,
            SUM(a7.c3) AS c6,
            SUM(a7.c2) AS c7,
            SUM(a7.c1) AS c8,
            SUM(a7.c0) AS c9
          FROM (
            (
              SELECT
                SUM(a3.ss_sales_price) AS c0,
                COUNT(a3.ss_sales_price) AS c1,
                SUM(a3.ss_coupon_amt) AS c2,
                COUNT(a3.ss_coupon_amt) AS c3,
                SUM(a3.ss_list_price) AS c4,
                COUNT(a3.ss_list_price) AS c5,
                SUM(CAST(a3.ss_quantity AS DOUBLE)) AS c6,
                COUNT(CAST(a3.ss_quantity AS DOUBLE)) AS c7,
                a3.ss_item_sk AS c8,
                a5.s_state AS c9
              FROM (
                (
                  (
                    store_sales AS a3
                      INNER JOIN customer_demographics AS a4
                        ON (
                          (
                            a3.ss_cdemo_sk = a4.cd_demo_sk
                          )
                          AND (
                            (
                              a3.ss_store_sk <= 1472
                            ) AND (
                              a3.ss_store_sk >= 29
                            )
                          )
                          AND (
                            (
                              a3.ss_sold_date_sk <= 2452640
                            ) AND (
                              a3.ss_sold_date_sk >= 2452276
                            )
                          )
                          AND (
                            (
                              a3.ss_cdemo_sk <= 1920753
                            ) AND (
                              a3.ss_cdemo_sk >= 23
                            )
                          )
                          AND (
                            a4.cd_gender = 'M'
                          )
                          AND (
                            a4.cd_marital_status = 'S'
                          )
                          AND (
                            a4.cd_education_status = 'College'
                          )
                        )
                  )
                  INNER JOIN store AS a5
                    ON (
                      (
                        a3.ss_store_sk = a5.s_store_sk
                      ) AND (
                        a5.s_state = 'TN'
                      )
                    )
                )
                INNER JOIN date_dim AS a6
                  ON (
                    (
                      a3.ss_sold_date_sk = a6.d_date_sk
                    ) AND (
                      a6.d_year = 2002
                    )
                  )
              )
              GROUP BY
                a3.ss_item_sk,
                a5.s_state
            ) AS a7
            INNER JOIN item AS a8
              ON (
                a7.c8 = a8.i_item_sk
              )
          )
          GROUP BY
            a8.i_item_id,
            a7.c9
        ) AS a2
        INNER JOIN VALUES
          (1),
          (2),
          (3) AS a9(c0)
          ON (
            1 = 1
          )
      )
    ) AS a1
    GROUP BY
      a1.c10,
      a1.c0,
      a1.c1
  ) AS a0
  RIGHT OUTER JOIN VALUES
    (1),
    (2),
    (3) AS a10(c0)
    ON (
      a10.c0 = a0.c10
    )
)
WHERE
  (
    (
      (
        a10.c0 = 3
      ) AND (
        a0.c10 IS NULL
      )
    )
    OR (
      NOT a0.c10 IS NULL
    )
  )
ORDER BY
  1 ASC NULLS LAST,
  2 ASC NULLS LAST
LIMIT 100