SELECT
  a0.c0 AS I_CATEGORY,
  a0.c1 AS I_CLASS,
  a0.c2 AS I_BRAND,
  a0.c3 AS I_PRODUCT_NAME,
  a0.c4 AS D_YEAR,
  a0.c5 AS D_QOY,
  a0.c6 AS D_MOY,
  a0.c7 AS S_STORE_ID,
  a0.c8 AS SUMSALES,
  a0.c9 AS RK
FROM (
  SELECT
    a1.c0 AS c0,
    a1.c1 AS c1,
    a1.c2 AS c2,
    a1.c3 AS c3,
    a1.c4 AS c4,
    a1.c5 AS c5,
    a1.c6 AS c6,
    a1.c7 AS c7,
    a1.c8 AS c8,
    RANK() OVER (PARTITION BY a1.c0 ORDER BY a1.c8 DESC) AS c9
  FROM (
    (
      SELECT
        a2.c0 AS c0,
        a2.c1 AS c1,
        a2.c2 AS c2,
        a2.c3 AS c3,
        a2.c4 AS c4,
        a2.c5 AS c5,
        a2.c6 AS c6,
        a2.c7 AS c7,
        SUM(a2.c8) AS c8,
        a2.c9 AS c9
      FROM (
        SELECT
          CASE WHEN (
            a9.c0 < 9
          ) THEN a3.c0 ELSE NULL END AS c0,
          CASE WHEN (
            a9.c0 < 8
          ) THEN a3.c1 ELSE NULL END AS c1,
          CASE WHEN (
            a9.c0 < 7
          ) THEN a3.c2 ELSE NULL END AS c2,
          CASE WHEN (
            a9.c0 < 6
          ) THEN a3.c3 ELSE NULL END AS c3,
          CASE WHEN (
            a9.c0 < 5
          ) THEN a3.c4 ELSE NULL END AS c4,
          CASE WHEN (
            a9.c0 < 4
          ) THEN a3.c5 ELSE NULL END AS c5,
          CASE WHEN (
            a9.c0 < 3
          ) THEN a3.c6 ELSE NULL END AS c6,
          CASE WHEN (
            a9.c0 < 2
          ) THEN a3.c7 ELSE NULL END AS c7,
          a3.c8 AS c8,
          a9.c0 AS c9
        FROM (
          (
            SELECT
              a4.c0 AS c0,
              a4.c1 AS c1,
              a4.c2 AS c2,
              a4.c3 AS c3,
              a4.c4 AS c4,
              a4.c5 AS c5,
              a4.c6 AS c6,
              a4.c7 AS c7,
              a4.c8 AS c8,
              RANK() OVER (PARTITION BY a4.c0, a4.c1, a4.c2, a4.c3 ORDER BY a4.c8 DESC) AS c9
            FROM (
              SELECT
                a8.i_category AS c0,
                a8.i_class AS c1,
                a8.i_brand AS c2,
                a8.i_product_name AS c3,
                a6.d_year AS c4,
                a6.d_qoy AS c5,
                a6.d_moy AS c6,
                a7.s_store_id AS c7,
                SUM(COALESCE((
                  a5.ss_sales_price * a5.ss_quantity
                ), 0000000000000000.00)) AS c8
              FROM (
                (
                  (
                    store_sales AS a5
                      INNER JOIN date_dim AS a6
                        ON (
                          (
                            a5.ss_sold_date_sk = a6.d_date_sk
                          )
                          AND (
                            (
                              a5.ss_sold_date_sk <= 2451910
                            ) AND (
                              a5.ss_sold_date_sk >= 2451545
                            )
                          )
                          AND (
                            1200 <= a6.d_month_seq
                          )
                          AND (
                            a6.d_month_seq <= 1211
                          )
                        )
                  )
                  INNER JOIN store AS a7
                    ON (
                      a5.ss_store_sk = a7.s_store_sk
                    )
                )
                INNER JOIN item AS a8
                  ON (
                    a5.ss_item_sk = a8.i_item_sk
                  )
              )
              GROUP BY
                a8.i_category,
                a8.i_class,
                a8.i_brand,
                a8.i_product_name,
                a6.d_year,
                a6.d_qoy,
                a6.d_moy,
                a7.s_store_id
            ) AS a4
          ) AS a3
          INNER JOIN VALUES
            (1),
            (2),
            (3),
            (4),
            (5),
            (6),
            (7),
            (8),
            (9) AS a9(c0)
            ON (
              (
                CASE
                  WHEN (
                    (
                      a9.c0 > 1
                    ) OR (
                      (
                        a9.c0 = 1
                      ) AND (
                        a3.c9 <= 100
                      )
                    )
                  )
                  THEN 1
                  ELSE 0
                END = 1
              )
            )
        )
      ) AS a2
      GROUP BY
        a2.c9,
        a2.c0,
        a2.c1,
        a2.c2,
        a2.c3,
        a2.c4,
        a2.c5,
        a2.c6,
        a2.c7
    ) AS a1
    RIGHT OUTER JOIN VALUES
      (1),
      (2),
      (3),
      (4),
      (5),
      (6),
      (7),
      (8),
      (9) AS a10(c0)
      ON (
        a10.c0 = a1.c9
      )
  )
  WHERE
    (
      (
        (
          a10.c0 = 9
        ) AND (
          a1.c9 IS NULL
        )
      ) OR (
        NOT a1.c9 IS NULL
      )
    )
) AS a0
WHERE
  (
    a0.c9 <= 100
  )
ORDER BY
  1 ASC NULLS LAST,
  2 ASC NULLS LAST,
  3 ASC NULLS LAST,
  4 ASC NULLS LAST,
  5 ASC NULLS LAST,
  6 ASC NULLS LAST,
  7 ASC NULLS LAST,
  8 ASC NULLS LAST,
  9 ASC NULLS LAST,
  10 ASC NULLS LAST
LIMIT 100