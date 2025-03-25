SELECT
  a0.c0 AS I_PRODUCT_NAME,
  a0.c1 AS I_BRAND,
  a0.c2 AS I_CLASS,
  a0.c3 AS I_CATEGORY,
  (
    a0.c4 / CASE
      WHEN (
        NOT a0.c6 IS NULL
      )
      THEN a0.c5
      ELSE 0000000000000000000000000000000.
    END
  ) AS QOH
FROM (
  (
    SELECT
      a1.c0 AS c0,
      a1.c1 AS c1,
      a1.c2 AS c2,
      a1.c3 AS c3,
      SUM(a1.c5) AS c4,
      SUM(a1.c4) AS c5,
      a1.c6 AS c6
    FROM (
      SELECT
        CASE WHEN (
          a7.c0 < 5
        ) THEN a2.c0 ELSE NULL END AS c0,
        CASE WHEN (
          a7.c0 < 4
        ) THEN a2.c1 ELSE NULL END AS c1,
        CASE WHEN (
          a7.c0 < 3
        ) THEN a2.c2 ELSE NULL END AS c2,
        CASE WHEN (
          a7.c0 < 2
        ) THEN a2.c3 ELSE NULL END AS c3,
        a2.c4 AS c4,
        a2.c5 AS c5,
        a7.c0 AS c6
      FROM (
        (
          SELECT
            a6.i_product_name AS c0,
            a6.i_brand AS c1,
            a6.i_class AS c2,
            a6.i_category AS c3,
            SUM(a5.c1) AS c4,
            SUM(a5.c0) AS c5
          FROM (
            (
              SELECT
                SUM(CAST(a3.inv_quantity_on_hand AS DOUBLE)) AS c0,
                COUNT(CAST(a3.inv_quantity_on_hand AS DOUBLE)) AS c1,
                a3.inv_item_sk AS c2
              FROM (
                inventory AS a3
                  INNER JOIN date_dim AS a4
                    ON (
                      (
                        a3.inv_date_sk = a4.d_date_sk
                      )
                      AND (
                        (
                          a3.inv_date_sk <= 2451910
                        ) AND (
                          a3.inv_date_sk >= 2451545
                        )
                      )
                      AND (
                        1200 <= a4.d_month_seq
                      )
                      AND (
                        a4.d_month_seq <= 1211
                      )
                    )
              )
              GROUP BY
                a3.inv_item_sk
            ) AS a5
            INNER JOIN item AS a6
              ON (
                a5.c2 = a6.i_item_sk
              )
          )
          GROUP BY
            a6.i_product_name,
            a6.i_brand,
            a6.i_class,
            a6.i_category
        ) AS a2
        INNER JOIN VALUES
          (1),
          (2),
          (3),
          (4),
          (5) AS a7(c0)
          ON (
            1 = 1
          )
      )
    ) AS a1
    GROUP BY
      a1.c6,
      a1.c0,
      a1.c1,
      a1.c2,
      a1.c3
  ) AS a0
  RIGHT OUTER JOIN VALUES
    (1),
    (2),
    (3),
    (4),
    (5) AS a8(c0)
    ON (
      a8.c0 = a0.c6
    )
)
WHERE
  (
    (
      (
        a8.c0 = 5
      ) AND (
        a0.c6 IS NULL
      )
    ) OR (
      NOT a0.c6 IS NULL
    )
  )
ORDER BY
  5 ASC NULLS LAST,
  1 ASC NULLS LAST,
  2 ASC NULLS LAST,
  3 ASC NULLS LAST,
  4 ASC NULLS LAST
LIMIT 100