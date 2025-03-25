WITH a0 AS (
  SELECT
    a1.inv_warehouse_sk AS c0,
    a1.inv_item_sk AS c1,
    a2.d_moy AS c2,
    STDDEV_SAMP(a1.inv_quantity_on_hand) AS c3,
    SUM(CAST(a1.inv_quantity_on_hand AS DOUBLE)) AS c4,
    COUNT(CAST(a1.inv_quantity_on_hand AS DOUBLE)) AS c5
  FROM (
    inventory AS a1
      INNER JOIN date_dim AS a2
        ON (
          (
            a1.inv_date_sk = a2.d_date_sk
          )
          AND (
            (
              a1.inv_date_sk <= 2451969
            ) AND (
              a1.inv_date_sk >= 2451911
            )
          )
          AND (
            a2.d_moy IN (2, 1)
          )
          AND (
            a2.d_year = 2001
          )
        )
  )
  GROUP BY
    a1.inv_warehouse_sk,
    a1.inv_item_sk,
    a2.d_moy
)
SELECT
  A4.c0 AS W_WAREHOUSE_SK,
  A4.c1 AS I_ITEM_SK,
  A4.c2 AS D_MOY,
  (
    A4.c4 / A4.c5
  ) AS MEAN,
  CASE
    WHEN (
      (
        A4.c4 / A4.c5
      ) = 0.0000000000000000E+000
    )
    THEN NULL
    ELSE (
      A4.c3 / (
        A4.c4 / A4.c5
      )
    )
  END AS COV,
  A3.c0 AS W_WAREHOUSE_SK,
  A3.c1 AS I_ITEM_SK,
  A3.c2 AS D_MOY,
  (
    A3.c4 / A3.c5
  ) AS MEAN,
  CASE
    WHEN (
      (
        A3.c4 / A3.c5
      ) = 0.0000000000000000E+000
    )
    THEN NULL
    ELSE (
      A3.c3 / (
        A3.c4 / A3.c5
      )
    )
  END AS COV
FROM (
  a0 AS A3
    INNER JOIN a0 AS A4
      ON (
        (
          A4.c1 = A3.c1
        )
        AND (
          A4.c0 = A3.c0
        )
        AND (
          1.0000000000000000E+000 < CASE
            WHEN (
              (
                A3.c4 / A3.c5
              ) = 0.0000000000000000E+000
            )
            THEN 0.0000000000000000E+000
            ELSE (
              A3.c3 / (
                A3.c4 / A3.c5
              )
            )
          END
        )
        AND (
          A3.c2 = 2
        )
        AND (
          1.5000000000000000E+000 < CASE
            WHEN (
              (
                A4.c4 / A4.c5
              ) = 0.0000000000000000E+000
            )
            THEN NULL
            ELSE (
              A4.c3 / (
                A4.c4 / A4.c5
              )
            )
          END
        )
        AND (
          1.0000000000000000E+000 < CASE
            WHEN (
              (
                A4.c4 / A4.c5
              ) = 0.0000000000000000E+000
            )
            THEN 0.0000000000000000E+000
            ELSE (
              A4.c3 / (
                A4.c4 / A4.c5
              )
            )
          END
        )
        AND (
          A4.c2 = 1
        )
      )
)
ORDER BY
  1 ASC NULLS LAST,
  2 ASC NULLS LAST