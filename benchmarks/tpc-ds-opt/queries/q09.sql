SELECT
  CASE
    WHEN (
      a0.c20 > 74129
    )
    THEN CAST((
      a0.c21 / a0.c22
    ) AS DECIMAL(7, 2))
    ELSE CAST((
      a0.c23 / a0.c24
    ) AS DECIMAL(7, 2))
  END AS BUCKET1,
  CASE
    WHEN (
      a0.c15 > 122840
    )
    THEN CAST((
      a0.c16 / a0.c17
    ) AS DECIMAL(7, 2))
    ELSE CAST((
      a0.c18 / a0.c19
    ) AS DECIMAL(7, 2))
  END AS BUCKET2,
  CASE
    WHEN (
      a0.c10 > 56580
    )
    THEN CAST((
      a0.c11 / a0.c12
    ) AS DECIMAL(7, 2))
    ELSE CAST((
      a0.c13 / a0.c14
    ) AS DECIMAL(7, 2))
  END AS BUCKET3,
  CASE
    WHEN (
      a0.c5 > 10097
    )
    THEN CAST((
      a0.c6 / a0.c7
    ) AS DECIMAL(7, 2))
    ELSE CAST((
      a0.c8 / a0.c9
    ) AS DECIMAL(7, 2))
  END AS BUCKET4,
  CASE
    WHEN (
      a0.c0 > 165306
    )
    THEN CAST((
      a0.c1 / a0.c2
    ) AS DECIMAL(7, 2))
    ELSE CAST((
      a0.c3 / a0.c4
    ) AS DECIMAL(7, 2))
  END AS BUCKET5
FROM (
  (
    SELECT
      COUNT(
        CASE
          WHEN (
            (
              81 <= a1.ss_quantity
            ) AND (
              a1.ss_quantity <= 100
            )
          )
          THEN 1
          ELSE NULL
        END
      ) AS c0,
      SUM(
        (
          a1.ss_ext_discount_amt * CASE
            WHEN (
              (
                81 <= a1.ss_quantity
              ) AND (
                a1.ss_quantity <= 100
              )
            )
            THEN 1
            ELSE NULL
          END
        )
      ) AS c1,
      COUNT(
        (
          a1.ss_ext_discount_amt * CASE
            WHEN (
              (
                81 <= a1.ss_quantity
              ) AND (
                a1.ss_quantity <= 100
              )
            )
            THEN 1
            ELSE NULL
          END
        )
      ) AS c2,
      SUM(
        (
          a1.ss_net_paid * CASE
            WHEN (
              (
                81 <= a1.ss_quantity
              ) AND (
                a1.ss_quantity <= 100
              )
            )
            THEN 1
            ELSE NULL
          END
        )
      ) AS c3,
      COUNT(
        (
          a1.ss_net_paid * CASE
            WHEN (
              (
                81 <= a1.ss_quantity
              ) AND (
                a1.ss_quantity <= 100
              )
            )
            THEN 1
            ELSE NULL
          END
        )
      ) AS c4,
      COUNT(
        CASE
          WHEN (
            (
              61 <= a1.ss_quantity
            ) AND (
              a1.ss_quantity <= 80
            )
          )
          THEN 1
          ELSE NULL
        END
      ) AS c5,
      SUM(
        (
          a1.ss_ext_discount_amt * CASE
            WHEN (
              (
                61 <= a1.ss_quantity
              ) AND (
                a1.ss_quantity <= 80
              )
            )
            THEN 1
            ELSE NULL
          END
        )
      ) AS c6,
      COUNT(
        (
          a1.ss_ext_discount_amt * CASE
            WHEN (
              (
                61 <= a1.ss_quantity
              ) AND (
                a1.ss_quantity <= 80
              )
            )
            THEN 1
            ELSE NULL
          END
        )
      ) AS c7,
      SUM(
        (
          a1.ss_net_paid * CASE
            WHEN (
              (
                61 <= a1.ss_quantity
              ) AND (
                a1.ss_quantity <= 80
              )
            )
            THEN 1
            ELSE NULL
          END
        )
      ) AS c8,
      COUNT(
        (
          a1.ss_net_paid * CASE
            WHEN (
              (
                61 <= a1.ss_quantity
              ) AND (
                a1.ss_quantity <= 80
              )
            )
            THEN 1
            ELSE NULL
          END
        )
      ) AS c9,
      COUNT(
        CASE
          WHEN (
            (
              41 <= a1.ss_quantity
            ) AND (
              a1.ss_quantity <= 60
            )
          )
          THEN 1
          ELSE NULL
        END
      ) AS c10,
      SUM(
        (
          a1.ss_ext_discount_amt * CASE
            WHEN (
              (
                41 <= a1.ss_quantity
              ) AND (
                a1.ss_quantity <= 60
              )
            )
            THEN 1
            ELSE NULL
          END
        )
      ) AS c11,
      COUNT(
        (
          a1.ss_ext_discount_amt * CASE
            WHEN (
              (
                41 <= a1.ss_quantity
              ) AND (
                a1.ss_quantity <= 60
              )
            )
            THEN 1
            ELSE NULL
          END
        )
      ) AS c12,
      SUM(
        (
          a1.ss_net_paid * CASE
            WHEN (
              (
                41 <= a1.ss_quantity
              ) AND (
                a1.ss_quantity <= 60
              )
            )
            THEN 1
            ELSE NULL
          END
        )
      ) AS c13,
      COUNT(
        (
          a1.ss_net_paid * CASE
            WHEN (
              (
                41 <= a1.ss_quantity
              ) AND (
                a1.ss_quantity <= 60
              )
            )
            THEN 1
            ELSE NULL
          END
        )
      ) AS c14,
      COUNT(
        CASE
          WHEN (
            (
              21 <= a1.ss_quantity
            ) AND (
              a1.ss_quantity <= 40
            )
          )
          THEN 1
          ELSE NULL
        END
      ) AS c15,
      SUM(
        (
          a1.ss_ext_discount_amt * CASE
            WHEN (
              (
                21 <= a1.ss_quantity
              ) AND (
                a1.ss_quantity <= 40
              )
            )
            THEN 1
            ELSE NULL
          END
        )
      ) AS c16,
      COUNT(
        (
          a1.ss_ext_discount_amt * CASE
            WHEN (
              (
                21 <= a1.ss_quantity
              ) AND (
                a1.ss_quantity <= 40
              )
            )
            THEN 1
            ELSE NULL
          END
        )
      ) AS c17,
      SUM(
        (
          a1.ss_net_paid * CASE
            WHEN (
              (
                21 <= a1.ss_quantity
              ) AND (
                a1.ss_quantity <= 40
              )
            )
            THEN 1
            ELSE NULL
          END
        )
      ) AS c18,
      COUNT(
        (
          a1.ss_net_paid * CASE
            WHEN (
              (
                21 <= a1.ss_quantity
              ) AND (
                a1.ss_quantity <= 40
              )
            )
            THEN 1
            ELSE NULL
          END
        )
      ) AS c19,
      COUNT(
        CASE
          WHEN (
            (
              1 <= a1.ss_quantity
            ) AND (
              a1.ss_quantity <= 20
            )
          )
          THEN 1
          ELSE NULL
        END
      ) AS c20,
      SUM(
        (
          a1.ss_ext_discount_amt * CASE
            WHEN (
              (
                1 <= a1.ss_quantity
              ) AND (
                a1.ss_quantity <= 20
              )
            )
            THEN 1
            ELSE NULL
          END
        )
      ) AS c21,
      COUNT(
        (
          a1.ss_ext_discount_amt * CASE
            WHEN (
              (
                1 <= a1.ss_quantity
              ) AND (
                a1.ss_quantity <= 20
              )
            )
            THEN 1
            ELSE NULL
          END
        )
      ) AS c22,
      SUM(
        (
          a1.ss_net_paid * CASE
            WHEN (
              (
                1 <= a1.ss_quantity
              ) AND (
                a1.ss_quantity <= 20
              )
            )
            THEN 1
            ELSE NULL
          END
        )
      ) AS c23,
      COUNT(
        (
          a1.ss_net_paid * CASE
            WHEN (
              (
                1 <= a1.ss_quantity
              ) AND (
                a1.ss_quantity <= 20
              )
            )
            THEN 1
            ELSE NULL
          END
        )
      ) AS c24
    FROM store_sales AS a1
    WHERE
      (
        a1.ss_quantity <= 100
      ) AND (
        1 <= a1.ss_quantity
      )
  ) AS a0
  INNER JOIN reason AS a2
    ON (
      (
        a2.r_reason_sk = 1
      )
    )
)