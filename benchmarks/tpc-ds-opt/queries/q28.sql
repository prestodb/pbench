SELECT
  CAST((
    a0.c4 / a0.c3
  ) AS DECIMAL(7, 2)) AS B1_LP,
  a0.c3 AS B1_CNT,
  a0.c5 AS B1_CNTD,
  CAST((
    a0.c7 / a0.c6
  ) AS DECIMAL(7, 2)) AS B2_LP,
  a0.c6 AS B2_CNT,
  a0.c8 AS B2_CNTD,
  CAST((
    a0.c10 / a0.c9
  ) AS DECIMAL(7, 2)) AS B3_LP,
  a0.c9 AS B3_CNT,
  a0.c11 AS B3_CNTD,
  CAST((
    a0.c13 / a0.c12
  ) AS DECIMAL(7, 2)) AS B4_LP,
  a0.c12 AS B4_CNT,
  a0.c14 AS B4_CNTD,
  CAST((
    a0.c16 / a0.c15
  ) AS DECIMAL(7, 2)) AS B5_LP,
  a0.c15 AS B5_CNT,
  a0.c17 AS B5_CNTD,
  CAST((
    a0.c1 / a0.c0
  ) AS DECIMAL(7, 2)) AS B6_LP,
  a0.c0 AS B6_CNT,
  a0.c2 AS B6_CNTD
FROM (
  SELECT
    MAX(CASE WHEN (
      a1.c0 = 1
    ) THEN a1.c3 ELSE 0 END) AS c0,
    MAX(CASE WHEN (
      a1.c0 = 1
    ) THEN a1.c2 ELSE NULL END) AS c1,
    MAX(CASE WHEN (
      a1.c0 = 1
    ) THEN a1.c1 ELSE 0 END) AS c2,
    MAX(CASE WHEN (
      a1.c0 = 2
    ) THEN a1.c3 ELSE 0 END) AS c3,
    MAX(CASE WHEN (
      a1.c0 = 2
    ) THEN a1.c2 ELSE NULL END) AS c4,
    MAX(CASE WHEN (
      a1.c0 = 2
    ) THEN a1.c1 ELSE 0 END) AS c5,
    MAX(CASE WHEN (
      a1.c0 = 3
    ) THEN a1.c3 ELSE 0 END) AS c6,
    MAX(CASE WHEN (
      a1.c0 = 3
    ) THEN a1.c2 ELSE NULL END) AS c7,
    MAX(CASE WHEN (
      a1.c0 = 3
    ) THEN a1.c1 ELSE 0 END) AS c8,
    MAX(CASE WHEN (
      a1.c0 = 4
    ) THEN a1.c3 ELSE 0 END) AS c9,
    MAX(CASE WHEN (
      a1.c0 = 4
    ) THEN a1.c2 ELSE NULL END) AS c10,
    MAX(CASE WHEN (
      a1.c0 = 4
    ) THEN a1.c1 ELSE 0 END) AS c11,
    MAX(CASE WHEN (
      a1.c0 = 5
    ) THEN a1.c3 ELSE 0 END) AS c12,
    MAX(CASE WHEN (
      a1.c0 = 5
    ) THEN a1.c2 ELSE NULL END) AS c13,
    MAX(CASE WHEN (
      a1.c0 = 5
    ) THEN a1.c1 ELSE 0 END) AS c14,
    MAX(CASE WHEN (
      a1.c0 = 6
    ) THEN a1.c3 ELSE 0 END) AS c15,
    MAX(CASE WHEN (
      a1.c0 = 6
    ) THEN a1.c2 ELSE NULL END) AS c16,
    MAX(CASE WHEN (
      a1.c0 = 6
    ) THEN a1.c1 ELSE 0 END) AS c17
  FROM (
    SELECT
      a2.c0 AS c0,
      COUNT(a2.c3) AS c1,
      SUM(a2.c2) AS c2,
      SUM(a2.c1) AS c3
    FROM (
      SELECT
        CASE
          WHEN (
            (
              a4.c0 = 1
            )
            AND (
              (
                a3.ss_quantity <= 30
              ) AND (
                26 <= a3.ss_quantity
              )
            )
          )
          THEN 1
          WHEN (
            (
              a4.c0 = 2
            )
            AND (
              (
                a3.ss_quantity <= 5
              ) AND (
                0 <= a3.ss_quantity
              )
            )
          )
          THEN 2
          WHEN (
            (
              a4.c0 = 3
            )
            AND (
              (
                a3.ss_quantity <= 10
              ) AND (
                6 <= a3.ss_quantity
              )
            )
          )
          THEN 3
          WHEN (
            (
              a4.c0 = 4
            )
            AND (
              (
                a3.ss_quantity <= 15
              ) AND (
                11 <= a3.ss_quantity
              )
            )
          )
          THEN 4
          WHEN (
            (
              a4.c0 = 5
            )
            AND (
              (
                a3.ss_quantity <= 20
              ) AND (
                16 <= a3.ss_quantity
              )
            )
          )
          THEN 5
          WHEN (
            (
              a4.c0 = 6
            )
            AND (
              (
                a3.ss_quantity <= 25
              ) AND (
                21 <= a3.ss_quantity
              )
            )
          )
          THEN 6
          ELSE NULL
        END AS c0,
        COUNT(a3.ss_list_price) AS c1,
        SUM(a3.ss_list_price) AS c2,
        a3.ss_list_price AS c3
      FROM (
        store_sales AS a3
          INNER JOIN VALUES
            (1),
            (2),
            (3),
            (4),
            (5),
            (6) AS a4(c0)
            ON (
              (
                (
                  (
                    (
                      (
                        (
                          (
                            (
                              (
                                (
                                  (
                                    a3.ss_list_price >= 154
                                  ) AND (
                                    a3.ss_list_price <= 164
                                  )
                                )
                                OR (
                                  (
                                    a3.ss_coupon_amt >= 7326
                                  ) AND (
                                    a3.ss_coupon_amt <= 8326
                                  )
                                )
                              )
                              OR (
                                (
                                  a3.ss_wholesale_cost >= 7
                                ) AND (
                                  a3.ss_wholesale_cost <= 27
                                )
                              )
                            )
                            AND (
                              a3.ss_quantity <= 30
                            )
                          )
                          AND (
                            26 <= a3.ss_quantity
                          )
                        )
                        OR (
                          (
                            (
                              (
                                (
                                  (
                                    a3.ss_list_price >= 8
                                  ) AND (
                                    a3.ss_list_price <= 18
                                  )
                                )
                                OR (
                                  (
                                    a3.ss_coupon_amt >= 459
                                  ) AND (
                                    a3.ss_coupon_amt <= 1459
                                  )
                                )
                              )
                              OR (
                                (
                                  a3.ss_wholesale_cost >= 57
                                ) AND (
                                  a3.ss_wholesale_cost <= 77
                                )
                              )
                            )
                            AND (
                              a3.ss_quantity <= 5
                            )
                          )
                          AND (
                            0 <= a3.ss_quantity
                          )
                        )
                      )
                      OR (
                        (
                          (
                            (
                              (
                                (
                                  a3.ss_list_price >= 90
                                ) AND (
                                  a3.ss_list_price <= 100
                                )
                              )
                              OR (
                                (
                                  a3.ss_coupon_amt >= 2323
                                ) AND (
                                  a3.ss_coupon_amt <= 3323
                                )
                              )
                            )
                            OR (
                              (
                                a3.ss_wholesale_cost >= 31
                              ) AND (
                                a3.ss_wholesale_cost <= 51
                              )
                            )
                          )
                          AND (
                            a3.ss_quantity <= 10
                          )
                        )
                        AND (
                          6 <= a3.ss_quantity
                        )
                      )
                    )
                    OR (
                      (
                        (
                          (
                            (
                              (
                                a3.ss_list_price >= 142
                              ) AND (
                                a3.ss_list_price <= 152
                              )
                            )
                            OR (
                              (
                                a3.ss_coupon_amt >= 12214
                              ) AND (
                                a3.ss_coupon_amt <= 13214
                              )
                            )
                          )
                          OR (
                            (
                              a3.ss_wholesale_cost >= 79
                            ) AND (
                              a3.ss_wholesale_cost <= 99
                            )
                          )
                        )
                        AND (
                          a3.ss_quantity <= 15
                        )
                      )
                      AND (
                        11 <= a3.ss_quantity
                      )
                    )
                  )
                  OR (
                    (
                      (
                        (
                          (
                            (
                              a3.ss_list_price >= 135
                            ) AND (
                              a3.ss_list_price <= 145
                            )
                          )
                          OR (
                            (
                              a3.ss_coupon_amt >= 6071
                            ) AND (
                              a3.ss_coupon_amt <= 7071
                            )
                          )
                        )
                        OR (
                          (
                            a3.ss_wholesale_cost >= 38
                          ) AND (
                            a3.ss_wholesale_cost <= 58
                          )
                        )
                      )
                      AND (
                        a3.ss_quantity <= 20
                      )
                    )
                    AND (
                      16 <= a3.ss_quantity
                    )
                  )
                )
                OR (
                  (
                    (
                      (
                        (
                          (
                            a3.ss_list_price >= 122
                          ) AND (
                            a3.ss_list_price <= 132
                          )
                        )
                        OR (
                          (
                            a3.ss_coupon_amt >= 836
                          ) AND (
                            a3.ss_coupon_amt <= 1836
                          )
                        )
                      )
                      OR (
                        (
                          a3.ss_wholesale_cost >= 17
                        ) AND (
                          a3.ss_wholesale_cost <= 37
                        )
                      )
                    )
                    AND (
                      a3.ss_quantity <= 25
                    )
                  )
                  AND (
                    21 <= a3.ss_quantity
                  )
                )
              )
            )
      )
      GROUP BY
        a3.ss_list_price,
        CASE
          WHEN (
            (
              a4.c0 = 1
            )
            AND (
              (
                a3.ss_quantity <= 30
              ) AND (
                26 <= a3.ss_quantity
              )
            )
          )
          THEN 1
          WHEN (
            (
              a4.c0 = 2
            )
            AND (
              (
                a3.ss_quantity <= 5
              ) AND (
                0 <= a3.ss_quantity
              )
            )
          )
          THEN 2
          WHEN (
            (
              a4.c0 = 3
            )
            AND (
              (
                a3.ss_quantity <= 10
              ) AND (
                6 <= a3.ss_quantity
              )
            )
          )
          THEN 3
          WHEN (
            (
              a4.c0 = 4
            )
            AND (
              (
                a3.ss_quantity <= 15
              ) AND (
                11 <= a3.ss_quantity
              )
            )
          )
          THEN 4
          WHEN (
            (
              a4.c0 = 5
            )
            AND (
              (
                a3.ss_quantity <= 20
              ) AND (
                16 <= a3.ss_quantity
              )
            )
          )
          THEN 5
          WHEN (
            (
              a4.c0 = 6
            )
            AND (
              (
                a3.ss_quantity <= 25
              ) AND (
                21 <= a3.ss_quantity
              )
            )
          )
          THEN 6
          ELSE NULL
        END
    ) AS a2
    GROUP BY
      a2.c0
  ) AS a1
) AS a0
LIMIT 100