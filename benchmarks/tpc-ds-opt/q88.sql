SELECT
  MAX(CASE WHEN (
    a0.c0 = 2
  ) THEN a0.c1 ELSE 0 END) AS H8_30_TO_9,
  MAX(CASE WHEN (
    a0.c0 = 3
  ) THEN a0.c1 ELSE 0 END) AS H9_TO_9_30,
  MAX(CASE WHEN (
    a0.c0 = 4
  ) THEN a0.c1 ELSE 0 END) AS H9_30_TO_10,
  MAX(CASE WHEN (
    a0.c0 = 5
  ) THEN a0.c1 ELSE 0 END) AS H10_TO_10_30,
  MAX(CASE WHEN (
    a0.c0 = 6
  ) THEN a0.c1 ELSE 0 END) AS H10_30_TO_11,
  MAX(CASE WHEN (
    a0.c0 = 7
  ) THEN a0.c1 ELSE 0 END) AS H11_TO_11_30,
  MAX(CASE WHEN (
    a0.c0 = 8
  ) THEN a0.c1 ELSE 0 END) AS H11_30_TO_12,
  MAX(CASE WHEN (
    a0.c0 = 1
  ) THEN a0.c1 ELSE 0 END) AS H12_TO_12_30
FROM (
  SELECT
    CASE
      WHEN (
        (
          a5.c0 = 1
        ) AND (
          (
            a4.t_minute < 30
          ) AND (
            a4.t_hour = 12
          )
        )
      )
      THEN 1
      WHEN (
        (
          a5.c0 = 2
        ) AND (
          (
            30 <= a4.t_minute
          ) AND (
            a4.t_hour = 8
          )
        )
      )
      THEN 2
      WHEN (
        (
          a5.c0 = 3
        ) AND (
          (
            a4.t_minute < 30
          ) AND (
            a4.t_hour = 9
          )
        )
      )
      THEN 3
      WHEN (
        (
          a5.c0 = 4
        ) AND (
          (
            30 <= a4.t_minute
          ) AND (
            a4.t_hour = 9
          )
        )
      )
      THEN 4
      WHEN (
        (
          a5.c0 = 5
        ) AND (
          (
            a4.t_minute < 30
          ) AND (
            a4.t_hour = 10
          )
        )
      )
      THEN 5
      WHEN (
        (
          a5.c0 = 6
        )
        AND (
          (
            30 <= a4.t_minute
          ) AND (
            a4.t_hour = 10
          )
        )
      )
      THEN 6
      WHEN (
        (
          a5.c0 = 7
        ) AND (
          (
            a4.t_minute < 30
          ) AND (
            a4.t_hour = 11
          )
        )
      )
      THEN 7
      WHEN (
        (
          a5.c0 = 8
        )
        AND (
          (
            30 <= a4.t_minute
          ) AND (
            a4.t_hour = 11
          )
        )
      )
      THEN 8
      ELSE NULL
    END AS c0,
    COUNT(a2.s_store_sk) AS c1
  FROM (
    (
      (
        (
          store_sales AS a1
            INNER JOIN store AS a2
              ON (
                (
                  a1.ss_store_sk = a2.s_store_sk
                )
                AND (
                  (
                    a1.ss_store_sk <= 1485
                  ) AND (
                    a1.ss_store_sk >= 4
                  )
                )
                AND (
                  (
                    a1.ss_sold_time_sk <= 44999
                  ) AND (
                    a1.ss_sold_time_sk >= 30600
                  )
                )
                AND (
                  (
                    a1.ss_hdemo_sk <= 7200
                  ) AND (
                    a1.ss_hdemo_sk >= 1
                  )
                )
                AND (
                  a2.s_store_name = 'ese'
                )
              )
        )
        INNER JOIN household_demographics AS a3
          ON (
            (
              a1.ss_hdemo_sk = a3.hd_demo_sk
            )
            AND (
              (
                (
                  (
                    a3.hd_dep_count = 4
                  ) AND (
                    a3.hd_vehicle_count <= 6
                  )
                )
                OR (
                  (
                    a3.hd_dep_count = 2
                  ) AND (
                    a3.hd_vehicle_count <= 4
                  )
                )
              )
              OR (
                (
                  a3.hd_dep_count = 0
                ) AND (
                  a3.hd_vehicle_count <= 2
                )
              )
            )
          )
      )
      INNER JOIN time_dim AS a4
        ON (
          (
            a1.ss_sold_time_sk = a4.t_time_sk
          )
          AND (
            (
              (
                (
                  (
                    (
                      (
                        (
                          (
                            a4.t_minute < 30
                          ) AND (
                            a4.t_hour = 12
                          )
                        )
                        OR (
                          (
                            30 <= a4.t_minute
                          ) AND (
                            a4.t_hour = 8
                          )
                        )
                      )
                      OR (
                        (
                          a4.t_minute < 30
                        ) AND (
                          a4.t_hour = 9
                        )
                      )
                    )
                    OR (
                      (
                        30 <= a4.t_minute
                      ) AND (
                        a4.t_hour = 9
                      )
                    )
                  )
                  OR (
                    (
                      a4.t_minute < 30
                    ) AND (
                      a4.t_hour = 10
                    )
                  )
                )
                OR (
                  (
                    30 <= a4.t_minute
                  ) AND (
                    a4.t_hour = 10
                  )
                )
              )
              OR (
                (
                  a4.t_minute < 30
                ) AND (
                  a4.t_hour = 11
                )
              )
            )
            OR (
              (
                30 <= a4.t_minute
              ) AND (
                a4.t_hour = 11
              )
            )
          )
        )
    )
    INNER JOIN VALUES
      (1),
      (2),
      (3),
      (4),
      (5),
      (6),
      (7),
      (8) AS a5(c0)
      ON (
        1 = 1
      )
  )
  GROUP BY
    CASE
      WHEN (
        (
          a5.c0 = 1
        ) AND (
          (
            a4.t_minute < 30
          ) AND (
            a4.t_hour = 12
          )
        )
      )
      THEN 1
      WHEN (
        (
          a5.c0 = 2
        ) AND (
          (
            30 <= a4.t_minute
          ) AND (
            a4.t_hour = 8
          )
        )
      )
      THEN 2
      WHEN (
        (
          a5.c0 = 3
        ) AND (
          (
            a4.t_minute < 30
          ) AND (
            a4.t_hour = 9
          )
        )
      )
      THEN 3
      WHEN (
        (
          a5.c0 = 4
        ) AND (
          (
            30 <= a4.t_minute
          ) AND (
            a4.t_hour = 9
          )
        )
      )
      THEN 4
      WHEN (
        (
          a5.c0 = 5
        ) AND (
          (
            a4.t_minute < 30
          ) AND (
            a4.t_hour = 10
          )
        )
      )
      THEN 5
      WHEN (
        (
          a5.c0 = 6
        )
        AND (
          (
            30 <= a4.t_minute
          ) AND (
            a4.t_hour = 10
          )
        )
      )
      THEN 6
      WHEN (
        (
          a5.c0 = 7
        ) AND (
          (
            a4.t_minute < 30
          ) AND (
            a4.t_hour = 11
          )
        )
      )
      THEN 7
      WHEN (
        (
          a5.c0 = 8
        )
        AND (
          (
            30 <= a4.t_minute
          ) AND (
            a4.t_hour = 11
          )
        )
      )
      THEN 8
      ELSE NULL
    END
) AS a0