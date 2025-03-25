SELECT
  (
    CAST(a0.c0 AS DECIMAL(15, 4)) / CAST(a0.c1 AS DECIMAL(15, 4))
  ) AS AM_PM_RATIO
FROM (
  SELECT
    MAX(CASE WHEN (
      a1.c0 = 1
    ) THEN a1.c1 ELSE 0 END) AS c0,
    MAX(CASE WHEN (
      a1.c0 = 2
    ) THEN a1.c1 ELSE 0 END) AS c1
  FROM (
    SELECT
      CASE
        WHEN (
          (
            a6.c0 = 1
          ) AND (
            (
              a5.t_hour <= 9
            ) AND (
              8 <= a5.t_hour
            )
          )
        )
        THEN 1
        WHEN (
          (
            a6.c0 = 2
          ) AND (
            (
              a5.t_hour <= 20
            ) AND (
              19 <= a5.t_hour
            )
          )
        )
        THEN 2
        ELSE NULL
      END AS c0,
      COUNT(a3.wp_char_count) AS c1
    FROM (
      (
        (
          (
            web_sales AS a2
              INNER JOIN web_page AS a3
                ON (
                  (
                    a2.ws_web_page_sk = a3.wp_web_page_sk
                  )
                  AND (
                    (
                      a2.ws_web_page_sk <= 3979
                    ) AND (
                      a2.ws_web_page_sk >= 10
                    )
                  )
                  AND (
                    (
                      a2.ws_ship_hdemo_sk <= 6839
                    ) AND (
                      a2.ws_ship_hdemo_sk >= 720
                    )
                  )
                  AND (
                    (
                      a2.ws_sold_time_sk <= 75599
                    ) AND (
                      a2.ws_sold_time_sk >= 28800
                    )
                  )
                  AND (
                    5000 <= a3.wp_char_count
                  )
                  AND (
                    a3.wp_char_count <= 5200
                  )
                )
          )
          INNER JOIN household_demographics AS a4
            ON (
              (
                a2.ws_ship_hdemo_sk = a4.hd_demo_sk
              ) AND (
                a4.hd_dep_count = 6
              )
            )
        )
        INNER JOIN time_dim AS a5
          ON (
            (
              a2.ws_sold_time_sk = a5.t_time_sk
            )
            AND (
              (
                (
                  a5.t_hour <= 9
                ) AND (
                  8 <= a5.t_hour
                )
              )
              OR (
                (
                  a5.t_hour <= 20
                ) AND (
                  19 <= a5.t_hour
                )
              )
            )
          )
      )
      INNER JOIN VALUES
        (1),
        (2) AS a6(c0)
        ON (
          1 = 1
        )
    )
    GROUP BY
      CASE
        WHEN (
          (
            a6.c0 = 1
          ) AND (
            (
              a5.t_hour <= 9
            ) AND (
              8 <= a5.t_hour
            )
          )
        )
        THEN 1
        WHEN (
          (
            a6.c0 = 2
          ) AND (
            (
              a5.t_hour <= 20
            ) AND (
              19 <= a5.t_hour
            )
          )
        )
        THEN 2
        ELSE NULL
      END
  ) AS a1
) AS a0
ORDER BY
  1 ASC NULLS LAST
LIMIT 100