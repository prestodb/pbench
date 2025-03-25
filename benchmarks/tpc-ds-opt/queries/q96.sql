SELECT
  COUNT(*)
FROM (
  (
    (
      store_sales AS a0
        INNER JOIN time_dim AS a1
          ON (
            (
              a0.ss_sold_time_sk = a1.t_time_sk
            )
            AND (
              (
                a0.ss_store_sk <= 1485
              ) AND (
                a0.ss_store_sk >= 4
              )
            )
            AND (
              (
                a0.ss_hdemo_sk <= 6959
              ) AND (
                a0.ss_hdemo_sk >= 840
              )
            )
            AND (
              (
                a0.ss_sold_time_sk <= 75599
              ) AND (
                a0.ss_sold_time_sk >= 73800
              )
            )
            AND (
              a1.t_hour = 20
            )
            AND (
              30 <= a1.t_minute
            )
          )
    )
    INNER JOIN household_demographics AS a2
      ON (
        (
          a0.ss_hdemo_sk = a2.hd_demo_sk
        ) AND (
          a2.hd_dep_count = 7
        )
      )
  )
  INNER JOIN store AS a3
    ON (
      (
        a0.ss_store_sk = a3.s_store_sk
      ) AND (
        a3.s_store_name = 'ese'
      )
    )
)
ORDER BY
  1 ASC NULLS LAST
LIMIT 100