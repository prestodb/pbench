SELECT
  a0.c0 AS S_STORE_NAME,
  a0.c1 AS S_STORE_ID,
  SUM(CASE WHEN (
    a0.c2 = 'Sunday'
  ) THEN a0.c3 ELSE NULL END) AS SUN_SALES,
  SUM(CASE WHEN (
    a0.c2 = 'Monday'
  ) THEN a0.c3 ELSE NULL END) AS MON_SALES,
  SUM(CASE WHEN (
    a0.c2 = 'Tuesday'
  ) THEN a0.c3 ELSE NULL END) AS TUE_SALES,
  SUM(CASE WHEN (
    a0.c2 = 'Wednesday'
  ) THEN a0.c3 ELSE NULL END) AS WED_SALES,
  SUM(CASE WHEN (
    a0.c2 = 'Thursday'
  ) THEN a0.c3 ELSE NULL END) AS THU_SALES,
  SUM(CASE WHEN (
    a0.c2 = 'Friday'
  ) THEN a0.c3 ELSE NULL END) AS FRI_SALES,
  SUM(CASE WHEN (
    a0.c2 = 'Saturday'
  ) THEN a0.c3 ELSE NULL END) AS SAT_SALES
FROM (
  SELECT
    a3.s_store_name AS c0,
    a3.s_store_id AS c1,
    a2.d_day_name AS c2,
    SUM(a1.ss_sales_price) AS c3
  FROM (
    (
      store_sales AS a1
        INNER JOIN date_dim AS a2
          ON (
            (
              a2.d_date_sk = a1.ss_sold_date_sk
            )
            AND (
              (
                a1.ss_store_sk <= 1500
              ) AND (
                a1.ss_store_sk >= 2
              )
            )
            AND (
              (
                a1.ss_sold_date_sk <= 2451910
              ) AND (
                a1.ss_sold_date_sk >= 2451545
              )
            )
            AND (
              a2.d_year = 2000
            )
          )
    )
    INNER JOIN store AS a3
      ON (
        (
          a3.s_store_sk = a1.ss_store_sk
        ) AND (
          a3.s_gmt_offset = -005.00
        )
      )
  )
  GROUP BY
    a2.d_day_name,
    a3.s_store_name,
    a3.s_store_id
) AS a0
GROUP BY
  a0.c0,
  a0.c1
ORDER BY
  1 ASC NULLS LAST,
  2 ASC NULLS LAST
LIMIT 100