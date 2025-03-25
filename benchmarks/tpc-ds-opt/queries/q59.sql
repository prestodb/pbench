WITH a1 AS (
  SELECT
    a2.c0 AS c0,
    a2.c1 AS c1,
    SUM(CASE WHEN (
      a2.c2 = 'Sunday'
    ) THEN a2.c3 ELSE NULL END) AS c2,
    SUM(CASE WHEN (
      a2.c2 = 'Monday'
    ) THEN a2.c3 ELSE NULL END) AS c3,
    SUM(CASE WHEN (
      a2.c2 = 'Tuesday'
    ) THEN a2.c3 ELSE NULL END) AS c4,
    SUM(CASE WHEN (
      a2.c2 = 'Wednesday'
    ) THEN a2.c3 ELSE NULL END) AS c5,
    SUM(CASE WHEN (
      a2.c2 = 'Thursday'
    ) THEN a2.c3 ELSE NULL END) AS c6,
    SUM(CASE WHEN (
      a2.c2 = 'Friday'
    ) THEN a2.c3 ELSE NULL END) AS c7,
    SUM(CASE WHEN (
      a2.c2 = 'Saturday'
    ) THEN a2.c3 ELSE NULL END) AS c8
  FROM (
    SELECT
      a4.d_week_seq AS c0,
      a3.ss_store_sk AS c1,
      a4.d_day_name AS c2,
      SUM(a3.ss_sales_price) AS c3
    FROM (
      store_sales AS a3
        INNER JOIN date_dim AS a4
          ON (
            a4.d_date_sk = a3.ss_sold_date_sk
          )
    )
    GROUP BY
      a3.ss_store_sk,
      a4.d_week_seq,
      a4.d_day_name
  ) AS a2
  GROUP BY
    a2.c0,
    a2.c1
)
SELECT
  a9.s_store_name AS S_STORE_NAME1,
  a9.s_store_id AS S_STORE_ID1,
  A6.c0 AS D_WEEK_SEQ1,
  (
    A6.c2 / A5.c2
  ),
  (
    A6.c3 / A5.c3
  ),
  (
    A6.c4 / A5.c4
  ),
  (
    A6.c5 / A5.c5
  ),
  (
    A6.c6 / A5.c6
  ),
  (
    A6.c7 / A5.c7
  ),
  (
    A6.c8 / A5.c8
  )
FROM (
  (
    (
      (
        (
          date_dim AS a0
            INNER JOIN a1 AS A5
              ON (
                (
                  a0.d_week_seq = A5.c0
                )
                AND (
                  a0.d_month_seq <= 1235
                )
                AND (
                  1224 <= a0.d_month_seq
                )
              )
        )
        INNER JOIN a1 AS A6
          ON (
            A6.c0 = (
              A5.c0 - 52
            )
          )
      )
      INNER JOIN store AS a7
        ON (
          A5.c1 = a7.s_store_sk
        )
    )
    INNER JOIN date_dim AS a8
      ON (
        (
          a8.d_week_seq = A6.c0
        )
        AND (
          a8.d_month_seq <= 1223
        )
        AND (
          1212 <= a8.d_month_seq
        )
      )
  )
  INNER JOIN store AS a9
    ON (
      a9.s_store_id = a7.s_store_id
    ) AND (
      A6.c1 = a9.s_store_sk
    )
)
ORDER BY
  1 ASC NULLS LAST,
  2 ASC NULLS LAST,
  3 ASC NULLS LAST
LIMIT 100