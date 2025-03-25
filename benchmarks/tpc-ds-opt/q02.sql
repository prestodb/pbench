WITH a1 AS (
  SELECT
    a2.c0 AS c0,
    SUM(CASE WHEN (
      a2.c1 = 'Sunday'
    ) THEN a2.c2 ELSE NULL END) AS c1,
    SUM(CASE WHEN (
      a2.c1 = 'Monday'
    ) THEN a2.c2 ELSE NULL END) AS c2,
    SUM(CASE WHEN (
      a2.c1 = 'Tuesday'
    ) THEN a2.c2 ELSE NULL END) AS c3,
    SUM(CASE WHEN (
      a2.c1 = 'Wednesday'
    ) THEN a2.c2 ELSE NULL END) AS c4,
    SUM(CASE WHEN (
      a2.c1 = 'Thursday'
    ) THEN a2.c2 ELSE NULL END) AS c5,
    SUM(CASE WHEN (
      a2.c1 = 'Friday'
    ) THEN a2.c2 ELSE NULL END) AS c6,
    SUM(CASE WHEN (
      a2.c1 = 'Saturday'
    ) THEN a2.c2 ELSE NULL END) AS c7
  FROM (
    SELECT
      a3.c1 AS c0,
      a3.c0 AS c1,
      SUM(a3.c2) AS c2
    FROM (
      (
        SELECT
          a6.d_day_name AS c0,
          a6.d_week_seq AS c1,
          SUM(a5.c0) AS c2
        FROM (
          (
            SELECT
              SUM(a4.cs_ext_sales_price) AS c0,
              a4.cs_sold_date_sk AS c1
            FROM catalog_sales AS a4
            GROUP BY
              a4.cs_sold_date_sk
          ) AS a5
          INNER JOIN date_dim AS a6
            ON (
              a6.d_date_sk = a5.c1
            )
        )
        GROUP BY
          a6.d_day_name,
          a6.d_week_seq
      )
      UNION ALL
      (
        SELECT
          a9.d_day_name AS c0,
          a9.d_week_seq AS c1,
          SUM(a8.c0) AS c2
        FROM (
          (
            SELECT
              SUM(a7.ws_ext_sales_price) AS c0,
              a7.ws_sold_date_sk AS c1
            FROM web_sales AS a7
            GROUP BY
              a7.ws_sold_date_sk
          ) AS a8
          INNER JOIN date_dim AS a9
            ON (
              a9.d_date_sk = a8.c1
            )
        )
        GROUP BY
          a9.d_day_name,
          a9.d_week_seq
      )
    ) AS a3
    GROUP BY
      a3.c1,
      a3.c0
  ) AS a2
  GROUP BY
    a2.c0
)
SELECT
  A11.c0 AS D_WEEK_SEQ1,
  ROUND((
    A11.c1 / A10.c1
  ), 2),
  ROUND((
    A11.c2 / A10.c2
  ), 2),
  ROUND((
    A11.c3 / A10.c3
  ), 2),
  ROUND((
    A11.c4 / A10.c4
  ), 2),
  ROUND((
    A11.c5 / A10.c5
  ), 2),
  ROUND((
    A11.c6 / A10.c6
  ), 2),
  ROUND((
    A11.c7 / A10.c7
  ), 2)
FROM (
  (
    (
      date_dim AS a0
        INNER JOIN a1 AS A10
          ON (
            (
              a0.d_week_seq = A10.c0
            ) AND (
              a0.d_year = 2002
            )
          )
    )
    INNER JOIN a1 AS A11
      ON (
        A11.c0 = (
          A10.c0 - 53
        )
      )
  )
  INNER JOIN date_dim AS a12
    ON (
      (
        a12.d_week_seq = A11.c0
      ) AND (
        a12.d_year = 2001
      )
    )
)
ORDER BY
  1 ASC NULLS LAST