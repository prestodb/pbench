SELECT
  a0.c0 AS CUSTOMER_ID,
  a0.c8 AS CUSTOMER_FIRST_NAME,
  a0.c7 AS CUSTOMER_LAST_NAME
FROM (
  SELECT
    a1.c0 AS c0,
    MAX(a1.c1) AS c1,
    MAX(a1.c2) AS c2,
    MAX(a1.c3) AS c3,
    MAX(a1.c4) AS c4,
    MAX(a1.c5) AS c5,
    MAX(a1.c6) AS c6,
    MAX(a1.c7) AS c7,
    MAX(a1.c8) AS c8,
    MAX(a1.c9) AS c9,
    MAX(a1.c10) AS c10,
    MAX(a1.c11) AS c11,
    MAX(a1.c12) AS c12,
    MAX(a1.c13) AS c13,
    MAX(a1.c14) AS c14
  FROM (
    SELECT
      a2.c0 AS c0,
      CASE
        WHEN (
          (
            a2.c3 = 2002
          ) AND (
            a2.c5 = 'w'
          )
        )
        THEN a2.c4
        ELSE NULL
      END AS c1,
      CASE
        WHEN (
          (
            a2.c3 = 2002
          ) AND (
            a2.c5 = 'w'
          )
        )
        THEN a2.c3
        ELSE NULL
      END AS c2,
      CASE
        WHEN (
          (
            a2.c3 = 2002
          ) AND (
            a2.c5 = 'w'
          )
        )
        THEN a2.c5
        ELSE NULL
      END AS c3,
      CASE
        WHEN (
          (
            a2.c3 = 2001
          ) AND (
            a2.c5 = 'w'
          )
        )
        THEN a2.c4
        ELSE NULL
      END AS c4,
      CASE
        WHEN (
          (
            a2.c3 = 2001
          ) AND (
            a2.c5 = 'w'
          )
        )
        THEN a2.c3
        ELSE NULL
      END AS c5,
      CASE
        WHEN (
          (
            a2.c3 = 2001
          ) AND (
            a2.c5 = 'w'
          )
        )
        THEN a2.c5
        ELSE NULL
      END AS c6,
      CASE
        WHEN (
          (
            a2.c3 = 2002
          ) AND (
            a2.c5 = 's'
          )
        )
        THEN a2.c2
        ELSE NULL
      END AS c7,
      CASE
        WHEN (
          (
            a2.c3 = 2002
          ) AND (
            a2.c5 = 's'
          )
        )
        THEN a2.c1
        ELSE NULL
      END AS c8,
      CASE
        WHEN (
          (
            a2.c3 = 2002
          ) AND (
            a2.c5 = 's'
          )
        )
        THEN a2.c4
        ELSE NULL
      END AS c9,
      CASE
        WHEN (
          (
            a2.c3 = 2002
          ) AND (
            a2.c5 = 's'
          )
        )
        THEN a2.c3
        ELSE NULL
      END AS c10,
      CASE
        WHEN (
          (
            a2.c3 = 2002
          ) AND (
            a2.c5 = 's'
          )
        )
        THEN a2.c5
        ELSE NULL
      END AS c11,
      CASE
        WHEN (
          (
            a2.c3 = 2001
          ) AND (
            a2.c5 = 's'
          )
        )
        THEN a2.c4
        ELSE NULL
      END AS c12,
      CASE
        WHEN (
          (
            a2.c3 = 2001
          ) AND (
            a2.c5 = 's'
          )
        )
        THEN a2.c3
        ELSE NULL
      END AS c13,
      CASE
        WHEN (
          (
            a2.c3 = 2001
          ) AND (
            a2.c5 = 's'
          )
        )
        THEN a2.c5
        ELSE NULL
      END AS c14
    FROM (
      (
        SELECT
          a3.c0 AS c0,
          a3.c1 AS c1,
          a3.c2 AS c2,
          a3.c3 AS c3,
          a3.c4 AS c4,
          'w' AS c5
        FROM (
          SELECT
            a6.c_customer_id AS c0,
            a6.c_first_name AS c1,
            a6.c_last_name AS c2,
            a5.d_year AS c3,
            SUM(a4.ws_net_paid) AS c4
          FROM (
            (
              web_sales AS a4
                INNER JOIN date_dim AS a5
                  ON (
                    (
                      a4.ws_sold_date_sk = a5.d_date_sk
                    )
                    AND (
                      (
                        a4.ws_sold_date_sk <= 2452640
                      ) AND (
                        a4.ws_sold_date_sk >= 2451911
                      )
                    )
                    AND (
                      a5.d_year IN (2001, 2002)
                    )
                  )
            )
            INNER JOIN customer AS a6
              ON (
                a6.c_customer_sk = a4.ws_bill_customer_sk
              )
          )
          GROUP BY
            a6.c_customer_id,
            a6.c_first_name,
            a6.c_last_name,
            a5.d_year
        ) AS a3
      )
      UNION ALL
      (
        SELECT
          a7.c0 AS c0,
          a7.c1 AS c1,
          a7.c2 AS c2,
          a7.c3 AS c3,
          a7.c4 AS c4,
          's' AS c5
        FROM (
          SELECT
            a10.c_customer_id AS c0,
            a10.c_first_name AS c1,
            a10.c_last_name AS c2,
            a9.d_year AS c3,
            SUM(a8.ss_net_paid) AS c4
          FROM (
            (
              store_sales AS a8
                INNER JOIN date_dim AS a9
                  ON (
                    (
                      a8.ss_sold_date_sk = a9.d_date_sk
                    )
                    AND (
                      (
                        a8.ss_sold_date_sk <= 2452640
                      ) AND (
                        a8.ss_sold_date_sk >= 2451911
                      )
                    )
                    AND (
                      a9.d_year IN (2001, 2002)
                    )
                  )
            )
            INNER JOIN customer AS a10
              ON (
                a10.c_customer_sk = a8.ss_customer_sk
              )
          )
          GROUP BY
            a10.c_customer_id,
            a10.c_first_name,
            a10.c_last_name,
            a9.d_year
        ) AS a7
      )
    ) AS a2
  ) AS a1
  GROUP BY
    a1.c0
) AS a0
WHERE
  (
    CASE WHEN (
      a0.c12 > 0
    ) THEN (
      a0.c9 / a0.c12
    ) ELSE NULL END < CASE WHEN (
      a0.c4 > 0
    ) THEN (
      a0.c1 / a0.c4
    ) ELSE NULL END
  )
  AND (
    a0.c14 = 's'
  )
  AND (
    a0.c6 = 'w'
  )
  AND (
    a0.c11 = 's'
  )
  AND (
    a0.c3 = 'w'
  )
  AND (
    a0.c13 = 2001
  )
  AND (
    a0.c10 = 2002
  )
  AND (
    a0.c5 = 2001
  )
  AND (
    a0.c2 = 2002
  )
  AND (
    0 < a0.c12
  )
  AND (
    0 < a0.c4
  )
ORDER BY
  1 ASC NULLS LAST
LIMIT 100