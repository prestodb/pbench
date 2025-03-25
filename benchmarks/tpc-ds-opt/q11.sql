SELECT
  a0.c0 AS CUSTOMER_ID,
  a0.c9 AS CUSTOMER_FIRST_NAME,
  a0.c8 AS CUSTOMER_LAST_NAME,
  a0.c7 AS CUSTOMER_PREFERRED_CUST_FLAG
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
    MAX(a1.c14) AS c14,
    MAX(a1.c15) AS c15
  FROM (
    SELECT
      a2.c0 AS c0,
      CASE
        WHEN (
          (
            a2.c4 = 2002
          ) AND (
            a2.c6 = 'w'
          )
        )
        THEN a2.c5
        ELSE NULL
      END AS c1,
      CASE
        WHEN (
          (
            a2.c4 = 2002
          ) AND (
            a2.c6 = 'w'
          )
        )
        THEN a2.c4
        ELSE NULL
      END AS c2,
      CASE
        WHEN (
          (
            a2.c4 = 2002
          ) AND (
            a2.c6 = 'w'
          )
        )
        THEN a2.c6
        ELSE NULL
      END AS c3,
      CASE
        WHEN (
          (
            a2.c4 = 2001
          ) AND (
            a2.c6 = 'w'
          )
        )
        THEN a2.c5
        ELSE NULL
      END AS c4,
      CASE
        WHEN (
          (
            a2.c4 = 2001
          ) AND (
            a2.c6 = 'w'
          )
        )
        THEN a2.c4
        ELSE NULL
      END AS c5,
      CASE
        WHEN (
          (
            a2.c4 = 2001
          ) AND (
            a2.c6 = 'w'
          )
        )
        THEN a2.c6
        ELSE NULL
      END AS c6,
      CASE
        WHEN (
          (
            a2.c4 = 2002
          ) AND (
            a2.c6 = 's'
          )
        )
        THEN a2.c3
        ELSE NULL
      END AS c7,
      CASE
        WHEN (
          (
            a2.c4 = 2002
          ) AND (
            a2.c6 = 's'
          )
        )
        THEN a2.c2
        ELSE NULL
      END AS c8,
      CASE
        WHEN (
          (
            a2.c4 = 2002
          ) AND (
            a2.c6 = 's'
          )
        )
        THEN a2.c1
        ELSE NULL
      END AS c9,
      CASE
        WHEN (
          (
            a2.c4 = 2002
          ) AND (
            a2.c6 = 's'
          )
        )
        THEN a2.c5
        ELSE NULL
      END AS c10,
      CASE
        WHEN (
          (
            a2.c4 = 2002
          ) AND (
            a2.c6 = 's'
          )
        )
        THEN a2.c4
        ELSE NULL
      END AS c11,
      CASE
        WHEN (
          (
            a2.c4 = 2002
          ) AND (
            a2.c6 = 's'
          )
        )
        THEN a2.c6
        ELSE NULL
      END AS c12,
      CASE
        WHEN (
          (
            a2.c4 = 2001
          ) AND (
            a2.c6 = 's'
          )
        )
        THEN a2.c5
        ELSE NULL
      END AS c13,
      CASE
        WHEN (
          (
            a2.c4 = 2001
          ) AND (
            a2.c6 = 's'
          )
        )
        THEN a2.c4
        ELSE NULL
      END AS c14,
      CASE
        WHEN (
          (
            a2.c4 = 2001
          ) AND (
            a2.c6 = 's'
          )
        )
        THEN a2.c6
        ELSE NULL
      END AS c15
    FROM (
      (
        SELECT
          a3.c0 AS c0,
          a3.c1 AS c1,
          a3.c2 AS c2,
          a3.c3 AS c3,
          a3.c4 AS c4,
          a3.c5 AS c5,
          'w' AS c6
        FROM (
          SELECT
            a4.c_customer_id AS c0,
            a4.c_first_name AS c1,
            a4.c_last_name AS c2,
            a4.c_preferred_cust_flag AS c3,
            a7.c1 AS c4,
            SUM(a7.c0) AS c5
          FROM (
            customer AS a4
              INNER JOIN (
                SELECT
                  SUM((
                    a5.ws_ext_list_price - a5.ws_ext_discount_amt
                  )) AS c0,
                  a6.d_year AS c1,
                  a5.ws_bill_customer_sk AS c2
                FROM (
                  web_sales AS a5
                    INNER JOIN date_dim AS a6
                      ON (
                        (
                          a5.ws_sold_date_sk = a6.d_date_sk
                        )
                        AND (
                          (
                            a5.ws_sold_date_sk <= 2452640
                          ) AND (
                            a5.ws_sold_date_sk >= 2451911
                          )
                        )
                        AND (
                          a6.d_year IN (2002, 2001)
                        )
                      )
                )
                GROUP BY
                  a5.ws_bill_customer_sk,
                  a6.d_year
              ) AS a7
                ON (
                  a4.c_customer_sk = a7.c2
                )
          )
          GROUP BY
            a4.c_customer_id,
            a4.c_first_name,
            a4.c_last_name,
            a4.c_preferred_cust_flag,
            a7.c1,
            a4.c_birth_country,
            a4.c_login,
            a4.c_email_address
        ) AS a3
      )
      UNION ALL
      (
        SELECT
          a8.c0 AS c0,
          a8.c1 AS c1,
          a8.c2 AS c2,
          a8.c3 AS c3,
          a8.c4 AS c4,
          a8.c5 AS c5,
          's' AS c6
        FROM (
          SELECT
            a9.c_customer_id AS c0,
            a9.c_first_name AS c1,
            a9.c_last_name AS c2,
            a9.c_preferred_cust_flag AS c3,
            a12.c1 AS c4,
            SUM(a12.c0) AS c5
          FROM (
            customer AS a9
              INNER JOIN (
                SELECT
                  SUM((
                    a10.ss_ext_list_price - a10.ss_ext_discount_amt
                  )) AS c0,
                  a11.d_year AS c1,
                  a10.ss_customer_sk AS c2
                FROM (
                  store_sales AS a10
                    INNER JOIN date_dim AS a11
                      ON (
                        (
                          a10.ss_sold_date_sk = a11.d_date_sk
                        )
                        AND (
                          (
                            a10.ss_sold_date_sk <= 2452640
                          ) AND (
                            a10.ss_sold_date_sk >= 2451911
                          )
                        )
                        AND (
                          a11.d_year IN (2002, 2001)
                        )
                      )
                )
                GROUP BY
                  a10.ss_customer_sk,
                  a11.d_year
              ) AS a12
                ON (
                  a9.c_customer_sk = a12.c2
                )
          )
          GROUP BY
            a9.c_customer_id,
            a9.c_first_name,
            a9.c_last_name,
            a9.c_preferred_cust_flag,
            a12.c1,
            a9.c_birth_country,
            a9.c_login,
            a9.c_email_address
        ) AS a8
      )
    ) AS a2
  ) AS a1
  GROUP BY
    a1.c0
) AS a0
WHERE
  (
    CASE
      WHEN (
        a0.c13 > 0
      )
      THEN (
        a0.c10 / a0.c13
      )
      ELSE 0000000000000000000000000.000000
    END < CASE
      WHEN (
        a0.c4 > 0
      )
      THEN (
        a0.c1 / a0.c4
      )
      ELSE 0000000000000000000000000.000000
    END
  )
  AND (
    a0.c15 = 's'
  )
  AND (
    a0.c6 = 'w'
  )
  AND (
    a0.c12 = 's'
  )
  AND (
    a0.c3 = 'w'
  )
  AND (
    a0.c14 = 2001
  )
  AND (
    a0.c11 = 2002
  )
  AND (
    a0.c5 = 2001
  )
  AND (
    a0.c2 = 2002
  )
  AND (
    0 < a0.c13
  )
  AND (
    0 < a0.c4
  )
ORDER BY
  1 ASC NULLS LAST
LIMIT 100