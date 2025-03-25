SELECT
  a0.c0 AS CUSTOMER_ID,
  a0.c15 AS CUSTOMER_FIRST_NAME,
  a0.c14 AS CUSTOMER_LAST_NAME,
  a0.c13 AS CUSTOMER_PREFERRED_CUST_FLAG
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
    MAX(a1.c15) AS c15,
    MAX(a1.c16) AS c16,
    MAX(a1.c17) AS c17,
    MAX(a1.c18) AS c18,
    MAX(a1.c19) AS c19,
    MAX(a1.c20) AS c20,
    MAX(a1.c21) AS c21
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
            a2.c6 = 'c'
          )
        )
        THEN a2.c5
        ELSE NULL
      END AS c7,
      CASE
        WHEN (
          (
            a2.c4 = 2002
          ) AND (
            a2.c6 = 'c'
          )
        )
        THEN a2.c4
        ELSE NULL
      END AS c8,
      CASE
        WHEN (
          (
            a2.c4 = 2002
          ) AND (
            a2.c6 = 'c'
          )
        )
        THEN a2.c6
        ELSE NULL
      END AS c9,
      CASE
        WHEN (
          (
            a2.c4 = 2001
          ) AND (
            a2.c6 = 'c'
          )
        )
        THEN a2.c5
        ELSE NULL
      END AS c10,
      CASE
        WHEN (
          (
            a2.c4 = 2001
          ) AND (
            a2.c6 = 'c'
          )
        )
        THEN a2.c4
        ELSE NULL
      END AS c11,
      CASE
        WHEN (
          (
            a2.c4 = 2001
          ) AND (
            a2.c6 = 'c'
          )
        )
        THEN a2.c6
        ELSE NULL
      END AS c12,
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
      END AS c13,
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
      END AS c14,
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
      END AS c15,
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
      END AS c16,
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
      END AS c17,
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
      END AS c18,
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
      END AS c19,
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
      END AS c20,
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
      END AS c21
    FROM (
      (
        SELECT
          a3.c0 AS c0,
          a3.c1 AS c1,
          a3.c2 AS c2,
          a3.c3 AS c3,
          a3.c4 AS c4,
          a3.c5 AS c5,
          's' AS c6
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
                  SUM(
                    (
                      (
                        (
                          (
                            a5.ss_ext_list_price - a5.ss_ext_wholesale_cost
                          ) - a5.ss_ext_discount_amt
                        ) + a5.ss_ext_sales_price
                      ) / 2
                    )
                  ) AS c0,
                  a6.d_year AS c1,
                  a5.ss_customer_sk AS c2
                FROM (
                  store_sales AS a5
                    INNER JOIN date_dim AS a6
                      ON (
                        (
                          a5.ss_sold_date_sk = a6.d_date_sk
                        )
                        AND (
                          (
                            a5.ss_sold_date_sk <= 2452640
                          ) AND (
                            a5.ss_sold_date_sk >= 2451911
                          )
                        )
                        AND (
                          a6.d_year IN (2002, 2001)
                        )
                      )
                )
                GROUP BY
                  a5.ss_customer_sk,
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
          'c' AS c6
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
                  SUM(
                    (
                      (
                        (
                          (
                            a10.cs_ext_list_price - a10.cs_ext_wholesale_cost
                          ) - a10.cs_ext_discount_amt
                        ) + a10.cs_ext_sales_price
                      ) / 2
                    )
                  ) AS c0,
                  a11.d_year AS c1,
                  a10.cs_bill_customer_sk AS c2
                FROM (
                  catalog_sales AS a10
                    INNER JOIN date_dim AS a11
                      ON (
                        (
                          a10.cs_sold_date_sk = a11.d_date_sk
                        )
                        AND (
                          (
                            a10.cs_sold_date_sk <= 2452640
                          ) AND (
                            a10.cs_sold_date_sk >= 2451911
                          )
                        )
                        AND (
                          a11.d_year IN (2002, 2001)
                        )
                      )
                )
                GROUP BY
                  a10.cs_bill_customer_sk,
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
      UNION ALL
      (
        SELECT
          a13.c0 AS c0,
          a13.c1 AS c1,
          a13.c2 AS c2,
          a13.c3 AS c3,
          a13.c4 AS c4,
          a13.c5 AS c5,
          'w' AS c6
        FROM (
          SELECT
            a14.c_customer_id AS c0,
            a14.c_first_name AS c1,
            a14.c_last_name AS c2,
            a14.c_preferred_cust_flag AS c3,
            a17.c1 AS c4,
            SUM(a17.c0) AS c5
          FROM (
            customer AS a14
              INNER JOIN (
                SELECT
                  SUM(
                    (
                      (
                        (
                          (
                            a15.ws_ext_list_price - a15.ws_ext_wholesale_cost
                          ) - a15.ws_ext_discount_amt
                        ) + a15.ws_ext_sales_price
                      ) / 2
                    )
                  ) AS c0,
                  a16.d_year AS c1,
                  a15.ws_bill_customer_sk AS c2
                FROM (
                  web_sales AS a15
                    INNER JOIN date_dim AS a16
                      ON (
                        (
                          a15.ws_sold_date_sk = a16.d_date_sk
                        )
                        AND (
                          (
                            a15.ws_sold_date_sk <= 2452640
                          ) AND (
                            a15.ws_sold_date_sk >= 2451911
                          )
                        )
                        AND (
                          a16.d_year IN (2002, 2001)
                        )
                      )
                )
                GROUP BY
                  a15.ws_bill_customer_sk,
                  a16.d_year
              ) AS a17
                ON (
                  a14.c_customer_sk = a17.c2
                )
          )
          GROUP BY
            a14.c_customer_id,
            a14.c_first_name,
            a14.c_last_name,
            a14.c_preferred_cust_flag,
            a17.c1,
            a14.c_birth_country,
            a14.c_login,
            a14.c_email_address
        ) AS a13
      )
    ) AS a2
  ) AS a1
  GROUP BY
    a1.c0
) AS a0
WHERE
  (
    CASE WHEN (
      a0.c19 > 0
    ) THEN (
      a0.c16 / a0.c19
    ) ELSE NULL END < CASE WHEN (
      a0.c10 > 0
    ) THEN (
      a0.c7 / a0.c10
    ) ELSE NULL END
  )
  AND (
    CASE WHEN (
      a0.c4 > 0
    ) THEN (
      a0.c1 / a0.c4
    ) ELSE NULL END < CASE WHEN (
      a0.c10 > 0
    ) THEN (
      a0.c7 / a0.c10
    ) ELSE NULL END
  )
  AND (
    a0.c21 = 's'
  )
  AND (
    a0.c12 = 'c'
  )
  AND (
    a0.c6 = 'w'
  )
  AND (
    a0.c18 = 's'
  )
  AND (
    a0.c9 = 'c'
  )
  AND (
    a0.c3 = 'w'
  )
  AND (
    a0.c20 = 2001
  )
  AND (
    a0.c17 = 2002
  )
  AND (
    a0.c11 = 2001
  )
  AND (
    a0.c8 = 2002
  )
  AND (
    a0.c5 = 2001
  )
  AND (
    a0.c2 = 2002
  )
  AND (
    0 < a0.c19
  )
  AND (
    0 < a0.c10
  )
  AND (
    0 < a0.c4
  )
ORDER BY
  1 ASC NULLS LAST
LIMIT 100