WITH a1 AS (
  SELECT
    a2.wr_returning_customer_sk AS c0,
    a4.ca_state AS c1,
    SUM(a2.wr_return_amt) AS c2
  FROM (
    (
      web_returns AS a2
        INNER JOIN date_dim AS a3
          ON (
            (
              a2.wr_returned_date_sk = a3.d_date_sk
            )
            AND (
              (
                a2.wr_returned_date_sk <= 2452640
              )
              AND (
                a2.wr_returned_date_sk >= 2452276
              )
            )
            AND (
              a3.d_year = 2002
            )
          )
    )
    INNER JOIN customer_address AS a4
      ON (
        a2.wr_returning_addr_sk = a4.ca_address_sk
      )
  )
  GROUP BY
    a2.wr_returning_customer_sk,
    a4.ca_state
), a0 AS (
  SELECT
    a6.c_last_review_date AS c0,
    a6.c_email_address AS c1,
    a6.c_login AS c2,
    a6.c_birth_country AS c3,
    a6.c_birth_year AS c4,
    a6.c_birth_month AS c5,
    a6.c_birth_day AS c6,
    a6.c_preferred_cust_flag AS c7,
    a6.c_last_name AS c8,
    a6.c_first_name AS c9,
    a6.c_salutation AS c10,
    a6.c_customer_id AS c11,
    A5.c2 AS c12,
    A5.c1 AS c13
  FROM (
    a1 AS A5
      INNER JOIN (
        customer AS a6
          INNER JOIN customer_address AS a7
            ON (
              (
                a7.ca_address_sk = a6.c_current_addr_sk
              ) AND (
                a7.ca_state = 'GA'
              )
            )
      )
        ON (
          A5.c0 = a6.c_customer_sk
        )
  )
)
SELECT
  A8.c11 AS C_CUSTOMER_ID,
  A8.c10 AS C_SALUTATION,
  A8.c9 AS C_FIRST_NAME,
  A8.c8 AS C_LAST_NAME,
  A8.c7 AS C_PREFERRED_CUST_FLAG,
  A8.c6 AS C_BIRTH_DAY,
  A8.c5 AS C_BIRTH_MONTH,
  A8.c4 AS C_BIRTH_YEAR,
  A8.c3 AS C_BIRTH_COUNTRY,
  A8.c2 AS C_LOGIN,
  A8.c1 AS C_EMAIL_ADDRESS,
  A8.c0 AS C_LAST_REVIEW_DATE,
  A8.c12 AS CTR_TOTAL_RETURN
FROM (
  a0 AS A8
    INNER JOIN (
      SELECT
        SUM(A10.c2) AS c0,
        COUNT(A10.c2) AS c1,
        a11.c0 AS c2
      FROM (
        a1 AS A10
          INNER JOIN (
            SELECT DISTINCT
              A12.c13 AS c0
            FROM a0 AS A12
          ) AS a11
            ON (
              a11.c0 = A10.c1
            )
      )
      GROUP BY
        a11.c0
    ) AS a9
      ON (
        a9.c2 = A8.c13
      )
      AND (
        (
          CAST((
            a9.c0 / a9.c1
          ) AS DECIMAL(31, 2)) * 1.2
        ) < A8.c12
      )
)
ORDER BY
  1 ASC NULLS LAST,
  2 ASC NULLS LAST,
  3 ASC NULLS LAST,
  4 ASC NULLS LAST,
  5 ASC NULLS LAST,
  6 ASC NULLS LAST,
  7 ASC NULLS LAST,
  8 ASC NULLS LAST,
  9 ASC NULLS LAST,
  10 ASC NULLS LAST,
  11 ASC NULLS LAST,
  12 ASC NULLS LAST,
  13 ASC NULLS LAST
LIMIT 100