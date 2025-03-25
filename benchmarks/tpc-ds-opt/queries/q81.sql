WITH a1 AS (
  SELECT
    a2.cr_returning_customer_sk AS c0,
    a4.ca_state AS c1,
    SUM(a2.cr_return_amt_inc_tax) AS c2
  FROM (
    (
      catalog_returns AS a2
        INNER JOIN date_dim AS a3
          ON (
            (
              a2.cr_returned_date_sk = a3.d_date_sk
            )
            AND (
              (
                a2.cr_returned_date_sk <= 2451910
              )
              AND (
                a2.cr_returned_date_sk >= 2451545
              )
            )
            AND (
              a3.d_year = 2000
            )
          )
    )
    INNER JOIN customer_address AS a4
      ON (
        a2.cr_returning_addr_sk = a4.ca_address_sk
      )
  )
  GROUP BY
    a2.cr_returning_customer_sk,
    a4.ca_state
), a0 AS (
  SELECT
    a7.ca_location_type AS c0,
    a7.ca_gmt_offset AS c1,
    a7.ca_country AS c2,
    a7.ca_zip AS c3,
    a7.ca_county AS c4,
    a7.ca_city AS c5,
    a7.ca_suite_number AS c6,
    a7.ca_street_type AS c7,
    a7.ca_street_name AS c8,
    a7.ca_street_number AS c9,
    a7.ca_state AS c10,
    a6.c_last_name AS c11,
    a6.c_first_name AS c12,
    a6.c_salutation AS c13,
    a6.c_customer_id AS c14,
    A5.c2 AS c15,
    A5.c1 AS c16
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
  A8.c14 AS C_CUSTOMER_ID,
  A8.c13 AS C_SALUTATION,
  A8.c12 AS C_FIRST_NAME,
  A8.c11 AS C_LAST_NAME,
  A8.c9 AS CA_STREET_NUMBER,
  A8.c8 AS CA_STREET_NAME,
  A8.c7 AS CA_STREET_TYPE,
  A8.c6 AS CA_SUITE_NUMBER,
  A8.c5 AS CA_CITY,
  A8.c4 AS CA_COUNTY,
  A8.c10 AS CA_STATE,
  A8.c3 AS CA_ZIP,
  A8.c2 AS CA_COUNTRY,
  A8.c1 AS CA_GMT_OFFSET,
  A8.c0 AS CA_LOCATION_TYPE,
  A8.c15 AS CTR_TOTAL_RETURN
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
              A12.c16 AS c0
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
        a9.c2 = A8.c16
      )
      AND (
        (
          CAST((
            a9.c0 / a9.c1
          ) AS DECIMAL(31, 2)) * 1.2
        ) < A8.c15
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
  12 ASC NULLS LAST,
  13 ASC NULLS LAST,
  14 ASC NULLS LAST,
  15 ASC NULLS LAST,
  16 ASC NULLS LAST
LIMIT 100