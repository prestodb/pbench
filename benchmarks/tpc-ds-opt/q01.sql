WITH a2 AS (
  SELECT
    a3.sr_customer_sk AS c0,
    a3.sr_store_sk AS c1,
    SUM(a3.sr_return_amt) AS c2
  FROM (
    store_returns AS a3
      INNER JOIN date_dim AS a4
        ON (
          (
            a3.sr_returned_date_sk = a4.d_date_sk
          )
          AND (
            (
              a3.sr_returned_date_sk <= 2451910
            )
            AND (
              a3.sr_returned_date_sk >= 2451545
            )
          )
          AND (
            a4.d_year = 2000
          )
        )
  )
  GROUP BY
    a3.sr_customer_sk,
    a3.sr_store_sk
), a0 AS (
  SELECT
    a1.c_customer_id AS c0,
    A5.c2 AS c1,
    A5.c1 AS c2
  FROM (
    customer AS a1
      INNER JOIN (
        a2 AS A5
          INNER JOIN store AS a6
            ON (
              (
                a6.s_store_sk = A5.c1
              ) AND (
                a6.s_state = 'TN'
              )
            )
      )
        ON (
          A5.c0 = a1.c_customer_sk
        )
  )
)
SELECT
  A7.c0 AS C_CUSTOMER_ID
FROM (
  a0 AS A7
    INNER JOIN (
      SELECT
        SUM(A9.c2) AS c0,
        COUNT(A9.c2) AS c1,
        a10.c0 AS c2
      FROM (
        a2 AS A9
          INNER JOIN (
            SELECT DISTINCT
              A11.c2 AS c0
            FROM a0 AS A11
          ) AS a10
            ON (
              a10.c0 = A9.c1
            )
      )
      GROUP BY
        a10.c0
    ) AS a8
      ON (
        a8.c2 = A7.c2
      )
      AND (
        (
          CAST((
            a8.c0 / a8.c1
          ) AS DECIMAL(31, 2)) * 1.2
        ) < A7.c1
      )
)
ORDER BY
  1 ASC NULLS LAST
LIMIT 100