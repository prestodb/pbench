SELECT
  a0.c0 AS STATE,
  a0.c1 AS CNT
FROM (
  SELECT
    a1.c0 AS c0,
    COUNT(*) AS c1
  FROM (
    SELECT
      a3.ca_state AS c0
    FROM (
      (
        customer AS a2
          INNER JOIN customer_address AS a3
            ON (
              a3.ca_address_sk = a2.c_current_addr_sk
            )
      )
      INNER JOIN (
        (
          store_sales AS a4
            INNER JOIN date_dim AS a5
              ON (
                (
                  a4.ss_sold_date_sk = a5.d_date_sk
                )
                AND (
                  (
                    a4.ss_sold_date_sk <= 2451941
                  ) AND (
                    a4.ss_sold_date_sk >= 2451911
                  )
                )
                AND (
                  a5.d_month_seq = (
                    SELECT DISTINCT
                      a6.d_month_seq
                    FROM date_dim AS a6
                    WHERE
                      (
                        a6.d_year = 2001
                      ) AND (
                        a6.d_moy = 1
                      )
                  )
                )
              )
        )
        INNER JOIN (
          SELECT
            COUNT(a8.i_current_price) OVER (PARTITION BY a8.i_category) AS c0,
            SUM(a8.i_current_price) OVER (PARTITION BY a8.i_category) AS c1,
            a8.i_current_price AS c2,
            a8.i_item_sk AS c3
          FROM item AS a8
          WHERE
            (
              NOT a8.i_category IS NULL
            )
        ) AS a7
          ON (
            (
              a4.ss_item_sk = a7.c3
            )
            AND (
              (
                1.2 * CAST((
                  a7.c1 / a7.c0
                ) AS DECIMAL(7, 2))
              ) < a7.c2
            )
          )
      )
        ON (
          a2.c_customer_sk = a4.ss_customer_sk
        )
    )
  ) AS a1
  GROUP BY
    a1.c0
) AS a0
WHERE
  (
    10 <= a0.c1
  )
ORDER BY
  2 ASC NULLS LAST
LIMIT 100