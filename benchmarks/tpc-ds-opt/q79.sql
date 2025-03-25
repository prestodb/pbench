SELECT
  a5.c_last_name AS C_LAST_NAME,
  a5.c_first_name AS C_FIRST_NAME,
  SUBSTRING(a0.c2, 1, 30),
  a0.c0 AS SS_TICKET_NUMBER,
  a0.c3 AS AMT,
  a0.c4 AS PROFIT
FROM (
  (
    SELECT
      a1.ss_ticket_number AS c0,
      a1.ss_customer_sk AS c1,
      a4.s_city AS c2,
      SUM(a1.ss_coupon_amt) AS c3,
      SUM(a1.ss_net_profit) AS c4
    FROM (
      (
        (
          store_sales AS a1
            INNER JOIN date_dim AS a2
              ON (
                (
                  a1.ss_sold_date_sk = a2.d_date_sk
                )
                AND (
                  (
                    a1.ss_store_sk <= 1500
                  ) AND (
                    a1.ss_store_sk >= 1
                  )
                )
                AND (
                  (
                    a1.ss_sold_date_sk <= 2452275
                  ) AND (
                    a1.ss_sold_date_sk >= 2451183
                  )
                )
                AND (
                  (
                    a1.ss_hdemo_sk <= 6839
                  ) AND (
                    a1.ss_hdemo_sk >= 720
                  )
                )
                AND (
                  a2.d_year IN (1999, 2000, 2001)
                )
                AND (
                  a2.d_dow = 1
                )
              )
        )
        INNER JOIN household_demographics AS a3
          ON (
            (
              a1.ss_hdemo_sk = a3.hd_demo_sk
            )
            AND (
              (
                a3.hd_dep_count = 6
              ) OR (
                a3.hd_vehicle_count > 2
              )
            )
          )
      )
      INNER JOIN store AS a4
        ON (
          (
            a1.ss_store_sk = a4.s_store_sk
          )
          AND (
            200 <= a4.s_number_employees
          )
          AND (
            a4.s_number_employees <= 295
          )
        )
    )
    GROUP BY
      a1.ss_ticket_number,
      a1.ss_customer_sk,
      a4.s_city,
      a1.ss_addr_sk
  ) AS a0
  INNER JOIN customer AS a5
    ON (
      a0.c1 = a5.c_customer_sk
    )
)
ORDER BY
  1 ASC NULLS LAST,
  2 ASC NULLS LAST,
  3 ASC NULLS LAST,
  6 ASC NULLS LAST
LIMIT 100