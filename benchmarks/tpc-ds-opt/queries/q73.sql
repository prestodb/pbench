SELECT
  a0.c_last_name AS C_LAST_NAME,
  a0.c_first_name AS C_FIRST_NAME,
  a0.c_salutation AS C_SALUTATION,
  a0.c_preferred_cust_flag AS C_PREFERRED_CUST_FLAG,
  a1.c0 AS SS_TICKET_NUMBER,
  a1.c2 AS CNT
FROM (
  customer AS a0
    INNER JOIN (
      SELECT
        a2.ss_ticket_number AS c0,
        a2.ss_customer_sk AS c1,
        COUNT(*) AS c2
      FROM (
        (
          (
            store_sales AS a2
              INNER JOIN date_dim AS a3
                ON (
                  (
                    a2.ss_sold_date_sk = a3.d_date_sk
                  )
                  AND (
                    (
                      a2.ss_hdemo_sk <= 5979
                    ) AND (
                      a2.ss_hdemo_sk >= 1520
                    )
                  )
                  AND (
                    (
                      a2.ss_sold_date_sk <= 2452246
                    ) AND (
                      a2.ss_sold_date_sk >= 2451180
                    )
                  )
                  AND (
                    (
                      a2.ss_store_sk <= 1464
                    ) AND (
                      a2.ss_store_sk >= 6
                    )
                  )
                  AND (
                    a3.d_year IN (1999, 2000, 2001)
                  )
                  AND (
                    1 <= a3.d_dom
                  )
                  AND (
                    a3.d_dom <= 2
                  )
                )
          )
          INNER JOIN household_demographics AS a4
            ON (
              (
                a2.ss_hdemo_sk = a4.hd_demo_sk
              )
              AND (
                1 < CASE
                  WHEN (
                    a4.hd_vehicle_count > 0
                  )
                  THEN (
                    a4.hd_dep_count / a4.hd_vehicle_count
                  )
                  ELSE NULL
                END
              )
              AND (
                a4.hd_buy_potential IN ('>10000', 'unknown')
              )
              AND (
                0 < a4.hd_vehicle_count
              )
            )
        )
        INNER JOIN store AS a5
          ON (
            (
              a2.ss_store_sk = a5.s_store_sk
            )
            AND (
              a5.s_county IN ('Williamson County', 'Franklin Parish', 'Bronx County', 'Orange County')
            )
          )
      )
      GROUP BY
        a2.ss_ticket_number,
        a2.ss_customer_sk
    ) AS a1
      ON (
        (
          a1.c1 = a0.c_customer_sk
        ) AND (
          1 <= a1.c2
        ) AND (
          a1.c2 <= 5
        )
      )
)
ORDER BY
  6 DESC,
  1 ASC NULLS LAST