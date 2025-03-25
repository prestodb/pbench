SELECT
  a1.c_last_name AS C_LAST_NAME,
  a1.c_first_name AS C_FIRST_NAME,
  a0.ca_city AS CA_CITY,
  a2.c2 AS BOUGHT_CITY,
  a2.c0 AS SS_TICKET_NUMBER,
  a2.c3 AS AMT,
  a2.c4 AS PROFIT
FROM (
  customer_address AS a0
    INNER JOIN (
      customer AS a1
        INNER JOIN (
          SELECT
            a4.ss_ticket_number AS c0,
            a4.ss_customer_sk AS c1,
            a3.ca_city AS c2,
            SUM(a4.ss_coupon_amt) AS c3,
            SUM(a4.ss_net_profit) AS c4
          FROM (
            customer_address AS a3
              INNER JOIN (
                (
                  (
                    store_sales AS a4
                      INNER JOIN store AS a5
                        ON (
                          (
                            a4.ss_store_sk = a5.s_store_sk
                          )
                          AND (
                            (
                              a4.ss_store_sk <= 1357
                            ) AND (
                              a4.ss_store_sk >= 12
                            )
                          )
                          AND (
                            (
                              a4.ss_sold_date_sk <= 2452274
                            ) AND (
                              a4.ss_sold_date_sk >= 2451181
                            )
                          )
                          AND (
                            (
                              a4.ss_hdemo_sk <= 6599
                            ) AND (
                              a4.ss_hdemo_sk >= 480
                            )
                          )
                          AND (
                            a5.s_city IN ('Fairview', 'Midway')
                          )
                        )
                  )
                  INNER JOIN date_dim AS a6
                    ON (
                      (
                        a4.ss_sold_date_sk = a6.d_date_sk
                      )
                      AND (
                        a6.d_dow IN (6, 0)
                      )
                      AND (
                        a6.d_year IN (1999, 2000, 2001)
                      )
                    )
                )
                INNER JOIN household_demographics AS a7
                  ON (
                    (
                      a4.ss_hdemo_sk = a7.hd_demo_sk
                    )
                    AND (
                      (
                        a7.hd_dep_count = 4
                      ) OR (
                        a7.hd_vehicle_count = 3
                      )
                    )
                  )
              )
                ON (
                  a4.ss_addr_sk = a3.ca_address_sk
                )
          )
          GROUP BY
            a4.ss_ticket_number,
            a4.ss_customer_sk,
            a4.ss_addr_sk,
            a3.ca_city
        ) AS a2
          ON (
            a2.c1 = a1.c_customer_sk
          )
    )
      ON (
        a1.c_current_addr_sk = a0.ca_address_sk
      ) AND (
        a0.ca_city <> a2.c2
      )
)
ORDER BY
  1 ASC NULLS LAST,
  2 ASC NULLS LAST,
  3 ASC NULLS LAST,
  4 ASC NULLS LAST,
  5 ASC NULLS LAST
LIMIT 100