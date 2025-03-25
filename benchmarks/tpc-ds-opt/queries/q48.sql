SELECT
  SUM(a0.ss_quantity)
FROM (
  (
    (
      store_sales AS a0
        INNER JOIN date_dim AS a1
          ON (
            (
              a0.ss_sold_date_sk = a1.d_date_sk
            )
            AND (
              (
                (
                  a0.ss_net_profit >= 0
                ) AND (
                  a0.ss_net_profit <= 2000
                )
              )
              OR (
                (
                  (
                    a0.ss_net_profit >= 150
                  ) AND (
                    a0.ss_net_profit <= 3000
                  )
                )
                OR (
                  (
                    a0.ss_net_profit >= 50
                  ) AND (
                    a0.ss_net_profit <= 25000
                  )
                )
              )
            )
            AND (
              (
                (
                  a0.ss_sales_price >= 100.00
                ) AND (
                  a0.ss_sales_price <= 150.00
                )
              )
              OR (
                (
                  (
                    a0.ss_sales_price >= 50.00
                  ) AND (
                    a0.ss_sales_price <= 100.00
                  )
                )
                OR (
                  (
                    a0.ss_sales_price >= 150.00
                  ) AND (
                    a0.ss_sales_price <= 200.00
                  )
                )
              )
            )
            AND (
              NOT a0.ss_store_sk IS NULL
            )
            AND (
              (
                a0.ss_addr_sk <= 32499999
              ) AND (
                a0.ss_addr_sk >= 1
              )
            )
            AND (
              (
                a0.ss_sold_date_sk <= 2451910
              ) AND (
                a0.ss_sold_date_sk >= 2451545
              )
            )
            AND (
              (
                a0.ss_cdemo_sk <= 1920776
              ) AND (
                a0.ss_cdemo_sk >= 21
              )
            )
            AND (
              a1.d_year = 2000
            )
          )
    )
    INNER JOIN customer_address AS a2
      ON (
        (
          a0.ss_addr_sk = a2.ca_address_sk
        )
        AND (
          (
            (
              (
                a2.ca_state IN ('CO', 'OH', 'TX')
              )
              AND (
                (
                  a0.ss_net_profit >= 0
                ) AND (
                  a0.ss_net_profit <= 2000
                )
              )
            )
            OR (
              (
                a2.ca_state IN ('OR', 'MN', 'KY')
              )
              AND (
                (
                  a0.ss_net_profit >= 150
                ) AND (
                  a0.ss_net_profit <= 3000
                )
              )
            )
          )
          OR (
            (
              a2.ca_state IN ('VA', 'CA', 'MS')
            )
            AND (
              (
                a0.ss_net_profit >= 50
              ) AND (
                a0.ss_net_profit <= 25000
              )
            )
          )
        )
        AND (
          a2.ca_state IN ('CO', 'OH', 'TX', 'OR', 'MN', 'KY', 'VA', 'CA', 'MS')
        )
        AND (
          a2.ca_country = 'United States'
        )
      )
  )
  INNER JOIN customer_demographics AS a3
    ON (
      (
        a3.cd_demo_sk = a0.ss_cdemo_sk
      )
      AND (
        (
          (
            (
              (
                a3.cd_marital_status = 'M'
              ) AND (
                a3.cd_education_status = '4 yr Degree'
              )
            )
            AND (
              (
                a0.ss_sales_price >= 100.00
              ) AND (
                a0.ss_sales_price <= 150.00
              )
            )
          )
          OR (
            (
              (
                a3.cd_marital_status = 'D'
              ) AND (
                a3.cd_education_status = '2 yr Degree'
              )
            )
            AND (
              (
                a0.ss_sales_price >= 50.00
              ) AND (
                a0.ss_sales_price <= 100.00
              )
            )
          )
        )
        OR (
          (
            (
              a3.cd_marital_status = 'S'
            ) AND (
              a3.cd_education_status = 'College'
            )
          )
          AND (
            (
              a0.ss_sales_price >= 150.00
            ) AND (
              a0.ss_sales_price <= 200.00
            )
          )
        )
      )
      AND (
        a3.cd_marital_status IN ('M', 'D', 'S')
      )
      AND (
        a3.cd_education_status IN ('4 yr Degree', '2 yr Degree', 'College')
      )
    )
)