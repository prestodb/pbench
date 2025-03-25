SELECT
  (
    a0.c4 / a0.c5
  ),
  CAST((
    a0.c2 / a0.c3
  ) AS DECIMAL(7, 2)),
  CAST((
    a0.c0 / a0.c1
  ) AS DECIMAL(7, 2)),
  a0.c0
FROM (
  SELECT
    SUM(a1.ss_ext_wholesale_cost) AS c0,
    COUNT(a1.ss_ext_wholesale_cost) AS c1,
    SUM(a1.ss_ext_sales_price) AS c2,
    COUNT(a1.ss_ext_sales_price) AS c3,
    SUM(CAST(a1.ss_quantity AS DOUBLE)) AS c4,
    COUNT(CAST(a1.ss_quantity AS DOUBLE)) AS c5
  FROM (
    (
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
                    (
                      a1.ss_net_profit >= 100
                    ) AND (
                      a1.ss_net_profit <= 200
                    )
                  )
                  OR (
                    (
                      (
                        a1.ss_net_profit >= 150
                      ) AND (
                        a1.ss_net_profit <= 300
                      )
                    )
                    OR (
                      (
                        a1.ss_net_profit >= 50
                      ) AND (
                        a1.ss_net_profit <= 250
                      )
                    )
                  )
                )
                AND (
                  (
                    (
                      a1.ss_sales_price >= 100.00
                    ) AND (
                      a1.ss_sales_price <= 150.00
                    )
                  )
                  OR (
                    (
                      (
                        a1.ss_sales_price >= 50.00
                      ) AND (
                        a1.ss_sales_price <= 100.00
                      )
                    )
                    OR (
                      (
                        a1.ss_sales_price >= 150.00
                      ) AND (
                        a1.ss_sales_price <= 200.00
                      )
                    )
                  )
                )
                AND (
                  NOT a1.ss_store_sk IS NULL
                )
                AND (
                  (
                    a1.ss_addr_sk <= 32499999
                  ) AND (
                    a1.ss_addr_sk >= 1
                  )
                )
                AND (
                  (
                    a1.ss_sold_date_sk <= 2452275
                  ) AND (
                    a1.ss_sold_date_sk >= 2451911
                  )
                )
                AND (
                  (
                    a1.ss_hdemo_sk <= 6479
                  ) AND (
                    a1.ss_hdemo_sk >= 120
                  )
                )
                AND (
                  (
                    a1.ss_cdemo_sk <= 1920788
                  ) AND (
                    a1.ss_cdemo_sk >= 21
                  )
                )
                AND (
                  a2.d_year = 2001
                )
              )
        )
        INNER JOIN household_demographics AS a3
          ON (
            (
              a1.ss_hdemo_sk = a3.hd_demo_sk
            ) AND (
              a3.hd_dep_count IN (3, 1)
            )
          )
      )
      INNER JOIN customer_address AS a4
        ON (
          (
            a1.ss_addr_sk = a4.ca_address_sk
          )
          AND (
            (
              (
                (
                  a4.ca_state IN ('TX', 'OH', 'TX')
                )
                AND (
                  (
                    a1.ss_net_profit >= 100
                  ) AND (
                    a1.ss_net_profit <= 200
                  )
                )
              )
              OR (
                (
                  a4.ca_state IN ('OR', 'NM', 'KY')
                )
                AND (
                  (
                    a1.ss_net_profit >= 150
                  ) AND (
                    a1.ss_net_profit <= 300
                  )
                )
              )
            )
            OR (
              (
                a4.ca_state IN ('VA', 'TX', 'MS')
              )
              AND (
                (
                  a1.ss_net_profit >= 50
                ) AND (
                  a1.ss_net_profit <= 250
                )
              )
            )
          )
          AND (
            a4.ca_state IN ('TX', 'OH', 'OR', 'NM', 'KY', 'VA', 'MS')
          )
          AND (
            a4.ca_country = 'United States'
          )
        )
    )
    INNER JOIN customer_demographics AS a5
      ON (
        (
          a5.cd_demo_sk = a1.ss_cdemo_sk
        )
        AND (
          (
            (
              (
                (
                  (
                    a5.cd_marital_status = 'M'
                  )
                  AND (
                    a5.cd_education_status = 'Advanced Degree'
                  )
                )
                AND (
                  (
                    a1.ss_sales_price >= 100.00
                  ) AND (
                    a1.ss_sales_price <= 150.00
                  )
                )
              )
              AND (
                a3.hd_dep_count = 3
              )
            )
            OR (
              (
                (
                  (
                    a5.cd_marital_status = 'S'
                  ) AND (
                    a5.cd_education_status = 'College'
                  )
                )
                AND (
                  (
                    a1.ss_sales_price >= 50.00
                  ) AND (
                    a1.ss_sales_price <= 100.00
                  )
                )
              )
              AND (
                a3.hd_dep_count = 1
              )
            )
          )
          OR (
            (
              (
                (
                  a5.cd_marital_status = 'W'
                ) AND (
                  a5.cd_education_status = '2 yr Degree'
                )
              )
              AND (
                (
                  a1.ss_sales_price >= 150.00
                ) AND (
                  a1.ss_sales_price <= 200.00
                )
              )
            )
            AND (
              a3.hd_dep_count = 1
            )
          )
        )
        AND (
          a5.cd_marital_status IN ('M', 'S', 'W')
        )
        AND (
          a5.cd_education_status IN ('Advanced Degree', 'College', '2 yr Degree')
        )
      )
  )
) AS a0