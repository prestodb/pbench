SELECT
  SUBSTRING(a0.c0, 1, 20),
  (
    a0.c5 / a0.c6
  ),
  CAST((
    a0.c3 / a0.c4
  ) AS DECIMAL(7, 2)),
  CAST((
    a0.c1 / a0.c2
  ) AS DECIMAL(7, 2))
FROM (
  SELECT
    a7.r_reason_desc AS c0,
    SUM(a3.wr_fee) AS c1,
    COUNT(a3.wr_fee) AS c2,
    SUM(a3.wr_refunded_cash) AS c3,
    COUNT(a3.wr_refunded_cash) AS c4,
    SUM(CAST(a1.ws_quantity AS DOUBLE)) AS c5,
    COUNT(CAST(a1.ws_quantity AS DOUBLE)) AS c6
  FROM (
    (
      (
        web_sales AS a1
          INNER JOIN date_dim AS a2
            ON (
              (
                a1.ws_sold_date_sk = a2.d_date_sk
              )
              AND (
                (
                  (
                    a1.ws_net_profit >= 100
                  ) AND (
                    a1.ws_net_profit <= 200
                  )
                )
                OR (
                  (
                    (
                      a1.ws_net_profit >= 150
                    ) AND (
                      a1.ws_net_profit <= 300
                    )
                  )
                  OR (
                    (
                      a1.ws_net_profit >= 50
                    ) AND (
                      a1.ws_net_profit <= 250
                    )
                  )
                )
              )
              AND (
                (
                  (
                    a1.ws_sales_price >= 100.00
                  ) AND (
                    a1.ws_sales_price <= 150.00
                  )
                )
                OR (
                  (
                    (
                      a1.ws_sales_price >= 50.00
                    ) AND (
                      a1.ws_sales_price <= 100.00
                    )
                  )
                  OR (
                    (
                      a1.ws_sales_price >= 150.00
                    ) AND (
                      a1.ws_sales_price <= 200.00
                    )
                  )
                )
              )
              AND (
                NOT a1.ws_web_page_sk IS NULL
              )
              AND (
                (
                  a1.ws_sold_date_sk <= 2451910
                ) AND (
                  a1.ws_sold_date_sk >= 2451545
                )
              )
              AND (
                a2.d_year = 2000
              )
            )
      )
      INNER JOIN (
        (
          (
            web_returns AS a3
              INNER JOIN customer_address AS a4
                ON (
                  (
                    a4.ca_address_sk = a3.wr_refunded_addr_sk
                  )
                  AND (
                    (
                      a3.wr_refunded_addr_sk <= 32500000
                    ) AND (
                      a3.wr_refunded_addr_sk >= 2
                    )
                  )
                  AND (
                    (
                      a3.wr_returning_cdemo_sk <= 1920788
                    )
                    AND (
                      a3.wr_returning_cdemo_sk >= 21
                    )
                  )
                  AND (
                    (
                      a3.wr_refunded_cdemo_sk <= 1920788
                    ) AND (
                      a3.wr_refunded_cdemo_sk >= 21
                    )
                  )
                  AND (
                    a4.ca_state IN ('IN', 'OH', 'NJ', 'WI', 'CT', 'KY', 'LA', 'IA', 'AR')
                  )
                  AND (
                    a4.ca_country = 'United States'
                  )
                )
          )
          INNER JOIN customer_demographics AS a5
            ON (
              (
                a5.cd_demo_sk = a3.wr_refunded_cdemo_sk
              )
              AND (
                a5.cd_marital_status IN ('M', 'S', 'W')
              )
              AND (
                a5.cd_education_status IN ('Advanced Degree', 'College', '2 yr Degree')
              )
            )
        )
        INNER JOIN customer_demographics AS a6
          ON (
            (
              a6.cd_demo_sk = a3.wr_returning_cdemo_sk
            )
            AND (
              a5.cd_marital_status = a6.cd_marital_status
            )
            AND (
              a5.cd_education_status = a6.cd_education_status
            )
            AND (
              a6.cd_education_status IN ('Advanced Degree', 'College', '2 yr Degree')
            )
            AND (
              a6.cd_marital_status IN ('M', 'S', 'W')
            )
          )
      )
        ON (
          a1.ws_item_sk = a3.wr_item_sk
        )
        AND (
          a1.ws_order_number = a3.wr_order_number
        )
        AND (
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
                  a1.ws_sales_price >= 100.00
                ) AND (
                  a1.ws_sales_price <= 150.00
                )
              )
            )
            OR (
              (
                (
                  a5.cd_marital_status = 'S'
                ) AND (
                  a5.cd_education_status = 'College'
                )
              )
              AND (
                (
                  a1.ws_sales_price >= 50.00
                ) AND (
                  a1.ws_sales_price <= 100.00
                )
              )
            )
          )
          OR (
            (
              (
                a5.cd_marital_status = 'W'
              ) AND (
                a5.cd_education_status = '2 yr Degree'
              )
            )
            AND (
              (
                a1.ws_sales_price >= 150.00
              ) AND (
                a1.ws_sales_price <= 200.00
              )
            )
          )
        )
        AND (
          (
            (
              (
                a4.ca_state IN ('IN', 'OH', 'NJ')
              )
              AND (
                (
                  a1.ws_net_profit >= 100
                ) AND (
                  a1.ws_net_profit <= 200
                )
              )
            )
            OR (
              (
                a4.ca_state IN ('WI', 'CT', 'KY')
              )
              AND (
                (
                  a1.ws_net_profit >= 150
                ) AND (
                  a1.ws_net_profit <= 300
                )
              )
            )
          )
          OR (
            (
              a4.ca_state IN ('LA', 'IA', 'AR')
            )
            AND (
              (
                a1.ws_net_profit >= 50
              ) AND (
                a1.ws_net_profit <= 250
              )
            )
          )
        )
    )
    INNER JOIN reason AS a7
      ON (
        a7.r_reason_sk = a3.wr_reason_sk
      )
  )
  GROUP BY
    a7.r_reason_desc
) AS a0
ORDER BY
  1 ASC NULLS LAST,
  2 ASC NULLS LAST,
  3 ASC NULLS LAST,
  4 ASC NULLS LAST
LIMIT 100