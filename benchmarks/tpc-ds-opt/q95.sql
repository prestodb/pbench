SELECT
  COUNT(a0.c0) AS `order_count`,
  SUM(a0.c1) AS `total_shipping_cost`,
  SUM(a0.c2) AS `total_net_profit`
FROM (
  SELECT
    a1.c0 AS c0,
    SUM(a1.c1) AS c1,
    SUM(a1.c2) AS c2
  FROM (
    SELECT DISTINCT
      a4.ws_order_number AS c0,
      a4.ws_ext_ship_cost AS c1,
      a4.ws_net_profit AS c2,
      a7.web_site_sk AS c3,
      a5.ca_address_sk AS c4,
      a6.d_date_sk AS c5,
      a4.ws_item_sk AS c6
    FROM (
      web_sales AS a2
        INNER JOIN (
          (
            web_sales AS a3
              INNER JOIN (
                (
                  (
                    web_sales AS a4
                      INNER JOIN customer_address AS a5
                        ON (
                          (
                            a4.ws_ship_addr_sk = a5.ca_address_sk
                          )
                          AND (
                            (
                              a4.ws_ship_date_sk <= 2451271
                            ) AND (
                              a4.ws_ship_date_sk >= 2451211
                            )
                          )
                          AND (
                            (
                              a4.ws_ship_addr_sk <= 32499976
                            ) AND (
                              a4.ws_ship_addr_sk >= 54
                            )
                          )
                          AND (
                            (
                              a4.ws_web_site_sk <= 75
                            ) AND (
                              a4.ws_web_site_sk >= 1
                            )
                          )
                          AND (
                            a5.ca_state = 'IL'
                          )
                        )
                  )
                  INNER JOIN date_dim AS a6
                    ON (
                      (
                        a4.ws_ship_date_sk = a6.d_date_sk
                      )
                      AND (
                        a6.d_date <= CAST('1999-04-02' AS DATE)
                      )
                      AND (
                        CAST('1999-02-01' AS DATE) <= a6.d_date
                      )
                    )
                )
                INNER JOIN web_site AS a7
                  ON (
                    (
                      a4.ws_web_site_sk = a7.web_site_sk
                    ) AND (
                      a7.web_company_name = 'pri'
                    )
                  )
              )
                ON (
                  a3.ws_order_number = a4.ws_order_number
                )
          )
          INNER JOIN (
            SELECT
              a9.wr_order_number AS c0
            FROM web_returns AS a9
            GROUP BY
              a9.wr_order_number
          ) AS a8
            ON (
              a4.ws_order_number = a8.c0
            )
        )
          ON (
            a4.ws_order_number = a2.ws_order_number
          )
          AND (
            a2.ws_warehouse_sk <> a3.ws_warehouse_sk
          )
    )
  ) AS a1
  GROUP BY
    a1.c0
) AS a0
ORDER BY
  1 ASC NULLS LAST
LIMIT 100