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
      a3.c3 AS c0,
      a3.c2 AS c1,
      a3.c1 AS c2,
      a3.c4 AS c3,
      a3.c5 AS c4,
      a3.c6 AS c5,
      a3.c0 AS c6
    FROM (
      web_returns AS a2
        RIGHT OUTER JOIN (
          SELECT
            a5.ws_item_sk AS c0,
            a5.ws_net_profit AS c1,
            a5.ws_ext_ship_cost AS c2,
            a5.ws_order_number AS c3,
            a8.web_site_sk AS c4,
            a6.ca_address_sk AS c5,
            a7.d_date_sk AS c6
          FROM (
            web_sales AS a4
              INNER JOIN (
                (
                  (
                    web_sales AS a5
                      INNER JOIN customer_address AS a6
                        ON (
                          (
                            a5.ws_ship_addr_sk = a6.ca_address_sk
                          )
                          AND (
                            (
                              a5.ws_ship_date_sk <= 2451271
                            ) AND (
                              a5.ws_ship_date_sk >= 2451211
                            )
                          )
                          AND (
                            (
                              a5.ws_ship_addr_sk <= 32499976
                            ) AND (
                              a5.ws_ship_addr_sk >= 54
                            )
                          )
                          AND (
                            (
                              a5.ws_web_site_sk <= 75
                            ) AND (
                              a5.ws_web_site_sk >= 1
                            )
                          )
                          AND (
                            a6.ca_state = 'IL'
                          )
                        )
                  )
                  INNER JOIN date_dim AS a7
                    ON (
                      (
                        a5.ws_ship_date_sk = a7.d_date_sk
                      )
                      AND (
                        a7.d_date <= CAST('1999-04-02' AS DATE)
                      )
                      AND (
                        CAST('1999-02-01' AS DATE) <= a7.d_date
                      )
                    )
                )
                INNER JOIN web_site AS a8
                  ON (
                    (
                      a5.ws_web_site_sk = a8.web_site_sk
                    ) AND (
                      a8.web_company_name = 'pri'
                    )
                  )
              )
                ON (
                  a5.ws_order_number = a4.ws_order_number
                )
                AND (
                  a5.ws_warehouse_sk <> a4.ws_warehouse_sk
                )
          )
        ) AS a3
          ON (
            a3.c3 = a2.wr_order_number
          )
    )
    WHERE
      (
        a2.wr_order_number IS NULL
      )
  ) AS a1
  GROUP BY
    a1.c0
) AS a0
ORDER BY
  1 ASC NULLS LAST
LIMIT 100