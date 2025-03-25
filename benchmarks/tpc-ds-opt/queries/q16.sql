SELECT
  COUNT(a0.c0) AS `order count`,
  SUM(a0.c1) AS `total shipping cost`,
  SUM(a0.c2) AS `total net profit`
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
      catalog_returns AS a2
        RIGHT OUTER JOIN (
          SELECT
            a5.cs_item_sk AS c0,
            a5.cs_net_profit AS c1,
            a5.cs_ext_ship_cost AS c2,
            a5.cs_order_number AS c3,
            a8.cc_call_center_sk AS c4,
            a6.ca_address_sk AS c5,
            a7.d_date_sk AS c6
          FROM (
            catalog_sales AS a4
              INNER JOIN (
                (
                  (
                    catalog_sales AS a5
                      INNER JOIN customer_address AS a6
                        ON (
                          (
                            a5.cs_ship_addr_sk = a6.ca_address_sk
                          )
                          AND (
                            (
                              a5.cs_ship_date_sk <= 2452367
                            ) AND (
                              a5.cs_ship_date_sk >= 2452307
                            )
                          )
                          AND (
                            (
                              a5.cs_ship_addr_sk <= 32499983
                            ) AND (
                              a5.cs_ship_addr_sk >= 21
                            )
                          )
                          AND (
                            (
                              a5.cs_call_center_sk <= 18
                            ) AND (
                              a5.cs_call_center_sk >= 16
                            )
                          )
                          AND (
                            a6.ca_state = 'GA'
                          )
                        )
                  )
                  INNER JOIN date_dim AS a7
                    ON (
                      (
                        a5.cs_ship_date_sk = a7.d_date_sk
                      )
                      AND (
                        a7.d_date <= CAST('2002-04-02' AS DATE)
                      )
                      AND (
                        CAST('2002-02-01' AS DATE) <= a7.d_date
                      )
                    )
                )
                INNER JOIN call_center AS a8
                  ON (
                    (
                      a5.cs_call_center_sk = a8.cc_call_center_sk
                    )
                    AND (
                      a8.cc_county = 'Williamson County'
                    )
                  )
              )
                ON (
                  a5.cs_order_number = a4.cs_order_number
                )
                AND (
                  a5.cs_warehouse_sk <> a4.cs_warehouse_sk
                )
          )
        ) AS a3
          ON (
            a3.c3 = a2.cr_order_number
          )
    )
    WHERE
      (
        a2.cr_order_number IS NULL
      )
  ) AS a1
  GROUP BY
    a1.c0
) AS a0
ORDER BY
  1 ASC NULLS LAST
LIMIT 100