SELECT
  a0.c0 AS SEGMENT,
  a0.c1 AS NUM_CUSTOMERS,
  (
    a0.c0 * 50
  ) AS SEGMENT_BASE
FROM (
  SELECT
    CAST((
      a1.c0 / 50
    ) AS INT) AS c0,
    COUNT(*) AS c1
  FROM (
    SELECT
      SUM(a2.c1) AS c0
    FROM (
      SELECT
        a4.c0 AS c0,
        a3.ss_ext_sales_price AS c1
      FROM (
        (
          (
            store_sales AS a3
              INNER JOIN (
                SELECT DISTINCT
                  a5.c1 AS c0,
                  a5.c0 AS c1
                FROM (
                  (
                    SELECT
                      a6.c_current_addr_sk AS c0,
                      a6.c_customer_sk AS c1
                    FROM (
                      customer AS a6
                        INNER JOIN (
                          (
                            web_sales AS a7
                              INNER JOIN date_dim AS a8
                                ON (
                                  (
                                    a7.ws_sold_date_sk = a8.d_date_sk
                                  )
                                  AND (
                                    (
                                      a7.ws_sold_date_sk <= 2451179
                                    ) AND (
                                      a7.ws_sold_date_sk >= 2451149
                                    )
                                  )
                                  AND (
                                    (
                                      a7.ws_item_sk <= 401946
                                    ) AND (
                                      a7.ws_item_sk >= 66
                                    )
                                  )
                                  AND (
                                    a8.d_moy = 12
                                  )
                                  AND (
                                    a8.d_year = 1998
                                  )
                                )
                          )
                          INNER JOIN item AS a9
                            ON (
                              (
                                a7.ws_item_sk = a9.i_item_sk
                              )
                              AND (
                                a9.i_category = 'Women'
                              )
                              AND (
                                a9.i_class = 'maternity'
                              )
                            )
                        )
                          ON (
                            a6.c_customer_sk = a7.ws_bill_customer_sk
                          )
                    )
                  )
                  UNION ALL
                  (
                    SELECT
                      a10.c_current_addr_sk AS c0,
                      a10.c_customer_sk AS c1
                    FROM (
                      customer AS a10
                        INNER JOIN (
                          (
                            catalog_sales AS a11
                              INNER JOIN date_dim AS a12
                                ON (
                                  (
                                    a11.cs_sold_date_sk = a12.d_date_sk
                                  )
                                  AND (
                                    (
                                      a11.cs_sold_date_sk <= 2451179
                                    ) AND (
                                      a11.cs_sold_date_sk >= 2451149
                                    )
                                  )
                                  AND (
                                    (
                                      a11.cs_item_sk <= 401946
                                    ) AND (
                                      a11.cs_item_sk >= 66
                                    )
                                  )
                                  AND (
                                    a12.d_moy = 12
                                  )
                                  AND (
                                    a12.d_year = 1998
                                  )
                                )
                          )
                          INNER JOIN item AS a13
                            ON (
                              (
                                a11.cs_item_sk = a13.i_item_sk
                              )
                              AND (
                                a13.i_category = 'Women'
                              )
                              AND (
                                a13.i_class = 'maternity'
                              )
                            )
                        )
                          ON (
                            a10.c_customer_sk = a11.cs_bill_customer_sk
                          )
                    )
                  )
                ) AS a5
              ) AS a4
                ON (
                  (
                    a4.c0 = a3.ss_customer_sk
                  )
                  AND (
                    (
                      a3.ss_sold_date_sk <= 2451269
                    ) AND (
                      a3.ss_sold_date_sk >= 2451180
                    )
                  )
                )
          )
          INNER JOIN (
            customer_address AS a14
              INNER JOIN store AS a15
                ON (
                  a14.ca_county = a15.s_county
                ) AND (
                  a14.ca_state = a15.s_state
                )
          )
            ON (
              a4.c1 = a14.ca_address_sk
            )
        )
        INNER JOIN date_dim AS a16
          ON (
            (
              a3.ss_sold_date_sk = a16.d_date_sk
            )
            AND (
              (
                SELECT DISTINCT
                  (
                    a17.d_month_seq + 1
                  )
                FROM date_dim AS a17
                WHERE
                  (
                    a17.d_year = 1998
                  ) AND (
                    a17.d_moy = 12
                  )
              ) <= a16.d_month_seq
            )
            AND (
              a16.d_month_seq <= (
                SELECT DISTINCT
                  (
                    a18.d_month_seq + 3
                  )
                FROM date_dim AS a18
                WHERE
                  (
                    a18.d_year = 1998
                  ) AND (
                    a18.d_moy = 12
                  )
              )
            )
          )
      )
    ) AS a2
    GROUP BY
      a2.c0
  ) AS a1
  GROUP BY
    CAST((
      a1.c0 / 50
    ) AS INT)
) AS a0
ORDER BY
  1 ASC NULLS LAST
LIMIT 100