SELECT
  a0.c0 AS CHANNEL,
  a0.c1 AS ID,
  a0.c2 AS SALES,
  a0.c3 AS RETURNS,
  a0.c4 AS PROFIT
FROM (
  (
    SELECT
      a1.c0 AS c0,
      a1.c1 AS c1,
      SUM(a1.c2) AS c2,
      SUM(a1.c3) AS c3,
      SUM(a1.c4) AS c4,
      a1.c5 AS c5
    FROM (
      SELECT
        CASE WHEN (
          a34.c0 < 3
        ) THEN a2.c0 ELSE NULL END AS c0,
        CASE WHEN (
          a34.c0 < 2
        ) THEN a2.c1 ELSE NULL END AS c1,
        a2.c2 AS c2,
        a2.c3 AS c3,
        a2.c4 AS c4,
        a34.c0 AS c5
      FROM (
        (
          SELECT
            a3.c0 AS c0,
            a3.c1 AS c1,
            SUM(a3.c2) AS c2,
            SUM(a3.c3) AS c3,
            SUM(a3.c4) AS c4
          FROM (
            (
              SELECT
                CAST('store channel' AS STRING) AS c0,
                CAST(CONCAT('store', a4.c0) AS STRING) AS c1,
                a4.c1 AS c2,
                a4.c2 AS c3,
                a4.c3 AS c4
              FROM (
                SELECT
                  a8.c4 AS c0,
                  SUM(a8.c1) AS c1,
                  SUM(COALESCE(CAST(a5.c2 AS DECIMAL(13, 2)), 00000000000.00)) AS c2,
                  SUM((
                    a8.c0 - COALESCE(CAST(a5.c3 AS DECIMAL(13, 2)), 00000000000.00)
                  )) AS c3
                FROM (
                  (
                    SELECT
                      a6.sr_item_sk AS c0,
                      a6.sr_ticket_number AS c1,
                      a6.sr_return_amt AS c2,
                      a6.sr_net_loss AS c3
                    FROM (
                      store_returns AS a6
                        INNER JOIN item AS a7
                          ON (
                            (
                              a7.i_item_sk = a6.sr_item_sk
                            )
                            AND (
                              (
                                a6.sr_item_sk <= 401996
                              ) AND (
                                a6.sr_item_sk >= 4
                              )
                            )
                            AND (
                              50 < a7.i_current_price
                            )
                          )
                    )
                  ) AS a5
                  RIGHT OUTER JOIN (
                    SELECT
                      a9.ss_net_profit AS c0,
                      a9.ss_ext_sales_price AS c1,
                      a9.ss_ticket_number AS c2,
                      a9.ss_item_sk AS c3,
                      a13.s_store_id AS c4
                    FROM (
                      (
                        (
                          (
                            store_sales AS a9
                              INNER JOIN date_dim AS a10
                                ON (
                                  (
                                    a10.d_date_sk = a9.ss_sold_date_sk
                                  )
                                  AND (
                                    (
                                      a9.ss_promo_sk <= 2000
                                    ) AND (
                                      a9.ss_promo_sk >= 1
                                    )
                                  )
                                  AND (
                                    (
                                      a9.ss_item_sk <= 401996
                                    ) AND (
                                      a9.ss_item_sk >= 4
                                    )
                                  )
                                  AND (
                                    (
                                      a9.ss_sold_date_sk <= 2451810
                                    ) AND (
                                      a9.ss_sold_date_sk >= 2451780
                                    )
                                  )
                                  AND (
                                    CAST('2000-08-23' AS DATE) <= a10.d_date
                                  )
                                  AND (
                                    a10.d_date <= CAST('2000-09-22' AS DATE)
                                  )
                                )
                          )
                          INNER JOIN promotion AS a11
                            ON (
                              (
                                a11.p_promo_sk = a9.ss_promo_sk
                              ) AND (
                                'N' = a11.p_channel_tv
                              )
                            )
                        )
                        INNER JOIN item AS a12
                          ON (
                            (
                              a12.i_item_sk = a9.ss_item_sk
                            ) AND (
                              50 < a12.i_current_price
                            )
                          )
                      )
                      INNER JOIN store AS a13
                        ON (
                          a13.s_store_sk = a9.ss_store_sk
                        )
                    )
                  ) AS a8
                    ON (
                      a8.c3 = a5.c0
                    ) AND (
                      a8.c2 = a5.c1
                    )
                )
                GROUP BY
                  a8.c4
              ) AS a4
            )
            UNION ALL
            (
              SELECT
                'catalog channel' AS c0,
                CONCAT('catalog_page', a14.c0) AS c1,
                a14.c1 AS c2,
                a14.c2 AS c3,
                a14.c3 AS c4
              FROM (
                SELECT
                  a18.c4 AS c0,
                  SUM(a18.c1) AS c1,
                  SUM(COALESCE(CAST(a15.c2 AS DECIMAL(13, 2)), 00000000000.00)) AS c2,
                  SUM((
                    a18.c0 - COALESCE(CAST(a15.c3 AS DECIMAL(13, 2)), 00000000000.00)
                  )) AS c3
                FROM (
                  (
                    SELECT
                      a16.cr_item_sk AS c0,
                      a16.cr_order_number AS c1,
                      a16.cr_return_amount AS c2,
                      a16.cr_net_loss AS c3
                    FROM (
                      catalog_returns AS a16
                        INNER JOIN item AS a17
                          ON (
                            (
                              a17.i_item_sk = a16.cr_item_sk
                            )
                            AND (
                              (
                                a16.cr_item_sk <= 401996
                              ) AND (
                                a16.cr_item_sk >= 4
                              )
                            )
                            AND (
                              50 < a17.i_current_price
                            )
                          )
                    )
                  ) AS a15
                  RIGHT OUTER JOIN (
                    SELECT
                      a19.cs_net_profit AS c0,
                      a19.cs_ext_sales_price AS c1,
                      a19.cs_order_number AS c2,
                      a19.cs_item_sk AS c3,
                      a21.cp_catalog_page_id AS c4
                    FROM (
                      (
                        (
                          (
                            catalog_sales AS a19
                              INNER JOIN date_dim AS a20
                                ON (
                                  (
                                    a20.d_date_sk = a19.cs_sold_date_sk
                                  )
                                  AND (
                                    (
                                      a19.cs_promo_sk <= 2000
                                    ) AND (
                                      a19.cs_promo_sk >= 1
                                    )
                                  )
                                  AND (
                                    (
                                      a19.cs_item_sk <= 401996
                                    ) AND (
                                      a19.cs_item_sk >= 4
                                    )
                                  )
                                  AND (
                                    (
                                      a19.cs_sold_date_sk <= 2451810
                                    ) AND (
                                      a19.cs_sold_date_sk >= 2451780
                                    )
                                  )
                                  AND (
                                    CAST('2000-08-23' AS DATE) <= a20.d_date
                                  )
                                  AND (
                                    a20.d_date <= CAST('2000-09-22' AS DATE)
                                  )
                                )
                          )
                          INNER JOIN catalog_page AS a21
                            ON (
                              a21.cp_catalog_page_sk = a19.cs_catalog_page_sk
                            )
                        )
                        INNER JOIN promotion AS a22
                          ON (
                            (
                              a22.p_promo_sk = a19.cs_promo_sk
                            ) AND (
                              'N' = a22.p_channel_tv
                            )
                          )
                      )
                      INNER JOIN item AS a23
                        ON (
                          (
                            a23.i_item_sk = a19.cs_item_sk
                          ) AND (
                            50 < a23.i_current_price
                          )
                        )
                    )
                  ) AS a18
                    ON (
                      a18.c3 = a15.c0
                    ) AND (
                      a18.c2 = a15.c1
                    )
                )
                GROUP BY
                  a18.c4
              ) AS a14
            )
            UNION ALL
            (
              SELECT
                CAST('web channel' AS STRING) AS c0,
                CAST(CONCAT('web_site', a24.c0) AS STRING) AS c1,
                a24.c1 AS c2,
                a24.c2 AS c3,
                a24.c3 AS c4
              FROM (
                SELECT
                  a28.c4 AS c0,
                  SUM(a28.c1) AS c1,
                  SUM(COALESCE(CAST(a25.c2 AS DECIMAL(13, 2)), 00000000000.00)) AS c2,
                  SUM((
                    a28.c0 - COALESCE(CAST(a25.c3 AS DECIMAL(13, 2)), 00000000000.00)
                  )) AS c3
                FROM (
                  (
                    SELECT
                      a26.wr_item_sk AS c0,
                      a26.wr_order_number AS c1,
                      a26.wr_return_amt AS c2,
                      a26.wr_net_loss AS c3
                    FROM (
                      web_returns AS a26
                        INNER JOIN item AS a27
                          ON (
                            (
                              a27.i_item_sk = a26.wr_item_sk
                            )
                            AND (
                              (
                                a26.wr_item_sk <= 401996
                              ) AND (
                                a26.wr_item_sk >= 4
                              )
                            )
                            AND (
                              50 < a27.i_current_price
                            )
                          )
                    )
                  ) AS a25
                  RIGHT OUTER JOIN (
                    SELECT
                      a29.ws_net_profit AS c0,
                      a29.ws_ext_sales_price AS c1,
                      a29.ws_order_number AS c2,
                      a29.ws_item_sk AS c3,
                      a31.web_site_id AS c4
                    FROM (
                      (
                        (
                          (
                            web_sales AS a29
                              INNER JOIN date_dim AS a30
                                ON (
                                  (
                                    a30.d_date_sk = a29.ws_sold_date_sk
                                  )
                                  AND (
                                    (
                                      a29.ws_promo_sk <= 2000
                                    ) AND (
                                      a29.ws_promo_sk >= 1
                                    )
                                  )
                                  AND (
                                    (
                                      a29.ws_item_sk <= 401996
                                    ) AND (
                                      a29.ws_item_sk >= 4
                                    )
                                  )
                                  AND (
                                    (
                                      a29.ws_sold_date_sk <= 2451810
                                    ) AND (
                                      a29.ws_sold_date_sk >= 2451780
                                    )
                                  )
                                  AND (
                                    CAST('2000-08-23' AS DATE) <= a30.d_date
                                  )
                                  AND (
                                    a30.d_date <= CAST('2000-09-22' AS DATE)
                                  )
                                )
                          )
                          INNER JOIN web_site AS a31
                            ON (
                              a31.web_site_sk = a29.ws_web_site_sk
                            )
                        )
                        INNER JOIN promotion AS a32
                          ON (
                            (
                              a32.p_promo_sk = a29.ws_promo_sk
                            ) AND (
                              'N' = a32.p_channel_tv
                            )
                          )
                      )
                      INNER JOIN item AS a33
                        ON (
                          (
                            a33.i_item_sk = a29.ws_item_sk
                          ) AND (
                            50 < a33.i_current_price
                          )
                        )
                    )
                  ) AS a28
                    ON (
                      a28.c3 = a25.c0
                    ) AND (
                      a28.c2 = a25.c1
                    )
                )
                GROUP BY
                  a28.c4
              ) AS a24
            )
          ) AS a3
          GROUP BY
            a3.c0,
            a3.c1
        ) AS a2
        INNER JOIN VALUES
          (1),
          (2),
          (3) AS a34(c0)
          ON (
            1 = 1
          )
      )
    ) AS a1
    GROUP BY
      a1.c5,
      a1.c0,
      a1.c1
  ) AS a0
  RIGHT OUTER JOIN VALUES
    (1),
    (2),
    (3) AS a35(c0)
    ON (
      a35.c0 = a0.c5
    )
)
WHERE
  (
    (
      (
        a35.c0 = 3
      ) AND (
        a0.c5 IS NULL
      )
    ) OR (
      NOT a0.c5 IS NULL
    )
  )
ORDER BY
  1 ASC NULLS LAST,
  2 ASC NULLS LAST
LIMIT 100