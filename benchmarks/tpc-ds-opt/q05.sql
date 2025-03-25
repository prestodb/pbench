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
          a32.c0 < 3
        ) THEN a2.c0 ELSE NULL END AS c0,
        CASE WHEN (
          a32.c0 < 2
        ) THEN a2.c1 ELSE NULL END AS c1,
        a2.c2 AS c2,
        a2.c3 AS c3,
        a2.c4 AS c4,
        a32.c0 AS c5
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
                a4.c3 AS c3,
                (
                  a4.c2 - a4.c4
                ) AS c4
              FROM (
                SELECT
                  a5.c0 AS c0,
                  SUM(a5.c4) AS c1,
                  SUM(a5.c3) AS c2,
                  SUM(a5.c2) AS c3,
                  SUM(a5.c1) AS c4
                FROM (
                  (
                    SELECT
                      a9.s_store_id AS c0,
                      SUM(a8.c3) AS c1,
                      SUM(a8.c2) AS c2,
                      SUM(a8.c1) AS c3,
                      SUM(a8.c0) AS c4
                    FROM (
                      (
                        SELECT
                          SUM(00000.00) AS c0,
                          SUM(00000.00) AS c1,
                          SUM(a6.sr_return_amt) AS c2,
                          SUM(a6.sr_net_loss) AS c3,
                          a6.sr_store_sk AS c4
                        FROM (
                          store_returns AS a6
                            INNER JOIN date_dim AS a7
                              ON (
                                (
                                  a6.sr_returned_date_sk = a7.d_date_sk
                                )
                                AND (
                                  (
                                    a6.sr_returned_date_sk <= 2451794
                                  )
                                  AND (
                                    a6.sr_returned_date_sk >= 2451780
                                  )
                                )
                                AND (
                                  CAST('2000-08-23' AS DATE) <= a7.d_date
                                )
                                AND (
                                  a7.d_date <= CAST('2000-09-06' AS DATE)
                                )
                              )
                        )
                        GROUP BY
                          a6.sr_store_sk
                      ) AS a8
                      INNER JOIN store AS a9
                        ON (
                          a8.c4 = a9.s_store_sk
                        )
                    )
                    GROUP BY
                      a9.s_store_id
                  )
                  UNION ALL
                  (
                    SELECT
                      a13.s_store_id AS c0,
                      SUM(a12.c3) AS c1,
                      SUM(a12.c2) AS c2,
                      SUM(a12.c1) AS c3,
                      SUM(a12.c0) AS c4
                    FROM (
                      (
                        SELECT
                          SUM(a10.ss_ext_sales_price) AS c0,
                          SUM(a10.ss_net_profit) AS c1,
                          SUM(00000.00) AS c2,
                          SUM(00000.00) AS c3,
                          a10.ss_store_sk AS c4
                        FROM (
                          store_sales AS a10
                            INNER JOIN date_dim AS a11
                              ON (
                                (
                                  a10.ss_sold_date_sk = a11.d_date_sk
                                )
                                AND (
                                  (
                                    a10.ss_sold_date_sk <= 2451794
                                  ) AND (
                                    a10.ss_sold_date_sk >= 2451780
                                  )
                                )
                                AND (
                                  CAST('2000-08-23' AS DATE) <= a11.d_date
                                )
                                AND (
                                  a11.d_date <= CAST('2000-09-06' AS DATE)
                                )
                              )
                        )
                        GROUP BY
                          a10.ss_store_sk
                      ) AS a12
                      INNER JOIN store AS a13
                        ON (
                          a12.c4 = a13.s_store_sk
                        )
                    )
                    GROUP BY
                      a13.s_store_id
                  )
                ) AS a5
                GROUP BY
                  a5.c0
              ) AS a4
            )
            UNION ALL
            (
              SELECT
                'catalog channel' AS c0,
                CONCAT('catalog_page', a14.c0) AS c1,
                a14.c1 AS c2,
                a14.c3 AS c3,
                (
                  a14.c2 - a14.c4
                ) AS c4
              FROM (
                SELECT
                  a15.c0 AS c0,
                  SUM(a15.c4) AS c1,
                  SUM(a15.c3) AS c2,
                  SUM(a15.c2) AS c3,
                  SUM(a15.c1) AS c4
                FROM (
                  (
                    SELECT
                      a18.cp_catalog_page_id AS c0,
                      SUM(a16.cr_net_loss) AS c1,
                      SUM(a16.cr_return_amount) AS c2,
                      SUM(00000.00) AS c3,
                      SUM(00000.00) AS c4
                    FROM (
                      (
                        catalog_returns AS a16
                          INNER JOIN date_dim AS a17
                            ON (
                              (
                                a16.cr_returned_date_sk = a17.d_date_sk
                              )
                              AND (
                                (
                                  a16.cr_returned_date_sk <= 2451794
                                )
                                AND (
                                  a16.cr_returned_date_sk >= 2451780
                                )
                              )
                              AND (
                                CAST('2000-08-23' AS DATE) <= a17.d_date
                              )
                              AND (
                                a17.d_date <= CAST('2000-09-06' AS DATE)
                              )
                            )
                      )
                      INNER JOIN catalog_page AS a18
                        ON (
                          a16.cr_catalog_page_sk = a18.cp_catalog_page_sk
                        )
                    )
                    GROUP BY
                      a18.cp_catalog_page_id
                  )
                  UNION ALL
                  (
                    SELECT
                      a21.cp_catalog_page_id AS c0,
                      SUM(00000.00) AS c1,
                      SUM(00000.00) AS c2,
                      SUM(a19.cs_net_profit) AS c3,
                      SUM(a19.cs_ext_sales_price) AS c4
                    FROM (
                      (
                        catalog_sales AS a19
                          INNER JOIN date_dim AS a20
                            ON (
                              (
                                a19.cs_sold_date_sk = a20.d_date_sk
                              )
                              AND (
                                (
                                  a19.cs_sold_date_sk <= 2451794
                                ) AND (
                                  a19.cs_sold_date_sk >= 2451780
                                )
                              )
                              AND (
                                CAST('2000-08-23' AS DATE) <= a20.d_date
                              )
                              AND (
                                a20.d_date <= CAST('2000-09-06' AS DATE)
                              )
                            )
                      )
                      INNER JOIN catalog_page AS a21
                        ON (
                          a19.cs_catalog_page_sk = a21.cp_catalog_page_sk
                        )
                    )
                    GROUP BY
                      a21.cp_catalog_page_id
                  )
                ) AS a15
                GROUP BY
                  a15.c0
              ) AS a14
            )
            UNION ALL
            (
              SELECT
                CAST('web channel' AS STRING) AS c0,
                CAST(CONCAT('web_site', a22.c0) AS STRING) AS c1,
                a22.c1 AS c2,
                a22.c3 AS c3,
                (
                  a22.c2 - a22.c4
                ) AS c4
              FROM (
                SELECT
                  a23.c0 AS c0,
                  SUM(a23.c4) AS c1,
                  SUM(a23.c3) AS c2,
                  SUM(a23.c2) AS c3,
                  SUM(a23.c1) AS c4
                FROM (
                  (
                    SELECT
                      a27.web_site_id AS c0,
                      SUM(a25.wr_net_loss) AS c1,
                      SUM(a25.wr_return_amt) AS c2,
                      SUM(00000.00) AS c3,
                      SUM(00000.00) AS c4
                    FROM (
                      (
                        web_sales AS a24
                          INNER JOIN (
                            web_returns AS a25
                              INNER JOIN date_dim AS a26
                                ON (
                                  (
                                    a25.wr_returned_date_sk = a26.d_date_sk
                                  )
                                  AND (
                                    (
                                      a25.wr_returned_date_sk <= 2451794
                                    )
                                    AND (
                                      a25.wr_returned_date_sk >= 2451780
                                    )
                                  )
                                  AND (
                                    CAST('2000-08-23' AS DATE) <= a26.d_date
                                  )
                                  AND (
                                    a26.d_date <= CAST('2000-09-06' AS DATE)
                                  )
                                )
                          )
                            ON (
                              a25.wr_order_number = a24.ws_order_number
                            )
                            AND (
                              a25.wr_item_sk = a24.ws_item_sk
                            )
                      )
                      INNER JOIN web_site AS a27
                        ON (
                          a24.ws_web_site_sk = a27.web_site_sk
                        )
                    )
                    GROUP BY
                      a27.web_site_id
                  )
                  UNION ALL
                  (
                    SELECT
                      a31.web_site_id AS c0,
                      SUM(a30.c3) AS c1,
                      SUM(a30.c2) AS c2,
                      SUM(a30.c1) AS c3,
                      SUM(a30.c0) AS c4
                    FROM (
                      (
                        SELECT
                          SUM(a28.ws_ext_sales_price) AS c0,
                          SUM(a28.ws_net_profit) AS c1,
                          SUM(00000.00) AS c2,
                          SUM(00000.00) AS c3,
                          a28.ws_web_site_sk AS c4
                        FROM (
                          web_sales AS a28
                            INNER JOIN date_dim AS a29
                              ON (
                                (
                                  a28.ws_sold_date_sk = a29.d_date_sk
                                )
                                AND (
                                  (
                                    a28.ws_sold_date_sk <= 2451794
                                  ) AND (
                                    a28.ws_sold_date_sk >= 2451780
                                  )
                                )
                                AND (
                                  CAST('2000-08-23' AS DATE) <= a29.d_date
                                )
                                AND (
                                  a29.d_date <= CAST('2000-09-06' AS DATE)
                                )
                              )
                        )
                        GROUP BY
                          a28.ws_web_site_sk
                      ) AS a30
                      INNER JOIN web_site AS a31
                        ON (
                          a30.c4 = a31.web_site_sk
                        )
                    )
                    GROUP BY
                      a31.web_site_id
                  )
                ) AS a23
                GROUP BY
                  a23.c0
              ) AS a22
            )
          ) AS a3
          GROUP BY
            a3.c0,
            a3.c1
        ) AS a2
        INNER JOIN VALUES
          (1),
          (2),
          (3) AS a32(c0)
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
    (3) AS a33(c0)
    ON (
      a33.c0 = a0.c5
    )
)
WHERE
  (
    (
      (
        a33.c0 = 3
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