SELECT
  2001 AS PREV_YEAR,
  2002 AS YEAR,
  a0.c0 AS I_BRAND_ID,
  a0.c1 AS I_CLASS_ID,
  a0.c2 AS I_CATEGORY_ID,
  a0.c3 AS I_MANUFACT_ID,
  a0.c5 AS PREV_YR_CNT,
  a0.c8 AS CURR_YR_CNT,
  (
    a0.c8 - a0.c5
  ) AS SALES_CNT_DIFF,
  (
    a0.c7 - a0.c4
  ) AS SALES_AMT_DIFF
FROM (
  SELECT
    a1.c0 AS c0,
    a1.c1 AS c1,
    a1.c2 AS c2,
    a1.c3 AS c3,
    MAX(a1.c4) AS c4,
    MAX(a1.c5) AS c5,
    MAX(a1.c6) AS c6,
    MAX(a1.c7) AS c7,
    MAX(a1.c8) AS c8,
    MAX(a1.c9) AS c9
  FROM (
    SELECT
      a2.c1 AS c0,
      a2.c2 AS c1,
      a2.c3 AS c2,
      a2.c4 AS c3,
      CASE WHEN (
        a2.c0 = 2001
      ) THEN a2.c6 ELSE NULL END AS c4,
      CASE WHEN (
        a2.c0 = 2001
      ) THEN a2.c5 ELSE NULL END AS c5,
      CASE WHEN (
        a2.c0 = 2001
      ) THEN a2.c0 ELSE NULL END AS c6,
      CASE WHEN (
        a2.c0 = 2002
      ) THEN a2.c6 ELSE NULL END AS c7,
      CASE WHEN (
        a2.c0 = 2002
      ) THEN a2.c5 ELSE NULL END AS c8,
      CASE WHEN (
        a2.c0 = 2002
      ) THEN a2.c0 ELSE NULL END AS c9
    FROM (
      SELECT
        a3.c0 AS c0,
        a3.c1 AS c1,
        a3.c2 AS c2,
        a3.c3 AS c3,
        a3.c4 AS c4,
        SUM(a3.c5) AS c5,
        SUM(a3.c6) AS c6
      FROM (
        SELECT DISTINCT
          a4.c0 AS c0,
          a4.c1 AS c1,
          a4.c2 AS c2,
          a4.c3 AS c3,
          a4.c4 AS c4,
          a4.c5 AS c5,
          a4.c6 AS c6
        FROM (
          (
            SELECT
              a5.c4 AS c0,
              a5.c8 AS c1,
              a5.c7 AS c2,
              a5.c6 AS c3,
              a5.c5 AS c4,
              (
                a5.c1 - COALESCE(a9.c2, 0)
              ) AS c5,
              (
                a5.c0 - COALESCE(a9.c3, 00000.00)
              ) AS c6
            FROM (
              (
                SELECT
                  a6.cs_ext_sales_price AS c0,
                  a6.cs_quantity AS c1,
                  a6.cs_order_number AS c2,
                  a6.cs_item_sk AS c3,
                  a7.d_year AS c4,
                  a8.i_manufact_id AS c5,
                  a8.i_category_id AS c6,
                  a8.i_class_id AS c7,
                  a8.i_brand_id AS c8
                FROM (
                  (
                    catalog_sales AS a6
                      INNER JOIN date_dim AS a7
                        ON (
                          (
                            a7.d_date_sk = a6.cs_sold_date_sk
                          )
                          AND (
                            (
                              a6.cs_item_sk <= 401999
                            ) AND (
                              a6.cs_item_sk >= 7
                            )
                          )
                          AND (
                            (
                              a6.cs_sold_date_sk <= 2452640
                            ) AND (
                              a6.cs_sold_date_sk >= 2451911
                            )
                          )
                          AND (
                            a7.d_year IN (2001, 2002)
                          )
                        )
                  )
                  INNER JOIN item AS a8
                    ON (
                      (
                        a8.i_item_sk = a6.cs_item_sk
                      )
                      AND (
                        NOT a8.i_manufact_id IS NULL
                      )
                      AND (
                        NOT a8.i_category_id IS NULL
                      )
                      AND (
                        NOT a8.i_class_id IS NULL
                      )
                      AND (
                        NOT a8.i_brand_id IS NULL
                      )
                      AND (
                        a8.i_category = 'Books'
                      )
                    )
                )
              ) AS a5
              LEFT OUTER JOIN (
                SELECT
                  a10.cr_item_sk AS c0,
                  a10.cr_order_number AS c1,
                  a10.cr_return_quantity AS c2,
                  a10.cr_return_amount AS c3
                FROM (
                  catalog_returns AS a10
                    INNER JOIN item AS a11
                      ON (
                        (
                          a11.i_item_sk = a10.cr_item_sk
                        )
                        AND (
                          (
                            a10.cr_item_sk <= 401999
                          ) AND (
                            a10.cr_item_sk >= 7
                          )
                        )
                        AND (
                          a11.i_category = 'Books'
                        )
                        AND (
                          NOT a11.i_brand_id IS NULL
                        )
                        AND (
                          NOT a11.i_class_id IS NULL
                        )
                        AND (
                          NOT a11.i_category_id IS NULL
                        )
                        AND (
                          NOT a11.i_manufact_id IS NULL
                        )
                      )
                )
              ) AS a9
                ON (
                  a5.c2 = a9.c1
                ) AND (
                  a5.c3 = a9.c0
                )
            )
            GROUP BY
              (
                a5.c0 - COALESCE(a9.c3, 00000.00)
              ),
              (
                a5.c1 - COALESCE(a9.c2, 0)
              ),
              a5.c5,
              a5.c6,
              a5.c7,
              a5.c8,
              a5.c4
          )
          UNION ALL
          (
            SELECT
              a12.c4 AS c0,
              a12.c8 AS c1,
              a12.c7 AS c2,
              a12.c6 AS c3,
              a12.c5 AS c4,
              (
                a12.c1 - COALESCE(a16.c2, 0)
              ) AS c5,
              (
                a12.c0 - COALESCE(a16.c3, 00000.00)
              ) AS c6
            FROM (
              (
                SELECT
                  a13.ss_ext_sales_price AS c0,
                  a13.ss_quantity AS c1,
                  a13.ss_ticket_number AS c2,
                  a13.ss_item_sk AS c3,
                  a14.d_year AS c4,
                  a15.i_manufact_id AS c5,
                  a15.i_category_id AS c6,
                  a15.i_class_id AS c7,
                  a15.i_brand_id AS c8
                FROM (
                  (
                    store_sales AS a13
                      INNER JOIN date_dim AS a14
                        ON (
                          (
                            a14.d_date_sk = a13.ss_sold_date_sk
                          )
                          AND (
                            (
                              a13.ss_item_sk <= 401999
                            ) AND (
                              a13.ss_item_sk >= 7
                            )
                          )
                          AND (
                            (
                              a13.ss_sold_date_sk <= 2452640
                            ) AND (
                              a13.ss_sold_date_sk >= 2451911
                            )
                          )
                          AND (
                            a14.d_year IN (2001, 2002)
                          )
                        )
                  )
                  INNER JOIN item AS a15
                    ON (
                      (
                        a15.i_item_sk = a13.ss_item_sk
                      )
                      AND (
                        NOT a15.i_manufact_id IS NULL
                      )
                      AND (
                        NOT a15.i_category_id IS NULL
                      )
                      AND (
                        NOT a15.i_class_id IS NULL
                      )
                      AND (
                        NOT a15.i_brand_id IS NULL
                      )
                      AND (
                        a15.i_category = 'Books'
                      )
                    )
                )
              ) AS a12
              LEFT OUTER JOIN (
                SELECT
                  a17.sr_item_sk AS c0,
                  a17.sr_ticket_number AS c1,
                  a17.sr_return_quantity AS c2,
                  a17.sr_return_amt AS c3
                FROM (
                  store_returns AS a17
                    INNER JOIN item AS a18
                      ON (
                        (
                          a18.i_item_sk = a17.sr_item_sk
                        )
                        AND (
                          (
                            a17.sr_item_sk <= 401999
                          ) AND (
                            a17.sr_item_sk >= 7
                          )
                        )
                        AND (
                          a18.i_category = 'Books'
                        )
                        AND (
                          NOT a18.i_brand_id IS NULL
                        )
                        AND (
                          NOT a18.i_class_id IS NULL
                        )
                        AND (
                          NOT a18.i_category_id IS NULL
                        )
                        AND (
                          NOT a18.i_manufact_id IS NULL
                        )
                      )
                )
              ) AS a16
                ON (
                  a12.c2 = a16.c1
                ) AND (
                  a12.c3 = a16.c0
                )
            )
            GROUP BY
              (
                a12.c0 - COALESCE(a16.c3, 00000.00)
              ),
              (
                a12.c1 - COALESCE(a16.c2, 0)
              ),
              a12.c5,
              a12.c6,
              a12.c7,
              a12.c8,
              a12.c4
          )
          UNION ALL
          (
            SELECT
              a19.c4 AS c0,
              a19.c8 AS c1,
              a19.c7 AS c2,
              a19.c6 AS c3,
              a19.c5 AS c4,
              (
                a19.c1 - COALESCE(a23.c2, 0)
              ) AS c5,
              (
                a19.c0 - COALESCE(a23.c3, 00000.00)
              ) AS c6
            FROM (
              (
                SELECT
                  a20.ws_ext_sales_price AS c0,
                  a20.ws_quantity AS c1,
                  a20.ws_order_number AS c2,
                  a20.ws_item_sk AS c3,
                  a21.d_year AS c4,
                  a22.i_manufact_id AS c5,
                  a22.i_category_id AS c6,
                  a22.i_class_id AS c7,
                  a22.i_brand_id AS c8
                FROM (
                  (
                    web_sales AS a20
                      INNER JOIN date_dim AS a21
                        ON (
                          (
                            a21.d_date_sk = a20.ws_sold_date_sk
                          )
                          AND (
                            (
                              a20.ws_item_sk <= 401999
                            ) AND (
                              a20.ws_item_sk >= 7
                            )
                          )
                          AND (
                            (
                              a20.ws_sold_date_sk <= 2452640
                            ) AND (
                              a20.ws_sold_date_sk >= 2451911
                            )
                          )
                          AND (
                            a21.d_year IN (2001, 2002)
                          )
                        )
                  )
                  INNER JOIN item AS a22
                    ON (
                      (
                        a22.i_item_sk = a20.ws_item_sk
                      )
                      AND (
                        NOT a22.i_manufact_id IS NULL
                      )
                      AND (
                        NOT a22.i_category_id IS NULL
                      )
                      AND (
                        NOT a22.i_class_id IS NULL
                      )
                      AND (
                        NOT a22.i_brand_id IS NULL
                      )
                      AND (
                        a22.i_category = 'Books'
                      )
                    )
                )
              ) AS a19
              LEFT OUTER JOIN (
                SELECT
                  a24.wr_item_sk AS c0,
                  a24.wr_order_number AS c1,
                  a24.wr_return_quantity AS c2,
                  a24.wr_return_amt AS c3
                FROM (
                  web_returns AS a24
                    INNER JOIN item AS a25
                      ON (
                        (
                          a25.i_item_sk = a24.wr_item_sk
                        )
                        AND (
                          (
                            a24.wr_item_sk <= 401999
                          ) AND (
                            a24.wr_item_sk >= 7
                          )
                        )
                        AND (
                          a25.i_category = 'Books'
                        )
                        AND (
                          NOT a25.i_brand_id IS NULL
                        )
                        AND (
                          NOT a25.i_class_id IS NULL
                        )
                        AND (
                          NOT a25.i_category_id IS NULL
                        )
                        AND (
                          NOT a25.i_manufact_id IS NULL
                        )
                      )
                )
              ) AS a23
                ON (
                  a19.c2 = a23.c1
                ) AND (
                  a19.c3 = a23.c0
                )
            )
            GROUP BY
              (
                a19.c0 - COALESCE(a23.c3, 00000.00)
              ),
              (
                a19.c1 - COALESCE(a23.c2, 0)
              ),
              a19.c5,
              a19.c6,
              a19.c7,
              a19.c8,
              a19.c4
          )
        ) AS a4
      ) AS a3
      GROUP BY
        a3.c0,
        a3.c1,
        a3.c2,
        a3.c3,
        a3.c4
    ) AS a2
  ) AS a1
  GROUP BY
    a1.c3,
    a1.c2,
    a1.c1,
    a1.c0
) AS a0
WHERE
  (
    (
      CAST(a0.c8 AS DECIMAL(17, 2)) / CAST(a0.c5 AS DECIMAL(17, 2))
    ) < 0.9
  )
  AND (
    a0.c9 = 2002
  )
  AND (
    a0.c6 = 2001
  )
ORDER BY
  9 ASC NULLS LAST
LIMIT 100