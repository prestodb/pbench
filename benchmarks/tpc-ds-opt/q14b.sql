WITH a4 AS (
  SELECT
    a5.c0 AS c0
  FROM (
    SELECT
      a23.i_item_sk AS c0
    FROM (
      (
        SELECT
          a7.c0 AS c0,
          a7.c1 AS c1,
          a7.c2 AS c2
        FROM (
          SELECT
            a8.c0 AS c0,
            a8.c1 AS c1,
            a8.c2 AS c2,
            SUM(a8.c3) AS c3,
            COUNT(*) AS c4
          FROM (
            (
              SELECT
                a12.i_brand_id AS c0,
                a12.i_class_id AS c1,
                a12.i_category_id AS c2,
                -1 AS c3
              FROM (
                (
                  SELECT
                    a9.ws_item_sk AS c0
                  FROM (
                    web_sales AS a9
                      INNER JOIN date_dim AS a10
                        ON (
                          (
                            a9.ws_sold_date_sk = a10.d_date_sk
                          )
                          AND (
                            (
                              a9.ws_sold_date_sk <= 2452275
                            ) AND (
                              a9.ws_sold_date_sk >= 2451180
                            )
                          )
                          AND (
                            1999 <= a10.d_year
                          )
                          AND (
                            a10.d_year <= 2001
                          )
                        )
                  )
                  GROUP BY
                    a9.ws_item_sk
                ) AS a11
                INNER JOIN item AS a12
                  ON (
                    a11.c0 = a12.i_item_sk
                  )
              )
              GROUP BY
                a12.i_brand_id,
                a12.i_class_id,
                a12.i_category_id

            )
            UNION ALL
            (
              SELECT
                a13.c0 AS c0,
                a13.c1 AS c1,
                a13.c2 AS c2,
                1 AS c3
              FROM (
                SELECT
                  a14.c0 AS c0,
                  a14.c1 AS c1,
                  a14.c2 AS c2,
                  SUM(a14.c3) AS c3,
                  COUNT(*) AS c4
                FROM (
                  (
                    SELECT
                      a18.i_brand_id AS c0,
                      a18.i_class_id AS c1,
                      a18.i_category_id AS c2,
                      -1 AS c3
                    FROM (
                      (
                        SELECT
                          a15.cs_item_sk AS c0
                        FROM (
                          catalog_sales AS a15
                            INNER JOIN date_dim AS a16
                              ON (
                                (
                                  a15.cs_sold_date_sk = a16.d_date_sk
                                )
                                AND (
                                  (
                                    a15.cs_sold_date_sk <= 2452275
                                  ) AND (
                                    a15.cs_sold_date_sk >= 2451180
                                  )
                                )
                                AND (
                                  1999 <= a16.d_year
                                )
                                AND (
                                  a16.d_year <= 2001
                                )
                              )
                        )
                        GROUP BY
                          a15.cs_item_sk
                      ) AS a17
                      INNER JOIN item AS a18
                        ON (
                          a17.c0 = a18.i_item_sk
                        )
                    )
                    GROUP BY
                      a18.i_brand_id,
                      a18.i_class_id,
                      a18.i_category_id
                  )
                  UNION ALL
                  (
                    SELECT
                      a22.i_brand_id AS c0,
                      a22.i_class_id AS c1,
                      a22.i_category_id AS c2,
                      1 AS c3
                    FROM (
                      (
                        SELECT
                          a19.ss_item_sk AS c0
                        FROM (
                          store_sales AS a19
                            INNER JOIN date_dim AS a20
                              ON (
                                (
                                  a19.ss_sold_date_sk = a20.d_date_sk
                                )
                                AND (
                                  (
                                    a19.ss_sold_date_sk <= 2452275
                                  ) AND (
                                    a19.ss_sold_date_sk >= 2451180
                                  )
                                )
                                AND (
                                  1999 <= a20.d_year
                                )
                                AND (
                                  a20.d_year <= 2001
                                )
                              )
                        )
                        GROUP BY
                          a19.ss_item_sk
                      ) AS a21
                      INNER JOIN item AS a22
                        ON (
                          a21.c0 = a22.i_item_sk
                        )
                    )
                    GROUP BY
                      a22.i_brand_id,
                      a22.i_class_id,
                      a22.i_category_id,
                      1
                  )
                ) AS a14
                GROUP BY
                  a14.c2,
                  a14.c1,
                  a14.c0
              ) AS a13
              WHERE
                (
                  (
                    a13.c4 - CASE WHEN (
                      a13.c3 >= 0
                    ) THEN a13.c3 ELSE (
                      -(
                        a13.c3
                      )
                    ) END
                  ) >= 2
                )
            )
          ) AS a8
          GROUP BY
            a8.c2,
            a8.c1,
            a8.c0
        ) AS a7
        WHERE
          (
            (
              a7.c4 - CASE WHEN (
                a7.c3 >= 0
              ) THEN a7.c3 ELSE (
                -(
                  a7.c3
                )
              ) END
            ) >= 2
          )
      ) AS a6
      INNER JOIN item AS a23
        ON (
          a23.i_category_id = a6.c2
        )
        AND (
          a23.i_class_id = a6.c1
        )
        AND (
          a23.i_brand_id = a6.c0
        )
    )
    GROUP BY
      a23.i_item_sk
  ) AS a5
  GROUP BY
    a5.c0
)
SELECT
  'store' AS CHANNEL,
  a35.c1 AS I_BRAND_ID,
  a35.c2 AS I_CLASS_ID,
  a35.c3 AS I_CATEGORY_ID,
  a35.c0 AS SALES,
  a35.c4 AS NUMBER_SALES,
  'store' AS CHANNEL,
  a0.c1 AS I_BRAND_ID,
  a0.c2 AS I_CLASS_ID,
  a0.c3 AS I_CATEGORY_ID,
  a0.c0 AS SALES,
  a0.c4 AS NUMBER_SALES
FROM (
  (
    (
      SELECT
        SUM((
          a1.ss_quantity * a1.ss_list_price
        )) AS c0,
        a25.i_brand_id AS c1,
        a25.i_class_id AS c2,
        a25.i_category_id AS c3,
        COUNT(*) AS c4
      FROM (
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
                      a1.ss_sold_date_sk <= 2451526
                    ) AND (
                      a1.ss_sold_date_sk >= 2451520
                    )
                  )
                  AND (
                    a2.d_week_seq = (
                      SELECT
                        a3.d_week_seq
                      FROM date_dim AS a3
                      WHERE
                        (
                          a3.d_year = 1999
                        ) AND (
                          a3.d_moy = 12
                        ) AND (
                          a3.d_dom = 11
                        )
                    )
                  )
                )
          )
          INNER JOIN a4 AS A24
            ON (
              a1.ss_item_sk = A24.c0
            )
        )
        INNER JOIN item AS a25
          ON (
            a25.i_item_sk = A24.c0
          )
      )
      GROUP BY
        a25.i_brand_id,
        a25.i_class_id,
        a25.i_category_id
    ) AS a0
    INNER JOIN (
      SELECT
        CAST((
          a27.c0 / COALESCE(a27.c1, 0000000000000000000000000000000.)
        ) AS DECIMAL(18, 2)) AS c0
      FROM (
        SELECT
          SUM(a28.c1) AS c0,
          SUM(a28.c0) AS c1
        FROM (
          (
            SELECT
              COUNT((
                a29.ss_quantity * a29.ss_list_price
              )) AS c0,
              SUM((
                a29.ss_quantity * a29.ss_list_price
              )) AS c1
            FROM (
              store_sales AS a29
                INNER JOIN date_dim AS a30
                  ON (
                    (
                      a29.ss_sold_date_sk = a30.d_date_sk
                    )
                    AND (
                      (
                        a29.ss_sold_date_sk <= 2452275
                      ) AND (
                        a29.ss_sold_date_sk >= 2451180
                      )
                    )
                    AND (
                      1999 <= a30.d_year
                    )
                    AND (
                      a30.d_year <= 2001
                    )
                  )
            )
          )
          UNION ALL
          (
            SELECT
              COUNT((
                a31.cs_quantity * a31.cs_list_price
              )) AS c0,
              SUM((
                a31.cs_quantity * a31.cs_list_price
              )) AS c1
            FROM (
              catalog_sales AS a31
                INNER JOIN date_dim AS a32
                  ON (
                    (
                      a31.cs_sold_date_sk = a32.d_date_sk
                    )
                    AND (
                      (
                        a31.cs_sold_date_sk <= 2452275
                      ) AND (
                        a31.cs_sold_date_sk >= 2451180
                      )
                    )
                    AND (
                      1999 <= a32.d_year
                    )
                    AND (
                      a32.d_year <= 2001
                    )
                  )
            )
          )
          UNION ALL
          (
            SELECT
              COUNT((
                a33.ws_quantity * a33.ws_list_price
              )) AS c0,
              SUM((
                a33.ws_quantity * a33.ws_list_price
              )) AS c1
            FROM (
              web_sales AS a33
                INNER JOIN date_dim AS a34
                  ON (
                    (
                      a33.ws_sold_date_sk = a34.d_date_sk
                    )
                    AND (
                      (
                        a33.ws_sold_date_sk <= 2452275
                      ) AND (
                        a33.ws_sold_date_sk >= 2451180
                      )
                    )
                    AND (
                      1999 <= a34.d_year
                    )
                    AND (
                      a34.d_year <= 2001
                    )
                  )
            )
          )
        ) AS a28
      ) AS a27
    ) AS a26
      ON (
        (
          a26.c0 < a0.c0
        )
      )
  )
  INNER JOIN (
    SELECT
      SUM((
        a36.ss_quantity * a36.ss_list_price
      )) AS c0,
      a40.i_brand_id AS c1,
      a40.i_class_id AS c2,
      a40.i_category_id AS c3,
      COUNT(*) AS c4
    FROM (
      (
        (
          store_sales AS a36
            INNER JOIN date_dim AS a37
              ON (
                (
                  a36.ss_sold_date_sk = a37.d_date_sk
                )
                AND (
                  (
                    a36.ss_sold_date_sk <= 2451890
                  ) AND (
                    a36.ss_sold_date_sk >= 2451884
                  )
                )
                AND (
                  a37.d_week_seq = (
                    SELECT
                      a38.d_week_seq
                    FROM date_dim AS a38
                    WHERE
                      (
                        a38.d_year = 2000
                      ) AND (
                        a38.d_moy = 12
                      ) AND (
                        a38.d_dom = 11
                      )
                  )
                )
              )
        )
        INNER JOIN a4 AS A39
          ON (
            a36.ss_item_sk = A39.c0
          )
      )
      INNER JOIN item AS a40
        ON (
          a40.i_item_sk = A39.c0
        )
    )
    GROUP BY
      a40.i_brand_id,
      a40.i_class_id,
      a40.i_category_id
  ) AS a35
    ON (
      a35.c1 = a0.c1
    )
    AND (
      a35.c2 = a0.c2
    )
    AND (
      a35.c3 = a0.c3
    )
    AND (
      a26.c0 < a35.c0
    )
)
ORDER BY
  8 ASC NULLS LAST,
  9 ASC NULLS LAST,
  10 ASC NULLS LAST
LIMIT 100