SELECT
  a14.c0 AS ITEM_ID,
  a14.c1 AS SS_ITEM_REV,
  (
    (
      (
        a14.c1 / (
          (
            a14.c1 + a7.c1
          ) + a0.c1
        )
      ) / 3
    ) * 100
  ) AS SS_DEV,
  a7.c1 AS CS_ITEM_REV,
  (
    (
      (
        a7.c1 / (
          (
            a14.c1 + a7.c1
          ) + a0.c1
        )
      ) / 3
    ) * 100
  ) AS CS_DEV,
  a0.c1 AS WS_ITEM_REV,
  (
    (
      (
        a0.c1 / (
          (
            a14.c1 + a7.c1
          ) + a0.c1
        )
      ) / 3
    ) * 100
  ) AS WS_DEV,
  (
    (
      (
        a14.c1 + a7.c1
      ) + a0.c1
    ) / 3
  ) AS AVERAGE
FROM (
  (
    (
      SELECT
        a6.i_item_id AS c0,
        SUM(a1.ws_ext_sales_price) AS c1
      FROM (
        (
          web_sales AS a1
            INNER JOIN (
              date_dim AS a2
                INNER JOIN (
                  SELECT DISTINCT
                    a4.d_date AS c0
                  FROM date_dim AS a4
                  WHERE
                    (
                      a4.d_week_seq = (
                        SELECT
                          a5.d_week_seq
                        FROM date_dim AS a5
                        WHERE
                          (
                            a5.d_date = CAST('2000-01-03' AS DATE)
                          )
                      )
                    )
                ) AS a3
                  ON (
                    a2.d_date = a3.c0
                  )
            )
              ON (
                a1.ws_sold_date_sk = a2.d_date_sk
              )
        )
        INNER JOIN item AS a6
          ON (
            a1.ws_item_sk = a6.i_item_sk
          )
      )
      GROUP BY
        a6.i_item_id
    ) AS a0
    INNER JOIN (
      SELECT
        a13.i_item_id AS c0,
        SUM(a8.cs_ext_sales_price) AS c1
      FROM (
        (
          catalog_sales AS a8
            INNER JOIN (
              date_dim AS a9
                INNER JOIN (
                  SELECT DISTINCT
                    a11.d_date AS c0
                  FROM date_dim AS a11
                  WHERE
                    (
                      a11.d_week_seq = (
                        SELECT
                          a12.d_week_seq
                        FROM date_dim AS a12
                        WHERE
                          (
                            a12.d_date = CAST('2000-01-03' AS DATE)
                          )
                      )
                    )
                ) AS a10
                  ON (
                    a9.d_date = a10.c0
                  )
            )
              ON (
                a8.cs_sold_date_sk = a9.d_date_sk
              )
        )
        INNER JOIN item AS a13
          ON (
            a8.cs_item_sk = a13.i_item_sk
          )
      )
      GROUP BY
        a13.i_item_id
    ) AS a7
      ON (
        a0.c0 = a7.c0
      )
      AND (
        (
          0.9 * a0.c1
        ) <= a7.c1
      )
      AND (
        a7.c1 <= (
          1.1 * a0.c1
        )
      )
      AND (
        (
          0.9 * a7.c1
        ) <= a0.c1
      )
      AND (
        a0.c1 <= (
          1.1 * a7.c1
        )
      )
  )
  INNER JOIN (
    SELECT
      a20.i_item_id AS c0,
      SUM(a15.ss_ext_sales_price) AS c1
    FROM (
      (
        store_sales AS a15
          INNER JOIN (
            date_dim AS a16
              INNER JOIN (
                SELECT DISTINCT
                  a18.d_date AS c0
                FROM date_dim AS a18
                WHERE
                  (
                    a18.d_week_seq = (
                      SELECT
                        a19.d_week_seq
                      FROM date_dim AS a19
                      WHERE
                        (
                          a19.d_date = CAST('2000-01-03' AS DATE)
                        )
                    )
                  )
              ) AS a17
                ON (
                  a16.d_date = a17.c0
                )
          )
            ON (
              a15.ss_sold_date_sk = a16.d_date_sk
            )
      )
      INNER JOIN item AS a20
        ON (
          a15.ss_item_sk = a20.i_item_sk
        )
    )
    GROUP BY
      a20.i_item_id
  ) AS a14
    ON (
      a7.c0 = a14.c0
    )
    AND (
      (
        0.9 * a7.c1
      ) <= a14.c1
    )
    AND (
      a14.c1 <= (
        1.1 * a7.c1
      )
    )
    AND (
      (
        0.9 * a0.c1
      ) <= a14.c1
    )
    AND (
      a14.c1 <= (
        1.1 * a0.c1
      )
    )
    AND (
      (
        0.9 * a14.c1
      ) <= a7.c1
    )
    AND (
      a7.c1 <= (
        1.1 * a14.c1
      )
    )
    AND (
      (
        0.9 * a14.c1
      ) <= a0.c1
    )
    AND (
      a0.c1 <= (
        1.1 * a14.c1
      )
    )
)
ORDER BY
  1 ASC NULLS LAST
LIMIT 100