SELECT
  a7.c0 AS ITEM_ID,
  a7.c1 AS SR_ITEM_QTY,
  (
    (
      (
        a7.c1 / (
          (
            a7.c1 + a0.c1
          ) + a14.c1
        )
      ) / 3.0
    ) * 100
  ) AS SR_DEV,
  a0.c1 AS CR_ITEM_QTY,
  (
    (
      (
        a0.c1 / (
          (
            a7.c1 + a0.c1
          ) + a14.c1
        )
      ) / 3.0
    ) * 100
  ) AS CR_DEV,
  a14.c1 AS WR_ITEM_QTY,
  (
    (
      (
        a14.c1 / (
          (
            a7.c1 + a0.c1
          ) + a14.c1
        )
      ) / 3.0
    ) * 100
  ) AS WR_DEV,
  (
    (
      (
        a7.c1 + a0.c1
      ) + a14.c1
    ) / 3.0
  ) AS AVERAGE
FROM (
  (
    (
      SELECT
        a1.c0 AS c0,
        SUM(a1.c1) AS c1
      FROM (
        SELECT DISTINCT
          a6.i_item_id AS c0,
          a2.cr_return_quantity AS c1,
          a3.d_date_sk AS c2,
          a2.cr_item_sk AS c3,
          a2.cr_item_sk AS c4,
          a2.cr_order_number AS c5
        FROM (
          (
            (
              catalog_returns AS a2
                INNER JOIN (
                  date_dim AS a3
                    INNER JOIN date_dim AS a4
                      ON (
                        a3.d_date = a4.d_date
                      )
                )
                  ON (
                    a2.cr_returned_date_sk = a3.d_date_sk
                  )
            )
            INNER JOIN date_dim AS a5
              ON (
                (
                  a4.d_week_seq = a5.d_week_seq
                )
                AND (
                  a5.d_date IN (CAST('2000-06-30' AS DATE), CAST('2000-09-27' AS DATE), CAST('2000-11-17' AS DATE))
                )
              )
          )
          INNER JOIN item AS a6
            ON (
              a2.cr_item_sk = a6.i_item_sk
            )
        )
      ) AS a1
      GROUP BY
        a1.c0
    ) AS a0
    INNER JOIN (
      SELECT
        a8.c0 AS c0,
        SUM(a8.c1) AS c1
      FROM (
        SELECT DISTINCT
          a13.i_item_id AS c0,
          a9.sr_return_quantity AS c1,
          a10.d_date_sk AS c2,
          a9.sr_item_sk AS c3,
          a9.sr_item_sk AS c4,
          a9.sr_ticket_number AS c5
        FROM (
          (
            (
              store_returns AS a9
                INNER JOIN (
                  date_dim AS a10
                    INNER JOIN date_dim AS a11
                      ON (
                        a10.d_date = a11.d_date
                      )
                )
                  ON (
                    a9.sr_returned_date_sk = a10.d_date_sk
                  )
            )
            INNER JOIN date_dim AS a12
              ON (
                (
                  a11.d_week_seq = a12.d_week_seq
                )
                AND (
                  a12.d_date IN (CAST('2000-06-30' AS DATE), CAST('2000-09-27' AS DATE), CAST('2000-11-17' AS DATE))
                )
              )
          )
          INNER JOIN item AS a13
            ON (
              a9.sr_item_sk = a13.i_item_sk
            )
        )
      ) AS a8
      GROUP BY
        a8.c0
    ) AS a7
      ON (
        a0.c0 = a7.c0
      )
  )
  INNER JOIN (
    SELECT
      a15.c0 AS c0,
      SUM(a15.c1) AS c1
    FROM (
      SELECT DISTINCT
        a20.i_item_id AS c0,
        a16.wr_return_quantity AS c1,
        a17.d_date_sk AS c2,
        a16.wr_item_sk AS c3,
        a16.wr_item_sk AS c4,
        a16.wr_order_number AS c5
      FROM (
        (
          (
            web_returns AS a16
              INNER JOIN date_dim AS a17
                ON (
                  a16.wr_returned_date_sk = a17.d_date_sk
                )
          )
          INNER JOIN (
            date_dim AS a18
              INNER JOIN date_dim AS a19
                ON (
                  (
                    a18.d_week_seq = a19.d_week_seq
                  )
                  AND (
                    a19.d_date IN (CAST('2000-06-30' AS DATE), CAST('2000-09-27' AS DATE), CAST('2000-11-17' AS DATE))
                  )
                )
          )
            ON (
              a17.d_date = a18.d_date
            )
        )
        INNER JOIN item AS a20
          ON (
            a16.wr_item_sk = a20.i_item_sk
          )
      )
    ) AS a15
    GROUP BY
      a15.c0
  ) AS a14
    ON (
      a7.c0 = a14.c0
    )
)
ORDER BY
  1 ASC NULLS LAST
LIMIT 100