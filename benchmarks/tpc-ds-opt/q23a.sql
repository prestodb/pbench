SELECT
  SUM(a0.c0)
FROM (
  (
    (
      (
        SELECT
          (
            a1.ws_quantity * a1.ws_list_price
          ) AS c0,
          a1.ws_bill_customer_sk AS c1,
          a1.ws_item_sk AS c2
        FROM (
          web_sales AS a1
            INNER JOIN date_dim AS a2
              ON (
                (
                  a1.ws_sold_date_sk = a2.d_date_sk
                )
                AND (
                  (
                    a1.ws_sold_date_sk <= 2451604
                  ) AND (
                    a1.ws_sold_date_sk >= 2451576
                  )
                )
                AND (
                  a2.d_moy = 2
                )
                AND (
                  a2.d_year = 2000
                )
              )
        )
      )
      UNION ALL
      (
        SELECT
          (
            a3.cs_quantity * a3.cs_list_price
          ) AS c0,
          a3.cs_bill_customer_sk AS c1,
          a3.cs_item_sk AS c2
        FROM (
          catalog_sales AS a3
            INNER JOIN date_dim AS a4
              ON (
                (
                  a3.cs_sold_date_sk = a4.d_date_sk
                )
                AND (
                  (
                    a3.cs_sold_date_sk <= 2451604
                  ) AND (
                    a3.cs_sold_date_sk >= 2451576
                  )
                )
                AND (
                  a4.d_moy = 2
                )
                AND (
                  a4.d_year = 2000
                )
              )
        )
      )
    ) AS a0
    INNER JOIN (
      SELECT
        a6.c0 AS c0
      FROM (
        SELECT
          a7.c1 AS c0
        FROM (
          SELECT
            COUNT(*) AS c0,
            a8.ss_item_sk AS c1
          FROM (
            store_sales AS a8
              INNER JOIN date_dim AS a9
                ON (
                  (
                    a8.ss_sold_date_sk = a9.d_date_sk
                  )
                  AND (
                    (
                      a8.ss_sold_date_sk <= 2453005
                    ) AND (
                      a8.ss_sold_date_sk >= 2451545
                    )
                  )
                  AND (
                    a9.d_year IN (2000, 2001, 2002, 2003)
                  )
                )
          )
          GROUP BY
            a8.ss_item_sk,
            a9.d_date
        ) AS a7
        WHERE
          (
            4 < a7.c0
          )
        GROUP BY
          a7.c1
      ) AS a6
      GROUP BY
        a6.c0
    ) AS a5
      ON (
        a0.c2 = a5.c0
      )
  )
  INNER JOIN (
    SELECT
      a11.c0 AS c0
    FROM (
      SELECT
        a12.c1 AS c0
      FROM (
        (
          SELECT
            SUM((
              a13.ss_quantity * a13.ss_sales_price
            )) AS c0,
            a14.c_customer_sk AS c1
          FROM (
            store_sales AS a13
              INNER JOIN customer AS a14
                ON (
                  a13.ss_customer_sk = a14.c_customer_sk
                )
          )
          GROUP BY
            a14.c_customer_sk
        ) AS a12
        INNER JOIN (
          SELECT
            MAX(a17.c1) AS c0
          FROM (
            customer AS a16
              INNER JOIN (
                SELECT
                  a18.ss_customer_sk AS c0,
                  SUM((
                    a18.ss_quantity * a18.ss_sales_price
                  )) AS c1
                FROM (
                  store_sales AS a18
                    INNER JOIN date_dim AS a19
                      ON (
                        (
                          a18.ss_sold_date_sk = a19.d_date_sk
                        )
                        AND (
                          (
                            a18.ss_sold_date_sk <= 2453005
                          ) AND (
                            a18.ss_sold_date_sk >= 2451545
                          )
                        )
                        AND (
                          a19.d_year IN (2000, 2001, 2002, 2003)
                        )
                      )
                )
                GROUP BY
                  a18.ss_customer_sk
              ) AS a17
                ON (
                  a17.c0 = a16.c_customer_sk
                )
          )
        ) AS a15
          ON (
            (
              (
                000000000000.9500000000000000000 * a15.c0
              ) < a12.c0
            )
          )
      )
      GROUP BY
        a12.c1
    ) AS a11
    GROUP BY
      a11.c0
  ) AS a10
    ON (
      a0.c1 = a10.c0
    )
)
LIMIT 100