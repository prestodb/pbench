SELECT
  a0.c0 AS C_LAST_NAME,
  a0.c1 AS C_FIRST_NAME,
  SUM(a0.c2) AS SALES
FROM (
  (
    (
      (
        SELECT
          a3.c_last_name AS c0,
          a3.c_first_name AS c1,
          (
            a1.ws_quantity * a1.ws_list_price
          ) AS c2,
          1 AS c3,
          a1.ws_bill_customer_sk AS c4,
          a1.ws_item_sk AS c5
        FROM (
          (
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
          INNER JOIN customer AS a3
            ON (
              a1.ws_bill_customer_sk = a3.c_customer_sk
            )
        )
      )
      UNION ALL
      (
        SELECT
          a6.c_last_name AS c0,
          a6.c_first_name AS c1,
          (
            a4.cs_quantity * a4.cs_list_price
          ) AS c2,
          2 AS c3,
          a4.cs_bill_customer_sk AS c4,
          a4.cs_item_sk AS c5
        FROM (
          (
            catalog_sales AS a4
              INNER JOIN date_dim AS a5
                ON (
                  (
                    a4.cs_sold_date_sk = a5.d_date_sk
                  )
                  AND (
                    (
                      a4.cs_sold_date_sk <= 2451604
                    ) AND (
                      a4.cs_sold_date_sk >= 2451576
                    )
                  )
                  AND (
                    a5.d_moy = 2
                  )
                  AND (
                    a5.d_year = 2000
                  )
                )
          )
          INNER JOIN customer AS a6
            ON (
              a4.cs_bill_customer_sk = a6.c_customer_sk
            )
        )
      )
    ) AS a0
    INNER JOIN (
      SELECT
        a8.c0 AS c0
      FROM (
        SELECT
          a9.c1 AS c0
        FROM (
          (
            SELECT
              SUM((
                a10.ss_quantity * a10.ss_sales_price
              )) AS c0,
              a11.c_customer_sk AS c1
            FROM (
              store_sales AS a10
                INNER JOIN customer AS a11
                  ON (
                    a10.ss_customer_sk = a11.c_customer_sk
                  )
            )
            GROUP BY
              a11.c_customer_sk
          ) AS a9
          INNER JOIN (
            SELECT
              MAX(a14.c1) AS c0
            FROM (
              customer AS a13
                INNER JOIN (
                  SELECT
                    a15.ss_customer_sk AS c0,
                    SUM((
                      a15.ss_quantity * a15.ss_sales_price
                    )) AS c1
                  FROM (
                    store_sales AS a15
                      INNER JOIN date_dim AS a16
                        ON (
                          (
                            a15.ss_sold_date_sk = a16.d_date_sk
                          )
                          AND (
                            (
                              a15.ss_sold_date_sk <= 2453005
                            ) AND (
                              a15.ss_sold_date_sk >= 2451545
                            )
                          )
                          AND (
                            a16.d_year IN (2000, 2001, 2002, 2003)
                          )
                        )
                  )
                  GROUP BY
                    a15.ss_customer_sk
                ) AS a14
                  ON (
                    a14.c0 = a13.c_customer_sk
                  )
            )
          ) AS a12
            ON (
              (
                (
                  000000000000.9500000000000000000 * a12.c0
                ) < a9.c0
              )
            )
        )
        GROUP BY
          a9.c1
      ) AS a8
      GROUP BY
        a8.c0
    ) AS a7
      ON (
        a0.c4 = a7.c0
      )
  )
  INNER JOIN (
    SELECT
      a18.c0 AS c0
    FROM (
      SELECT
        a19.c1 AS c0
      FROM (
        SELECT
          COUNT(*) AS c0,
          a20.ss_item_sk AS c1
        FROM (
          store_sales AS a20
            INNER JOIN date_dim AS a21
              ON (
                (
                  a20.ss_sold_date_sk = a21.d_date_sk
                )
                AND (
                  (
                    a20.ss_sold_date_sk <= 2453005
                  ) AND (
                    a20.ss_sold_date_sk >= 2451545
                  )
                )
                AND (
                  a21.d_year IN (2000, 2001, 2002, 2003)
                )
              )
        )
        GROUP BY
          a20.ss_item_sk,
          a21.d_date
      ) AS a19
      WHERE
        (
          4 < a19.c0
        )
      GROUP BY
        a19.c1
    ) AS a18
    GROUP BY
      a18.c0
  ) AS a17
    ON (
      a0.c5 = a17.c0
    )
)
GROUP BY
  a0.c1,
  a0.c0,
  a0.c3
ORDER BY
  1 ASC NULLS LAST,
  2 ASC NULLS LAST,
  3 ASC NULLS LAST
LIMIT 100