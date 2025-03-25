SELECT
  COUNT(*)
FROM (
  SELECT
    a1.c0 AS c0,
    a1.c1 AS c1,
    a1.c2 AS c2,
    SUM(a1.c3) AS c3,
    COUNT(*) AS c4
  FROM (
    (
      SELECT
        a4.c_last_name AS c0,
        a4.c_first_name AS c1,
        a3.d_date AS c2,
        -1 AS c3
      FROM (
        (
          web_sales AS a2
            INNER JOIN date_dim AS a3
              ON (
                (
                  a2.ws_sold_date_sk = a3.d_date_sk
                )
                AND (
                  (
                    a2.ws_sold_date_sk <= 2451910
                  ) AND (
                    a2.ws_sold_date_sk >= 2451545
                  )
                )
                AND (
                  1200 <= a3.d_month_seq
                )
                AND (
                  a3.d_month_seq <= 1211
                )
              )
        )
        INNER JOIN customer AS a4
          ON (
            a2.ws_bill_customer_sk = a4.c_customer_sk
          )
      )
      GROUP BY
        a4.c_last_name,
        a4.c_first_name,
        a3.d_date
    )
    UNION ALL
    (
      SELECT
        a5.c0 AS c0,
        a5.c1 AS c1,
        a5.c2 AS c2,
        1 AS c3
      FROM (
        SELECT
          a6.c0 AS c0,
          a6.c1 AS c1,
          a6.c2 AS c2,
          SUM(a6.c3) AS c3,
          COUNT(*) AS c4
        FROM (
          (
            SELECT
              a9.c_last_name AS c0,
              a9.c_first_name AS c1,
              a8.d_date AS c2,
              -1 AS c3
            FROM (
              (
                catalog_sales AS a7
                  INNER JOIN date_dim AS a8
                    ON (
                      (
                        a7.cs_sold_date_sk = a8.d_date_sk
                      )
                      AND (
                        (
                          a7.cs_sold_date_sk <= 2451910
                        ) AND (
                          a7.cs_sold_date_sk >= 2451545
                        )
                      )
                      AND (
                        1200 <= a8.d_month_seq
                      )
                      AND (
                        a8.d_month_seq <= 1211
                      )
                    )
              )
              INNER JOIN customer AS a9
                ON (
                  a7.cs_bill_customer_sk = a9.c_customer_sk
                )
            )
            GROUP BY
              a9.c_last_name,
              a9.c_first_name,
              a8.d_date
          )
          UNION ALL
          (
            SELECT
              a12.c_last_name AS c0,
              a12.c_first_name AS c1,
              a11.d_date AS c2,
              1 AS c3
            FROM (
              (
                store_sales AS a10
                  INNER JOIN date_dim AS a11
                    ON (
                      (
                        a10.ss_sold_date_sk = a11.d_date_sk
                      )
                      AND (
                        (
                          a10.ss_sold_date_sk <= 2451910
                        ) AND (
                          a10.ss_sold_date_sk >= 2451545
                        )
                      )
                      AND (
                        1200 <= a11.d_month_seq
                      )
                      AND (
                        a11.d_month_seq <= 1211
                      )
                    )
              )
              INNER JOIN customer AS a12
                ON (
                  a10.ss_customer_sk = a12.c_customer_sk
                )
            )
            GROUP BY
              a12.c_last_name,
              a12.c_first_name,
              a11.d_date
          )
        ) AS a6
        GROUP BY
          a6.c2,
          a6.c1,
          a6.c0
      ) AS a5
      WHERE
        (
          a5.c4 = a5.c3
        )
    )
  ) AS a1
  GROUP BY
    a1.c2,
    a1.c1,
    a1.c0
) AS a0
WHERE
  (
    a0.c4 = a0.c3
  )