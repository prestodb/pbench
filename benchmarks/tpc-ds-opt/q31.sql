SELECT
  a0.c0 AS CA_COUNTY,
  a0.c8 AS D_YEAR,
  (
    a6.c4 / a6.c7
  ) AS WEB_Q1_Q2_INCREASE,
  (
    a0.c4 / a0.c7
  ) AS STORE_Q1_Q2_INCREASE,
  (
    a6.c1 / a6.c4
  ) AS WEB_Q2_Q3_INCREASE,
  (
    a0.c1 / a0.c4
  ) AS STORE_Q2_Q3_INCREASE
FROM (
  (
    SELECT
      a1.c0 AS c0,
      MAX(a1.c1) AS c1,
      MAX(a1.c2) AS c2,
      MAX(a1.c3) AS c3,
      MAX(a1.c4) AS c4,
      MAX(a1.c5) AS c5,
      MAX(a1.c6) AS c6,
      MAX(a1.c7) AS c7,
      MAX(a1.c8) AS c8,
      MAX(a1.c9) AS c9
    FROM (
      SELECT
        a2.c0 AS c0,
        CASE
          WHEN (
            (
              a2.c2 = 2000
            ) AND (
              a2.c1 = 3
            )
          )
          THEN a2.c3
          ELSE NULL
        END AS c1,
        CASE
          WHEN (
            (
              a2.c2 = 2000
            ) AND (
              a2.c1 = 3
            )
          )
          THEN a2.c2
          ELSE NULL
        END AS c2,
        CASE
          WHEN (
            (
              a2.c2 = 2000
            ) AND (
              a2.c1 = 3
            )
          )
          THEN a2.c1
          ELSE NULL
        END AS c3,
        CASE
          WHEN (
            (
              a2.c2 = 2000
            ) AND (
              a2.c1 = 2
            )
          )
          THEN a2.c3
          ELSE NULL
        END AS c4,
        CASE
          WHEN (
            (
              a2.c2 = 2000
            ) AND (
              a2.c1 = 2
            )
          )
          THEN a2.c2
          ELSE NULL
        END AS c5,
        CASE
          WHEN (
            (
              a2.c2 = 2000
            ) AND (
              a2.c1 = 2
            )
          )
          THEN a2.c1
          ELSE NULL
        END AS c6,
        CASE
          WHEN (
            (
              a2.c2 = 2000
            ) AND (
              a2.c1 = 1
            )
          )
          THEN a2.c3
          ELSE NULL
        END AS c7,
        CASE
          WHEN (
            (
              a2.c2 = 2000
            ) AND (
              a2.c1 = 1
            )
          )
          THEN a2.c2
          ELSE NULL
        END AS c8,
        CASE
          WHEN (
            (
              a2.c2 = 2000
            ) AND (
              a2.c1 = 1
            )
          )
          THEN a2.c1
          ELSE NULL
        END AS c9
      FROM (
        SELECT
          a5.ca_county AS c0,
          a4.d_qoy AS c1,
          a4.d_year AS c2,
          SUM(a3.ss_ext_sales_price) AS c3
        FROM (
          (
            store_sales AS a3
              INNER JOIN date_dim AS a4
                ON (
                  (
                    a3.ss_sold_date_sk = a4.d_date_sk
                  )
                  AND (
                    (
                      a3.ss_sold_date_sk <= 2451818
                    ) AND (
                      a3.ss_sold_date_sk >= 2451545
                    )
                  )
                  AND (
                    (
                      a3.ss_addr_sk <= 32500000
                    ) AND (
                      a3.ss_addr_sk >= 1
                    )
                  )
                  AND (
                    a4.d_qoy IN (3, 2, 1)
                  )
                  AND (
                    a4.d_year = 2000
                  )
                )
          )
          INNER JOIN customer_address AS a5
            ON (
              (
                a3.ss_addr_sk = a5.ca_address_sk
              ) AND (
                NOT a5.ca_county IS NULL
              )
            )
        )
        GROUP BY
          a5.ca_county,
          a4.d_qoy,
          a4.d_year
      ) AS a2
    ) AS a1
    GROUP BY
      a1.c0
  ) AS a0
  INNER JOIN (
    SELECT
      a7.c0 AS c0,
      MAX(a7.c1) AS c1,
      MAX(a7.c2) AS c2,
      MAX(a7.c3) AS c3,
      MAX(a7.c4) AS c4,
      MAX(a7.c5) AS c5,
      MAX(a7.c6) AS c6,
      MAX(a7.c7) AS c7,
      MAX(a7.c8) AS c8,
      MAX(a7.c9) AS c9
    FROM (
      SELECT
        a8.c0 AS c0,
        CASE
          WHEN (
            (
              a8.c2 = 2000
            ) AND (
              a8.c1 = 3
            )
          )
          THEN a8.c3
          ELSE NULL
        END AS c1,
        CASE
          WHEN (
            (
              a8.c2 = 2000
            ) AND (
              a8.c1 = 3
            )
          )
          THEN a8.c2
          ELSE NULL
        END AS c2,
        CASE
          WHEN (
            (
              a8.c2 = 2000
            ) AND (
              a8.c1 = 3
            )
          )
          THEN a8.c1
          ELSE NULL
        END AS c3,
        CASE
          WHEN (
            (
              a8.c2 = 2000
            ) AND (
              a8.c1 = 2
            )
          )
          THEN a8.c3
          ELSE NULL
        END AS c4,
        CASE
          WHEN (
            (
              a8.c2 = 2000
            ) AND (
              a8.c1 = 2
            )
          )
          THEN a8.c2
          ELSE NULL
        END AS c5,
        CASE
          WHEN (
            (
              a8.c2 = 2000
            ) AND (
              a8.c1 = 2
            )
          )
          THEN a8.c1
          ELSE NULL
        END AS c6,
        CASE
          WHEN (
            (
              a8.c2 = 2000
            ) AND (
              a8.c1 = 1
            )
          )
          THEN a8.c3
          ELSE NULL
        END AS c7,
        CASE
          WHEN (
            (
              a8.c2 = 2000
            ) AND (
              a8.c1 = 1
            )
          )
          THEN a8.c2
          ELSE NULL
        END AS c8,
        CASE
          WHEN (
            (
              a8.c2 = 2000
            ) AND (
              a8.c1 = 1
            )
          )
          THEN a8.c1
          ELSE NULL
        END AS c9
      FROM (
        SELECT
          a11.ca_county AS c0,
          a10.d_qoy AS c1,
          a10.d_year AS c2,
          SUM(a9.ws_ext_sales_price) AS c3
        FROM (
          (
            web_sales AS a9
              INNER JOIN date_dim AS a10
                ON (
                  (
                    a9.ws_sold_date_sk = a10.d_date_sk
                  )
                  AND (
                    (
                      a9.ws_sold_date_sk <= 2451818
                    ) AND (
                      a9.ws_sold_date_sk >= 2451545
                    )
                  )
                  AND (
                    (
                      a9.ws_bill_addr_sk <= 32500000
                    ) AND (
                      a9.ws_bill_addr_sk >= 1
                    )
                  )
                  AND (
                    a10.d_qoy IN (3, 2, 1)
                  )
                  AND (
                    a10.d_year = 2000
                  )
                )
          )
          INNER JOIN customer_address AS a11
            ON (
              (
                a9.ws_bill_addr_sk = a11.ca_address_sk
              ) AND (
                NOT a11.ca_county IS NULL
              )
            )
        )
        GROUP BY
          a11.ca_county,
          a10.d_qoy,
          a10.d_year
      ) AS a8
    ) AS a7
    GROUP BY
      a7.c0
  ) AS a6
    ON (
      (
        a0.c0 = a6.c0
      )
      AND (
        a0.c3 = a6.c3
      )
      AND (
        a0.c6 = a6.c6
      )
      AND (
        a0.c2 = a6.c2
      )
      AND (
        a0.c9 = a6.c9
      )
      AND (
        CASE WHEN (
          a0.c7 > 0
        ) THEN (
          a0.c4 / a0.c7
        ) ELSE NULL END < CASE WHEN (
          a6.c7 > 0
        ) THEN (
          a6.c4 / a6.c7
        ) ELSE NULL END
      )
      AND (
        CASE WHEN (
          a0.c4 > 0
        ) THEN (
          a0.c1 / a0.c4
        ) ELSE NULL END < CASE WHEN (
          a6.c4 > 0
        ) THEN (
          a6.c1 / a6.c4
        ) ELSE NULL END
      )
      AND (
        a0.c9 = 1
      )
      AND (
        a0.c8 = 2000
      )
      AND (
        a0.c6 = 2
      )
      AND (
        a0.c5 = 2000
      )
      AND (
        a0.c3 = 3
      )
      AND (
        a0.c2 = 2000
      )
      AND (
        a6.c9 = 1
      )
      AND (
        a6.c8 = 2000
      )
      AND (
        a6.c6 = 2
      )
      AND (
        a6.c5 = 2000
      )
      AND (
        a6.c3 = 3
      )
      AND (
        a6.c2 = 2000
      )
    )
)
ORDER BY
  1 ASC NULLS LAST