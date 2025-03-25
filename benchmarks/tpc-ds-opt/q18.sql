SELECT
  a0.c0 AS I_ITEM_ID,
  a0.c1 AS CA_COUNTRY,
  a0.c2 AS CA_STATE,
  a0.c3 AS CA_COUNTY,
  CAST((
    a0.c16 / CASE
      WHEN (
        NOT a0.c18 IS NULL
      )
      THEN a0.c17
      ELSE 0000000000000000000000000000000.
    END
  ) AS DECIMAL(12, 2)) AS AGG1,
  CAST((
    a0.c14 / CASE
      WHEN (
        NOT a0.c18 IS NULL
      )
      THEN a0.c15
      ELSE 0000000000000000000000000000000.
    END
  ) AS DECIMAL(12, 2)) AS AGG2,
  CAST((
    a0.c12 / CASE
      WHEN (
        NOT a0.c18 IS NULL
      )
      THEN a0.c13
      ELSE 0000000000000000000000000000000.
    END
  ) AS DECIMAL(12, 2)) AS AGG3,
  CAST((
    a0.c10 / CASE
      WHEN (
        NOT a0.c18 IS NULL
      )
      THEN a0.c11
      ELSE 0000000000000000000000000000000.
    END
  ) AS DECIMAL(12, 2)) AS AGG4,
  CAST((
    a0.c8 / CASE
      WHEN (
        NOT a0.c18 IS NULL
      )
      THEN a0.c9
      ELSE 0000000000000000000000000000000.
    END
  ) AS DECIMAL(12, 2)) AS AGG5,
  CAST((
    a0.c6 / CASE
      WHEN (
        NOT a0.c18 IS NULL
      )
      THEN a0.c7
      ELSE 0000000000000000000000000000000.
    END
  ) AS DECIMAL(12, 2)) AS AGG6,
  CAST((
    a0.c4 / CASE
      WHEN (
        NOT a0.c18 IS NULL
      )
      THEN a0.c5
      ELSE 0000000000000000000000000000000.
    END
  ) AS DECIMAL(12, 2)) AS AGG7
FROM (
  (
    SELECT
      a1.c0 AS c0,
      a1.c1 AS c1,
      a1.c2 AS c2,
      a1.c3 AS c3,
      SUM(a1.c17) AS c4,
      SUM(a1.c16) AS c5,
      SUM(a1.c15) AS c6,
      SUM(a1.c14) AS c7,
      SUM(a1.c13) AS c8,
      SUM(a1.c12) AS c9,
      SUM(a1.c11) AS c10,
      SUM(a1.c10) AS c11,
      SUM(a1.c9) AS c12,
      SUM(a1.c8) AS c13,
      SUM(a1.c7) AS c14,
      SUM(a1.c6) AS c15,
      SUM(a1.c5) AS c16,
      SUM(a1.c4) AS c17,
      a1.c18 AS c18
    FROM (
      SELECT
        CASE WHEN (
          a9.c0 < 5
        ) THEN a2.c0 ELSE NULL END AS c0,
        CASE WHEN (
          a9.c0 < 4
        ) THEN a2.c1 ELSE NULL END AS c1,
        CASE WHEN (
          a9.c0 < 3
        ) THEN a2.c2 ELSE NULL END AS c2,
        CASE WHEN (
          a9.c0 < 2
        ) THEN a2.c3 ELSE NULL END AS c3,
        a2.c4 AS c4,
        a2.c5 AS c5,
        a2.c6 AS c6,
        a2.c7 AS c7,
        a2.c8 AS c8,
        a2.c9 AS c9,
        a2.c10 AS c10,
        a2.c11 AS c11,
        a2.c12 AS c12,
        a2.c13 AS c13,
        a2.c14 AS c14,
        a2.c15 AS c15,
        a2.c16 AS c16,
        a2.c17 AS c17,
        a9.c0 AS c18
      FROM (
        (
          SELECT
            a8.i_item_id AS c0,
            a6.ca_country AS c1,
            a6.ca_state AS c2,
            a6.ca_county AS c3,
            COUNT(CAST(a3.cs_quantity AS DECIMAL(12, 2))) AS c4,
            SUM(CAST(a3.cs_quantity AS DECIMAL(12, 2))) AS c5,
            COUNT(CAST(a3.cs_list_price AS DECIMAL(12, 2))) AS c6,
            SUM(CAST(a3.cs_list_price AS DECIMAL(12, 2))) AS c7,
            COUNT(CAST(a3.cs_coupon_amt AS DECIMAL(12, 2))) AS c8,
            SUM(CAST(a3.cs_coupon_amt AS DECIMAL(12, 2))) AS c9,
            COUNT(CAST(a3.cs_sales_price AS DECIMAL(12, 2))) AS c10,
            SUM(CAST(a3.cs_sales_price AS DECIMAL(12, 2))) AS c11,
            COUNT(CAST(a3.cs_net_profit AS DECIMAL(12, 2))) AS c12,
            SUM(CAST(a3.cs_net_profit AS DECIMAL(12, 2))) AS c13,
            COUNT(CAST(a5.c_birth_year AS DECIMAL(12, 2))) AS c14,
            SUM(CAST(a5.c_birth_year AS DECIMAL(12, 2))) AS c15,
            COUNT(CAST(a4.cd_dep_count AS DECIMAL(12, 2))) AS c16,
            SUM(CAST(a4.cd_dep_count AS DECIMAL(12, 2))) AS c17
          FROM (
            (
              (
                (
                  catalog_sales AS a3
                    INNER JOIN customer_demographics AS a4
                      ON (
                        (
                          a3.cs_bill_cdemo_sk = a4.cd_demo_sk
                        )
                        AND (
                          (
                            a3.cs_bill_customer_sk <= 65000000
                          ) AND (
                            a3.cs_bill_customer_sk >= 2
                          )
                        )
                        AND (
                          (
                            a3.cs_sold_date_sk <= 2451179
                          ) AND (
                            a3.cs_sold_date_sk >= 2450815
                          )
                        )
                        AND (
                          (
                            a3.cs_bill_cdemo_sk <= 1920800
                          ) AND (
                            a3.cs_bill_cdemo_sk >= 62
                          )
                        )
                        AND (
                          a4.cd_gender = 'F'
                        )
                        AND (
                          a4.cd_education_status = 'Unknown'
                        )
                      )
                )
                INNER JOIN (
                  customer AS a5
                    INNER JOIN customer_address AS a6
                      ON (
                        (
                          a5.c_current_addr_sk = a6.ca_address_sk
                        )
                        AND (
                          a5.c_birth_month IN (1, 6, 8, 9, 12, 2)
                        )
                        AND (
                          NOT a5.c_current_cdemo_sk IS NULL
                        )
                        AND (
                          a6.ca_state IN ('MS', 'IN', 'ND', 'OK', 'NM', 'VA')
                        )
                      )
                )
                  ON (
                    a3.cs_bill_customer_sk = a5.c_customer_sk
                  )
              )
              INNER JOIN date_dim AS a7
                ON (
                  (
                    a3.cs_sold_date_sk = a7.d_date_sk
                  ) AND (
                    a7.d_year = 1998
                  )
                )
            )
            INNER JOIN item AS a8
              ON (
                a3.cs_item_sk = a8.i_item_sk
              )
          )
          GROUP BY
            a8.i_item_id,
            a6.ca_country,
            a6.ca_state,
            a6.ca_county
        ) AS a2
        INNER JOIN VALUES
          (1),
          (2),
          (3),
          (4),
          (5) AS a9(c0)
          ON (
            1 = 1
          )
      )
    ) AS a1
    GROUP BY
      a1.c18,
      a1.c0,
      a1.c1,
      a1.c2,
      a1.c3
  ) AS a0
  RIGHT OUTER JOIN VALUES
    (1),
    (2),
    (3),
    (4),
    (5) AS a10(c0)
    ON (
      a10.c0 = a0.c18
    )
)
WHERE
  (
    (
      (
        a10.c0 = 5
      ) AND (
        a0.c18 IS NULL
      )
    )
    OR (
      NOT a0.c18 IS NULL
    )
  )
ORDER BY
  2 ASC NULLS LAST,
  3 ASC NULLS LAST,
  4 ASC NULLS LAST,
  1 ASC NULLS LAST
LIMIT 100