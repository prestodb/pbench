SELECT
  a0.c0 AS CA_STATE,
  a0.c1 AS CD_GENDER,
  a0.c2 AS CD_MARITAL_STATUS,
  a0.c3 AS CD_DEP_COUNT,
  a0.c4 AS CNT1,
  a0.c5,
  a0.c6,
  (
    a0.c17 / a0.c18
  ),
  a0.c7 AS CD_DEP_EMPLOYED_COUNT,
  a0.c4 AS CNT2,
  a0.c8,
  a0.c9,
  (
    a0.c15 / a0.c16
  ),
  a0.c10 AS CD_DEP_COLLEGE_COUNT,
  a0.c4 AS CNT3,
  a0.c11,
  a0.c12,
  (
    a0.c13 / a0.c14
  )
FROM (
  SELECT
    a1.c5 AS c0,
    a1.c4 AS c1,
    a1.c3 AS c2,
    a1.c2 AS c3,
    COUNT(*) AS c4,
    MIN(a1.c2) AS c5,
    MAX(a1.c2) AS c6,
    a1.c1 AS c7,
    MIN(a1.c1) AS c8,
    MAX(a1.c1) AS c9,
    a1.c0 AS c10,
    MIN(a1.c0) AS c11,
    MAX(a1.c0) AS c12,
    SUM(a1.c6) AS c13,
    COUNT(a1.c6) AS c14,
    SUM(a1.c7) AS c15,
    COUNT(a1.c7) AS c16,
    SUM(a1.c8) AS c17,
    COUNT(a1.c8) AS c18
  FROM (
    (
      SELECT
        a2.c0 AS c0,
        a2.c1 AS c1,
        a2.c2 AS c2,
        a2.c3 AS c3,
        a2.c4 AS c4,
        a2.c5 AS c5,
        a2.c6 AS c6,
        a2.c7 AS c7,
        a2.c8 AS c8,
        a9.c0 AS c9,
        a2.c9 AS c10
      FROM (
        (
          SELECT
            a8.cd_dep_college_count AS c0,
            a8.cd_dep_employed_count AS c1,
            a8.cd_dep_count AS c2,
            a8.cd_marital_status AS c3,
            a8.cd_gender AS c4,
            a4.ca_state AS c5,
            CAST(a8.cd_dep_college_count AS DOUBLE) AS c6,
            CAST(a8.cd_dep_employed_count AS DOUBLE) AS c7,
            CAST(a8.cd_dep_count AS DOUBLE) AS c8,
            a5.c0 AS c9
          FROM (
            (
              (
                customer AS a3
                  INNER JOIN customer_address AS a4
                    ON (
                      (
                        a3.c_current_addr_sk = a4.ca_address_sk
                      )
                      AND (
                        NOT a3.c_customer_sk IS NULL
                      )
                    )
              )
              INNER JOIN (
                SELECT DISTINCT
                  a6.ss_customer_sk AS c0
                FROM (
                  store_sales AS a6
                    INNER JOIN date_dim AS a7
                      ON (
                        (
                          a6.ss_sold_date_sk = a7.d_date_sk
                        )
                        AND (
                          NOT a6.ss_customer_sk IS NULL
                        )
                        AND (
                          (
                            a6.ss_sold_date_sk <= 2452549
                          ) AND (
                            a6.ss_sold_date_sk >= 2452276
                          )
                        )
                        AND (
                          a7.d_year = 2002
                        )
                        AND (
                          a7.d_qoy < 4
                        )
                      )
                )
              ) AS a5
                ON (
                  (
                    a3.c_customer_sk = a5.c0
                  ) AND (
                    NOT a5.c0 IS NULL
                  )
                )
            )
            INNER JOIN customer_demographics AS a8
              ON (
                a8.cd_demo_sk = a3.c_current_cdemo_sk
              )
          )
        ) AS a2
        LEFT OUTER JOIN (
          SELECT DISTINCT
            a10.ws_bill_customer_sk AS c0
          FROM (
            web_sales AS a10
              INNER JOIN date_dim AS a11
                ON (
                  (
                    a10.ws_sold_date_sk = a11.d_date_sk
                  )
                  AND (
                    NOT a10.ws_bill_customer_sk IS NULL
                  )
                  AND (
                    (
                      a10.ws_sold_date_sk <= 2452549
                    ) AND (
                      a10.ws_sold_date_sk >= 2452276
                    )
                  )
                  AND (
                    a11.d_year = 2002
                  )
                  AND (
                    a11.d_qoy < 4
                  )
                )
          )
        ) AS a9
          ON (
            a2.c9 = a9.c0
          )
      )
    ) AS a1
    LEFT OUTER JOIN (
      SELECT DISTINCT
        a13.cs_ship_customer_sk AS c0
      FROM (
        catalog_sales AS a13
          INNER JOIN date_dim AS a14
            ON (
              (
                a13.cs_sold_date_sk = a14.d_date_sk
              )
              AND (
                NOT a13.cs_ship_customer_sk IS NULL
              )
              AND (
                (
                  a13.cs_sold_date_sk <= 2452549
                ) AND (
                  a13.cs_sold_date_sk >= 2452276
                )
              )
              AND (
                a14.d_year = 2002
              )
              AND (
                a14.d_qoy < 4
              )
            )
      )
    ) AS a12
      ON (
        a1.c10 = a12.c0
      )
  )
  WHERE
    (
      (
        NOT a1.c9 IS NULL
      ) OR (
        NOT a12.c0 IS NULL
      )
    )
  GROUP BY
    a1.c5,
    a1.c4,
    a1.c3,
    a1.c2,
    a1.c1,
    a1.c0
) AS a0
ORDER BY
  1 ASC NULLS LAST,
  2 ASC NULLS LAST,
  3 ASC NULLS LAST,
  4 ASC NULLS LAST,
  9 ASC NULLS LAST,
  14 ASC NULLS LAST
LIMIT 100