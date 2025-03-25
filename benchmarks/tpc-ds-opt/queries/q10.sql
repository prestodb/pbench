SELECT
  a0.c0 AS CD_GENDER,
  a0.c1 AS CD_MARITAL_STATUS,
  a0.c2 AS CD_EDUCATION_STATUS,
  a0.c3 AS CNT1,
  a0.c4 AS CD_PURCHASE_ESTIMATE,
  a0.c3 AS CNT2,
  a0.c5 AS CD_CREDIT_RATING,
  a0.c3 AS CNT3,
  a0.c6 AS CD_DEP_COUNT,
  a0.c3 AS CNT4,
  a0.c7 AS CD_DEP_EMPLOYED_COUNT,
  a0.c3 AS CNT5,
  a0.c8 AS CD_DEP_COLLEGE_COUNT,
  a0.c3 AS CNT6
FROM (
  SELECT
    a4.c7 AS c0,
    a4.c6 AS c1,
    a4.c5 AS c2,
    COUNT(*) AS c3,
    a4.c4 AS c4,
    a4.c3 AS c5,
    a4.c2 AS c6,
    a4.c1 AS c7,
    a4.c0 AS c8
  FROM (
    (
      SELECT DISTINCT
        a2.cs_ship_customer_sk AS c0
      FROM (
        catalog_sales AS a2
          INNER JOIN date_dim AS a3
            ON (
              (
                a2.cs_sold_date_sk = a3.d_date_sk
              )
              AND (
                NOT a2.cs_ship_customer_sk IS NULL
              )
              AND (
                (
                  a2.cs_sold_date_sk <= 2452395
                ) AND (
                  a2.cs_sold_date_sk >= 2452276
                )
              )
              AND (
                a3.d_year = 2002
              )
              AND (
                1 <= a3.d_moy
              )
              AND (
                a3.d_moy <= 4
              )
            )
      )
    ) AS a1
    RIGHT OUTER JOIN (
      SELECT
        a8.c0 AS c0,
        a8.c1 AS c1,
        a8.c2 AS c2,
        a8.c3 AS c3,
        a8.c4 AS c4,
        a8.c5 AS c5,
        a8.c6 AS c6,
        a8.c7 AS c7,
        a5.c0 AS c8,
        a8.c8 AS c9
      FROM (
        (
          SELECT DISTINCT
            a6.ws_bill_customer_sk AS c0
          FROM (
            web_sales AS a6
              INNER JOIN date_dim AS a7
                ON (
                  (
                    a6.ws_sold_date_sk = a7.d_date_sk
                  )
                  AND (
                    NOT a6.ws_bill_customer_sk IS NULL
                  )
                  AND (
                    (
                      a6.ws_sold_date_sk <= 2452395
                    ) AND (
                      a6.ws_sold_date_sk >= 2452276
                    )
                  )
                  AND (
                    a7.d_year = 2002
                  )
                  AND (
                    1 <= a7.d_moy
                  )
                  AND (
                    a7.d_moy <= 4
                  )
                )
          )
        ) AS a5
        RIGHT OUTER JOIN (
          SELECT
            a14.cd_dep_college_count AS c0,
            a14.cd_dep_employed_count AS c1,
            a14.cd_dep_count AS c2,
            a14.cd_credit_rating AS c3,
            a14.cd_purchase_estimate AS c4,
            a14.cd_education_status AS c5,
            a14.cd_marital_status AS c6,
            a14.cd_gender AS c7,
            a9.c0 AS c8
          FROM (
            (
              (
                SELECT DISTINCT
                  a10.ss_customer_sk AS c0
                FROM (
                  store_sales AS a10
                    INNER JOIN date_dim AS a11
                      ON (
                        (
                          a10.ss_sold_date_sk = a11.d_date_sk
                        )
                        AND (
                          NOT a10.ss_customer_sk IS NULL
                        )
                        AND (
                          (
                            a10.ss_sold_date_sk <= 2452395
                          ) AND (
                            a10.ss_sold_date_sk >= 2452276
                          )
                        )
                        AND (
                          a11.d_year = 2002
                        )
                        AND (
                          1 <= a11.d_moy
                        )
                        AND (
                          a11.d_moy <= 4
                        )
                      )
                )
              ) AS a9
              INNER JOIN (
                customer AS a12
                  INNER JOIN customer_address AS a13
                    ON (
                      (
                        a12.c_current_addr_sk = a13.ca_address_sk
                      )
                      AND (
                        NOT a12.c_customer_sk IS NULL
                      )
                      AND (
                        a13.ca_county IN ('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County', 'La Porte County')
                      )
                    )
              )
                ON (
                  (
                    a12.c_customer_sk = a9.c0
                  ) AND (
                    NOT a9.c0 IS NULL
                  )
                )
            )
            INNER JOIN customer_demographics AS a14
              ON (
                a14.cd_demo_sk = a12.c_current_cdemo_sk
              )
          )
        ) AS a8
          ON (
            a8.c8 = a5.c0
          )
      )
    ) AS a4
      ON (
        a4.c9 = a1.c0
      )
  )
  WHERE
    (
      (
        NOT a4.c8 IS NULL
      ) OR (
        NOT a1.c0 IS NULL
      )
    )
  GROUP BY
    a4.c7,
    a4.c6,
    a4.c5,
    a4.c4,
    a4.c3,
    a4.c2,
    a4.c1,
    a4.c0
) AS a0
ORDER BY
  1 ASC NULLS LAST,
  2 ASC NULLS LAST,
  3 ASC NULLS LAST,
  5 ASC NULLS LAST,
  7 ASC NULLS LAST,
  9 ASC NULLS LAST,
  11 ASC NULLS LAST,
  13 ASC NULLS LAST
LIMIT 100