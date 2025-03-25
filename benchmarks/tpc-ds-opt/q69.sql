SELECT
  a0.c0 AS CD_GENDER,
  a0.c1 AS CD_MARITAL_STATUS,
  a0.c2 AS CD_EDUCATION_STATUS,
  a0.c3 AS CNT1,
  a0.c4 AS CD_PURCHASE_ESTIMATE,
  a0.c3 AS CNT2,
  a0.c5 AS CD_CREDIT_RATING,
  a0.c3 AS CNT3
FROM (
  SELECT
    a5.c4 AS c0,
    a5.c3 AS c1,
    a5.c2 AS c2,
    COUNT(*) AS c3,
    a5.c1 AS c4,
    a5.c0 AS c5
  FROM (
    (
      SELECT
        a2.c0 AS c0,
        '1' AS c1
      FROM (
        SELECT DISTINCT
          a3.ws_bill_customer_sk AS c0
        FROM (
          web_sales AS a3
            INNER JOIN date_dim AS a4
              ON (
                (
                  a3.ws_sold_date_sk = a4.d_date_sk
                )
                AND (
                  NOT a3.ws_bill_customer_sk IS NULL
                )
                AND (
                  (
                    a3.ws_sold_date_sk <= 2452091
                  ) AND (
                    a3.ws_sold_date_sk >= 2452001
                  )
                )
                AND (
                  a4.d_year = 2001
                )
                AND (
                  4 <= a4.d_moy
                )
                AND (
                  a4.d_moy <= 6
                )
              )
        )
      ) AS a2
    ) AS a1
    RIGHT OUTER JOIN (
      SELECT
        a10.c0 AS c0,
        a10.c1 AS c1,
        a10.c2 AS c2,
        a10.c3 AS c3,
        a10.c4 AS c4,
        a10.c5 AS c5
      FROM (
        (
          SELECT
            a7.c1 AS c0,
            '1' AS c1
          FROM (
            SELECT DISTINCT
              a9.d_moy AS c0,
              a8.cs_ship_customer_sk AS c1
            FROM (
              catalog_sales AS a8
                INNER JOIN date_dim AS a9
                  ON (
                    (
                      a8.cs_sold_date_sk = a9.d_date_sk
                    )
                    AND (
                      (
                        a8.cs_sold_date_sk <= 2452091
                      ) AND (
                        a8.cs_sold_date_sk >= 2452001
                      )
                    )
                    AND (
                      a9.d_year = 2001
                    )
                    AND (
                      4 <= a9.d_moy
                    )
                    AND (
                      a9.d_moy <= 6
                    )
                  )
            )
          ) AS a7
        ) AS a6
        RIGHT OUTER JOIN (
          SELECT
            a16.cd_credit_rating AS c0,
            a16.cd_purchase_estimate AS c1,
            a16.cd_education_status AS c2,
            a16.cd_marital_status AS c3,
            a16.cd_gender AS c4,
            a11.c0 AS c5
          FROM (
            (
              (
                SELECT DISTINCT
                  a12.ss_customer_sk AS c0
                FROM (
                  store_sales AS a12
                    INNER JOIN date_dim AS a13
                      ON (
                        (
                          a12.ss_sold_date_sk = a13.d_date_sk
                        )
                        AND (
                          NOT a12.ss_customer_sk IS NULL
                        )
                        AND (
                          (
                            a12.ss_sold_date_sk <= 2452091
                          ) AND (
                            a12.ss_sold_date_sk >= 2452001
                          )
                        )
                        AND (
                          a13.d_year = 2001
                        )
                        AND (
                          4 <= a13.d_moy
                        )
                        AND (
                          a13.d_moy <= 6
                        )
                      )
                )
              ) AS a11
              INNER JOIN (
                customer AS a14
                  INNER JOIN customer_address AS a15
                    ON (
                      (
                        a14.c_current_addr_sk = a15.ca_address_sk
                      )
                      AND (
                        NOT a14.c_customer_sk IS NULL
                      )
                      AND (
                        a15.ca_state IN ('KY', 'GA', 'NM')
                      )
                    )
              )
                ON (
                  (
                    a14.c_customer_sk = a11.c0
                  ) AND (
                    NOT a11.c0 IS NULL
                  )
                )
            )
            INNER JOIN customer_demographics AS a16
              ON (
                a16.cd_demo_sk = a14.c_current_cdemo_sk
              )
          )
        ) AS a10
          ON (
            a10.c5 = a6.c0
          )
      )
      WHERE
        (
          a6.c1 IS NULL
        )
    ) AS a5
      ON (
        a5.c5 = a1.c0
      )
  )
  WHERE
    (
      a1.c1 IS NULL
    )
  GROUP BY
    a5.c4,
    a5.c3,
    a5.c2,
    a5.c1,
    a5.c0
) AS a0
ORDER BY
  1 ASC NULLS LAST,
  2 ASC NULLS LAST,
  3 ASC NULLS LAST,
  5 ASC NULLS LAST,
  7 ASC NULLS LAST
LIMIT 100