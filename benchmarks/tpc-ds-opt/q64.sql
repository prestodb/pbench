WITH a0 AS (
  SELECT
    a18.i_product_name AS c0,
    a18.i_item_sk AS c1,
    a14.s_store_name AS c2,
    a14.s_zip AS c3,
    a2.ca_street_number AS c4,
    a2.ca_street_name AS c5,
    a2.ca_city AS c6,
    a2.ca_zip AS c7,
    a1.ca_street_number AS c8,
    a1.ca_street_name AS c9,
    a1.ca_city AS c10,
    a1.ca_zip AS c11,
    a5.d_year AS c12,
    COUNT(*) AS c13,
    SUM(a4.ss_wholesale_cost) AS c14,
    SUM(a4.ss_list_price) AS c15,
    SUM(a4.ss_coupon_amt) AS c16
  FROM (
    customer_address AS a1
      INNER JOIN (
        (
          customer_address AS a2
            INNER JOIN (
              (
                (
                  (
                    (
                      (
                        (
                          customer AS a3
                            INNER JOIN (
                              (
                                (
                                  store_sales AS a4
                                    INNER JOIN date_dim AS a5
                                      ON (
                                        (
                                          a4.ss_sold_date_sk = a5.d_date_sk
                                        )
                                        AND (
                                          NOT a4.ss_promo_sk IS NULL
                                        )
                                        AND (
                                          (
                                            a4.ss_item_sk <= 401370
                                          ) AND (
                                            a4.ss_item_sk >= 307
                                          )
                                        )
                                        AND (
                                          (
                                            a4.ss_hdemo_sk <= 7200
                                          ) AND (
                                            a4.ss_hdemo_sk >= 1
                                          )
                                        )
                                        AND (
                                          (
                                            a4.ss_sold_date_sk <= 2451910
                                          ) AND (
                                            a4.ss_sold_date_sk >= 2451180
                                          )
                                        )
                                        AND (
                                          a5.d_year IN (2000, 1999)
                                        )
                                      )
                                )
                                INNER JOIN (
                                  store_returns AS a6
                                    INNER JOIN (
                                      SELECT
                                        SUM(a8.cs_ext_list_price) AS c0,
                                        SUM((
                                          (
                                            a9.cr_refunded_cash + a9.cr_reversed_charge
                                          ) + a9.cr_store_credit
                                        )) AS c1,
                                        a8.cs_item_sk AS c2
                                      FROM (
                                        catalog_sales AS a8
                                          INNER JOIN (
                                            catalog_returns AS a9
                                              INNER JOIN item AS a10
                                                ON (
                                                  (
                                                    a9.cr_item_sk = a10.i_item_sk
                                                  )
                                                  AND (
                                                    (
                                                      a9.cr_item_sk <= 401370
                                                    ) AND (
                                                      a9.cr_item_sk >= 307
                                                    )
                                                  )
                                                  AND (
                                                    a10.i_color IN ('purple', 'burlywood', 'indian', 'spring', 'floral', 'medium')
                                                  )
                                                  AND (
                                                    a10.i_current_price <= 79
                                                  )
                                                  AND (
                                                    65 <= a10.i_current_price
                                                  )
                                                  AND (
                                                    a10.i_current_price <= 74
                                                  )
                                                  AND (
                                                    64 <= a10.i_current_price
                                                  )
                                                )
                                          )
                                            ON (
                                              (
                                                a8.cs_order_number = a9.cr_order_number
                                              )
                                              AND (
                                                a10.i_item_sk = a8.cs_item_sk
                                              )
                                              AND (
                                                (
                                                  a8.cs_item_sk <= 401370
                                                ) AND (
                                                  a8.cs_item_sk >= 307
                                                )
                                              )
                                            )
                                      )
                                      GROUP BY
                                        a8.cs_item_sk
                                    ) AS a7
                                      ON (
                                        (
                                          a7.c2 = a6.sr_item_sk
                                        )
                                        AND (
                                          (
                                            a6.sr_item_sk <= 401370
                                          ) AND (
                                            a6.sr_item_sk >= 307
                                          )
                                        )
                                        AND (
                                          (
                                            2 * a7.c1
                                          ) < a7.c0
                                        )
                                      )
                                )
                                  ON (
                                    a4.ss_ticket_number = a6.sr_ticket_number
                                  )
                                  AND (
                                    a6.sr_item_sk = a4.ss_item_sk
                                  )
                              )
                              INNER JOIN household_demographics AS a11
                                ON (
                                  (
                                    a4.ss_hdemo_sk = a11.hd_demo_sk
                                  ) AND (
                                    NOT a11.hd_income_band_sk IS NULL
                                  )
                                )
                            )
                              ON (
                                (
                                  a4.ss_customer_sk = a3.c_customer_sk
                                )
                                AND (
                                  (
                                    a3.c_current_hdemo_sk <= 7200
                                  ) AND (
                                    a3.c_current_hdemo_sk >= 1
                                  )
                                )
                              )
                        )
                        INNER JOIN date_dim AS a12
                          ON (
                            a3.c_first_sales_date_sk = a12.d_date_sk
                          )
                      )
                      INNER JOIN date_dim AS a13
                        ON (
                          a3.c_first_shipto_date_sk = a13.d_date_sk
                        )
                    )
                    INNER JOIN store AS a14
                      ON (
                        a4.ss_store_sk = a14.s_store_sk
                      )
                  )
                  INNER JOIN customer_demographics AS a15
                    ON (
                      a4.ss_cdemo_sk = a15.cd_demo_sk
                    )
                )
                INNER JOIN customer_demographics AS a16
                  ON (
                    a3.c_current_cdemo_sk = a16.cd_demo_sk
                  )
                  AND (
                    a15.cd_marital_status <> a16.cd_marital_status
                  )
              )
              INNER JOIN household_demographics AS a17
                ON (
                  (
                    a3.c_current_hdemo_sk = a17.hd_demo_sk
                  )
                  AND (
                    NOT a17.hd_income_band_sk IS NULL
                  )
                )
            )
              ON (
                a4.ss_addr_sk = a2.ca_address_sk
              )
        )
        INNER JOIN item AS a18
          ON (
            (
              a18.i_item_sk = a4.ss_item_sk
            )
            AND (
              a18.i_color IN ('purple', 'burlywood', 'indian', 'spring', 'floral', 'medium')
            )
            AND (
              64 <= a18.i_current_price
            )
            AND (
              a18.i_current_price <= 74
            )
            AND (
              65 <= a18.i_current_price
            )
            AND (
              a18.i_current_price <= 79
            )
          )
      )
        ON (
          a3.c_current_addr_sk = a1.ca_address_sk
        )
  )
  GROUP BY
    a18.i_item_sk,
    a14.s_store_name,
    a14.s_zip,
    a2.ca_street_number,
    a2.ca_street_name,
    a2.ca_city,
    a2.ca_zip,
    a1.ca_street_number,
    a1.ca_street_name,
    a1.ca_city,
    a1.ca_zip,
    a5.d_year,
    a12.d_year,
    a13.d_year,
    a18.i_product_name
)
SELECT
  A19.c0 AS PRODUCT_NAME,
  A19.c2 AS STORE_NAME,
  A19.c3 AS STORE_ZIP,
  A19.c4 AS B_STREET_NUMBER,
  A19.c5 AS B_STREEN_NAME,
  A19.c6 AS B_CITY,
  A19.c7 AS B_ZIP,
  A19.c8 AS C_STREET_NUMBER,
  A19.c9 AS C_STREET_NAME,
  A19.c10 AS C_CITY,
  A19.c11 AS C_ZIP,
  A19.c12 AS SYEAR,
  A19.c13 AS CNT,
  A19.c14 AS S1,
  A19.c15 AS S2,
  A19.c16 AS S3,
  A20.c14 AS S1,
  A20.c15 AS S2,
  A20.c16 AS S3,
  A20.c12 AS SYEAR,
  A20.c13 AS CNT
FROM (
  a0 AS A19
    INNER JOIN a0 AS A20
      ON (
        (
          A19.c1 = A20.c1
        )
        AND (
          A19.c2 = A20.c2
        )
        AND (
          A19.c3 = A20.c3
        )
        AND (
          A20.c13 <= A19.c13
        )
        AND (
          A19.c12 = 1999
        )
        AND (
          A20.c12 = 2000
        )
      )
)
ORDER BY
  1 ASC NULLS LAST,
  2 ASC NULLS LAST,
  21 ASC NULLS LAST