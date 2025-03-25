SELECT
  a1.c2 AS I_ITEM_DESC,
  a1.c3 AS W_WAREHOUSE_NAME,
  a1.c1 AS D_WEEK_SEQ,
  SUM(CASE WHEN (
    a0.p_promo_sk IS NULL
  ) THEN 1 ELSE 0 END) AS NO_PROMO,
  SUM(CASE WHEN (
    NOT a0.p_promo_sk IS NULL
  ) THEN 1 ELSE 0 END) AS PROMO,
  COUNT(*) AS TOTAL_CNT
FROM (
  promotion AS a0
    RIGHT OUTER JOIN (
      SELECT
        a3.cs_promo_sk AS c0,
        a7.d_week_seq AS c1,
        a8.i_item_desc AS c2,
        a10.w_warehouse_name AS c3
      FROM (
        (
          inventory AS a2
            INNER JOIN (
              (
                (
                  (
                    (
                      (
                        catalog_sales AS a3
                          INNER JOIN household_demographics AS a4
                            ON (
                              (
                                a3.cs_bill_hdemo_sk = a4.hd_demo_sk
                              )
                              AND (
                                (
                                  a3.cs_bill_cdemo_sk <= 1920796
                                ) AND (
                                  a3.cs_bill_cdemo_sk >= 5
                                )
                              )
                              AND (
                                (
                                  a3.cs_sold_date_sk <= 2451544
                                ) AND (
                                  a3.cs_sold_date_sk >= 2451180
                                )
                              )
                              AND (
                                (
                                  a3.cs_bill_hdemo_sk <= 7179
                                ) AND (
                                  a3.cs_bill_hdemo_sk >= 80
                                )
                              )
                              AND (
                                a4.hd_buy_potential = '>10000'
                              )
                            )
                      )
                      INNER JOIN customer_demographics AS a5
                        ON (
                          (
                            a3.cs_bill_cdemo_sk = a5.cd_demo_sk
                          ) AND (
                            a5.cd_marital_status = 'D'
                          )
                        )
                    )
                    INNER JOIN date_dim AS a6
                      ON (
                        a3.cs_ship_date_sk = a6.d_date_sk
                      )
                  )
                  INNER JOIN date_dim AS a7
                    ON (
                      (
                        a3.cs_sold_date_sk = a7.d_date_sk
                      )
                      AND (
                        (
                          a7.d_date + INTERVAL '5' DAY
                        ) < a6.d_date
                      )
                      AND (
                        a7.d_year = 1999
                      )
                    )
                )
                INNER JOIN item AS a8
                  ON (
                    a8.i_item_sk = a3.cs_item_sk
                  )
              )
              INNER JOIN date_dim AS a9
                ON (
                  (
                    a7.d_week_seq = a9.d_week_seq
                  )
                  AND (
                    (
                      a9.d_week_seq <= 5218
                    ) AND (
                      a9.d_week_seq >= 5166
                    )
                  )
                )
            )
              ON (
                (
                  a2.inv_date_sk = a9.d_date_sk
                )
                AND (
                  a3.cs_item_sk = a2.inv_item_sk
                )
                AND (
                  a2.inv_quantity_on_hand < a3.cs_quantity
                )
                AND (
                  (
                    a2.inv_date_sk <= 2451547
                  ) AND (
                    a2.inv_date_sk >= 2451177
                  )
                )
              )
        )
        INNER JOIN warehouse AS a10
          ON (
            a10.w_warehouse_sk = a2.inv_warehouse_sk
          )
      )
    ) AS a1
      ON (
        a1.c0 = a0.p_promo_sk
      )
)
GROUP BY
  a1.c2,
  a1.c3,
  a1.c1
ORDER BY
  6 DESC,
  1 ASC NULLS LAST,
  2 ASC NULLS LAST,
  3 ASC NULLS LAST
LIMIT 100