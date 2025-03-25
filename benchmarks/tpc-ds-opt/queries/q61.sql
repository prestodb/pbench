SELECT
  a0.c0 AS PROMOTIONS,
  a8.c0 AS TOTAL,
  (
    (
      CAST(a0.c0 AS DECIMAL(15, 4)) / CAST(a8.c0 AS DECIMAL(15, 4))
    ) * 100
  )
FROM (
  (
    SELECT
      SUM(a1.ss_ext_sales_price) AS c0
    FROM (
      (
        (
          (
            (
              store_sales AS a1
                INNER JOIN date_dim AS a2
                  ON (
                    (
                      a1.ss_sold_date_sk = a2.d_date_sk
                    )
                    AND (
                      (
                        a1.ss_sold_date_sk <= 2451148
                      ) AND (
                        a1.ss_sold_date_sk >= 2451119
                      )
                    )
                    AND (
                      (
                        a1.ss_store_sk <= 1500
                      ) AND (
                        a1.ss_store_sk >= 2
                      )
                    )
                    AND (
                      (
                        a1.ss_item_sk <= 401996
                      ) AND (
                        a1.ss_item_sk >= 5
                      )
                    )
                    AND (
                      (
                        a1.ss_promo_sk <= 1998
                      ) AND (
                        a1.ss_promo_sk >= 4
                      )
                    )
                    AND (
                      a2.d_year = 1998
                    )
                    AND (
                      a2.d_moy = 11
                    )
                  )
            )
            INNER JOIN item AS a3
              ON (
                (
                  a1.ss_item_sk = a3.i_item_sk
                ) AND (
                  a3.i_category = 'Jewelry'
                )
              )
          )
          INNER JOIN (
            customer AS a4
              INNER JOIN customer_address AS a5
                ON (
                  (
                    a5.ca_address_sk = a4.c_current_addr_sk
                  )
                  AND (
                    a5.ca_gmt_offset = -005.00
                  )
                )
          )
            ON (
              a1.ss_customer_sk = a4.c_customer_sk
            )
        )
        INNER JOIN store AS a6
          ON (
            (
              a1.ss_store_sk = a6.s_store_sk
            )
            AND (
              a5.ca_gmt_offset = a6.s_gmt_offset
            )
            AND (
              a6.s_gmt_offset = -005.00
            )
          )
      )
      INNER JOIN promotion AS a7
        ON (
          (
            a1.ss_promo_sk = a7.p_promo_sk
          )
          AND (
            (
              (
                a7.p_channel_dmail = 'Y'
              ) OR (
                a7.p_channel_email = 'Y'
              )
            )
            OR (
              a7.p_channel_tv = 'Y'
            )
          )
        )
    )
  ) AS a0
  INNER JOIN (
    SELECT
      SUM(a9.ss_ext_sales_price) AS c0
    FROM (
      (
        (
          (
            store_sales AS a9
              INNER JOIN date_dim AS a10
                ON (
                  (
                    a9.ss_sold_date_sk = a10.d_date_sk
                  )
                  AND (
                    (
                      a9.ss_sold_date_sk <= 2451148
                    ) AND (
                      a9.ss_sold_date_sk >= 2451119
                    )
                  )
                  AND (
                    (
                      a9.ss_store_sk <= 1500
                    ) AND (
                      a9.ss_store_sk >= 2
                    )
                  )
                  AND (
                    (
                      a9.ss_item_sk <= 401996
                    ) AND (
                      a9.ss_item_sk >= 5
                    )
                  )
                  AND (
                    a10.d_year = 1998
                  )
                  AND (
                    a10.d_moy = 11
                  )
                )
          )
          INNER JOIN item AS a11
            ON (
              (
                a9.ss_item_sk = a11.i_item_sk
              ) AND (
                a11.i_category = 'Jewelry'
              )
            )
        )
        INNER JOIN (
          customer AS a12
            INNER JOIN customer_address AS a13
              ON (
                (
                  a13.ca_address_sk = a12.c_current_addr_sk
                )
                AND (
                  a13.ca_gmt_offset = -005.00
                )
              )
        )
          ON (
            a9.ss_customer_sk = a12.c_customer_sk
          )
      )
      INNER JOIN store AS a14
        ON (
          (
            a9.ss_store_sk = a14.s_store_sk
          )
          AND (
            a13.ca_gmt_offset = a14.s_gmt_offset
          )
          AND (
            a14.s_gmt_offset = -005.00
          )
        )
    )
  ) AS a8
    ON (
      1 = 1
    )
)
ORDER BY
  1 ASC NULLS LAST,
  2 ASC NULLS LAST
LIMIT 100