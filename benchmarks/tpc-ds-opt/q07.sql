SELECT
  a0.c0 AS I_ITEM_ID,
  (
    a0.c7 / a0.c8
  ) AS AGG1,
  CAST((
    a0.c5 / a0.c6
  ) AS DECIMAL(7, 2)) AS AGG2,
  CAST((
    a0.c3 / a0.c4
  ) AS DECIMAL(7, 2)) AS AGG3,
  CAST((
    a0.c1 / a0.c2
  ) AS DECIMAL(7, 2)) AS AGG4
FROM (
  SELECT
    a4.i_item_id AS c0,
    SUM(a1.ss_sales_price) AS c1,
    COUNT(a1.ss_sales_price) AS c2,
    SUM(a1.ss_coupon_amt) AS c3,
    COUNT(a1.ss_coupon_amt) AS c4,
    SUM(a1.ss_list_price) AS c5,
    COUNT(a1.ss_list_price) AS c6,
    SUM(CAST(a1.ss_quantity AS DOUBLE)) AS c7,
    COUNT(CAST(a1.ss_quantity AS DOUBLE)) AS c8
  FROM (
    (
      (
        (
          store_sales AS a1
            INNER JOIN customer_demographics AS a2
              ON (
                (
                  a1.ss_cdemo_sk = a2.cd_demo_sk
                )
                AND (
                  (
                    a1.ss_sold_date_sk <= 2451910
                  ) AND (
                    a1.ss_sold_date_sk >= 2451545
                  )
                )
                AND (
                  (
                    a1.ss_cdemo_sk <= 1920753
                  ) AND (
                    a1.ss_cdemo_sk >= 23
                  )
                )
                AND (
                  (
                    a1.ss_promo_sk <= 2000
                  ) AND (
                    a1.ss_promo_sk >= 1
                  )
                )
                AND (
                  a2.cd_gender = 'M'
                )
                AND (
                  a2.cd_marital_status = 'S'
                )
                AND (
                  a2.cd_education_status = 'College'
                )
              )
        )
        INNER JOIN date_dim AS a3
          ON (
            (
              a1.ss_sold_date_sk = a3.d_date_sk
            ) AND (
              a3.d_year = 2000
            )
          )
      )
      INNER JOIN item AS a4
        ON (
          a1.ss_item_sk = a4.i_item_sk
        )
    )
    INNER JOIN promotion AS a5
      ON (
        (
          a1.ss_promo_sk = a5.p_promo_sk
        )
        AND (
          (
            a5.p_channel_email = 'N'
          ) OR (
            a5.p_channel_event = 'N'
          )
        )
      )
  )
  GROUP BY
    a4.i_item_id
) AS a0
ORDER BY
  1 ASC NULLS LAST
LIMIT 100