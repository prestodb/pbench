SELECT
  a2.d_year AS D_YEAR,
  a1.i_brand_id AS BRAND_ID,
  a1.i_brand AS BRAND,
  SUM(a0.ss_ext_sales_price) AS SUM_AGG
FROM (
  (
    store_sales AS a0
      INNER JOIN item AS a1
        ON (
          (
            a0.ss_item_sk = a1.i_item_sk
          )
          AND (
            (
              a0.ss_sold_date_sk <= 2488038
            ) AND (
              a0.ss_sold_date_sk >= 2415325
            )
          )
          AND (
            (
              a0.ss_item_sk <= 401772
            ) AND (
              a0.ss_item_sk >= 127
            )
          )
          AND (
            a1.i_manufact_id = 128
          )
        )
  )
  INNER JOIN date_dim AS a2
    ON (
      (
        a2.d_date_sk = a0.ss_sold_date_sk
      ) AND (
        a2.d_moy = 11
      )
    )
)
GROUP BY
  a2.d_year,
  a1.i_brand,
  a1.i_brand_id
ORDER BY
  1 ASC NULLS LAST,
  4 DESC,
  2 ASC NULLS LAST
LIMIT 100