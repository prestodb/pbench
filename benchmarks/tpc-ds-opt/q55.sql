SELECT
  a2.i_brand_id AS BRAND_ID,
  a2.i_brand AS BRAND,
  SUM(a0.ss_ext_sales_price) AS EXT_PRICE
FROM (
  (
    store_sales AS a0
      INNER JOIN date_dim AS a1
        ON (
          (
            a1.d_date_sk = a0.ss_sold_date_sk
          )
          AND (
            (
              a0.ss_sold_date_sk <= 2451513
            ) AND (
              a0.ss_sold_date_sk >= 2451484
            )
          )
          AND (
            (
              a0.ss_item_sk <= 401995
            ) AND (
              a0.ss_item_sk >= 45
            )
          )
          AND (
            a1.d_moy = 11
          )
          AND (
            a1.d_year = 1999
          )
        )
  )
  INNER JOIN item AS a2
    ON (
      (
        a0.ss_item_sk = a2.i_item_sk
      ) AND (
        a2.i_manager_id = 28
      )
    )
)
GROUP BY
  a2.i_brand,
  a2.i_brand_id
ORDER BY
  3 DESC,
  1 ASC NULLS LAST
LIMIT 100