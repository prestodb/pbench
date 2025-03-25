SELECT
  a0.c2 AS BRAND_ID,
  a0.c3 AS BRAND,
  a0.c1 AS T_HOUR,
  a0.c0 AS T_MINUTE,
  SUM(a0.c4) AS EXT_PRICE
FROM (
  (
    SELECT
      a4.t_minute AS c0,
      a4.t_hour AS c1,
      a3.i_brand_id AS c2,
      a3.i_brand AS c3,
      SUM(a1.ws_ext_sales_price) AS c4
    FROM (
      (
        (
          web_sales AS a1
            INNER JOIN date_dim AS a2
              ON (
                (
                  a2.d_date_sk = a1.ws_sold_date_sk
                )
                AND (
                  (
                    a1.ws_item_sk <= 401980
                  ) AND (
                    a1.ws_item_sk >= 74
                  )
                )
                AND (
                  (
                    a1.ws_sold_date_sk <= 2451513
                  ) AND (
                    a1.ws_sold_date_sk >= 2451484
                  )
                )
                AND (
                  (
                    a1.ws_sold_time_sk <= 71999
                  ) AND (
                    a1.ws_sold_time_sk >= 21600
                  )
                )
                AND (
                  a2.d_moy = 11
                )
                AND (
                  a2.d_year = 1999
                )
              )
        )
        INNER JOIN item AS a3
          ON (
            (
              a1.ws_item_sk = a3.i_item_sk
            ) AND (
              a3.i_manager_id = 1
            )
          )
      )
      INNER JOIN time_dim AS a4
        ON (
          (
            a1.ws_sold_time_sk = a4.t_time_sk
          )
          AND (
            a4.t_meal_time IN ('breakfast', 'dinner')
          )
        )
    )
    GROUP BY
      a4.t_minute,
      a4.t_hour,
      a3.i_brand_id,
      a3.i_brand
  )
  UNION ALL
  (
    SELECT
      a8.t_minute AS c0,
      a8.t_hour AS c1,
      a7.i_brand_id AS c2,
      a7.i_brand AS c3,
      SUM(a5.cs_ext_sales_price) AS c4
    FROM (
      (
        (
          catalog_sales AS a5
            INNER JOIN date_dim AS a6
              ON (
                (
                  a6.d_date_sk = a5.cs_sold_date_sk
                )
                AND (
                  (
                    a5.cs_item_sk <= 401980
                  ) AND (
                    a5.cs_item_sk >= 74
                  )
                )
                AND (
                  (
                    a5.cs_sold_date_sk <= 2451513
                  ) AND (
                    a5.cs_sold_date_sk >= 2451484
                  )
                )
                AND (
                  (
                    a5.cs_sold_time_sk <= 71999
                  ) AND (
                    a5.cs_sold_time_sk >= 21600
                  )
                )
                AND (
                  a6.d_moy = 11
                )
                AND (
                  a6.d_year = 1999
                )
              )
        )
        INNER JOIN item AS a7
          ON (
            (
              a5.cs_item_sk = a7.i_item_sk
            ) AND (
              a7.i_manager_id = 1
            )
          )
      )
      INNER JOIN time_dim AS a8
        ON (
          (
            a5.cs_sold_time_sk = a8.t_time_sk
          )
          AND (
            a8.t_meal_time IN ('breakfast', 'dinner')
          )
        )
    )
    GROUP BY
      a8.t_minute,
      a8.t_hour,
      a7.i_brand_id,
      a7.i_brand
  )
  UNION ALL
  (
    SELECT
      a12.t_minute AS c0,
      a12.t_hour AS c1,
      a11.i_brand_id AS c2,
      a11.i_brand AS c3,
      SUM(a9.ss_ext_sales_price) AS c4
    FROM (
      (
        (
          store_sales AS a9
            INNER JOIN date_dim AS a10
              ON (
                (
                  a10.d_date_sk = a9.ss_sold_date_sk
                )
                AND (
                  (
                    a9.ss_item_sk <= 401980
                  ) AND (
                    a9.ss_item_sk >= 74
                  )
                )
                AND (
                  (
                    a9.ss_sold_date_sk <= 2451513
                  ) AND (
                    a9.ss_sold_date_sk >= 2451484
                  )
                )
                AND (
                  (
                    a9.ss_sold_time_sk <= 71999
                  ) AND (
                    a9.ss_sold_time_sk >= 21600
                  )
                )
                AND (
                  a10.d_moy = 11
                )
                AND (
                  a10.d_year = 1999
                )
              )
        )
        INNER JOIN item AS a11
          ON (
            (
              a9.ss_item_sk = a11.i_item_sk
            ) AND (
              a11.i_manager_id = 1
            )
          )
      )
      INNER JOIN time_dim AS a12
        ON (
          (
            a9.ss_sold_time_sk = a12.t_time_sk
          )
          AND (
            a12.t_meal_time IN ('breakfast', 'dinner')
          )
        )
    )
    GROUP BY
      a12.t_minute,
      a12.t_hour,
      a11.i_brand_id,
      a11.i_brand
  )
) AS a0
GROUP BY
  a0.c3,
  a0.c2,
  a0.c1,
  a0.c0
ORDER BY
  5 DESC,
  1 ASC NULLS LAST