SELECT
  a0.c0 AS CHANNEL,
  a0.c1 AS COL_NAME,
  a0.c2 AS D_YEAR,
  a0.c3 AS D_QOY,
  a0.c4 AS I_CATEGORY,
  SUM(a0.c6) AS SALES_CNT,
  SUM(a0.c5) AS SALES_AMT
FROM (
  (
    SELECT
      CAST('store' AS STRING) AS c0,
      a1.c0 AS c1,
      a1.c1 AS c2,
      a1.c2 AS c3,
      a1.c3 AS c4,
      a1.c4 AS c5,
      a1.c5 AS c6
    FROM (
      SELECT
        a2.ss_store_sk AS c0,
        a4.d_year AS c1,
        a4.d_qoy AS c2,
        a3.i_category AS c3,
        SUM(a2.ss_ext_sales_price) AS c4,
        COUNT(*) AS c5
      FROM (
        (
          store_sales AS a2
            INNER JOIN item AS a3
              ON (
                (
                  a2.ss_item_sk = a3.i_item_sk
                ) AND (
                  a2.ss_store_sk IS NULL
                )
              )
        )
        INNER JOIN date_dim AS a4
          ON (
            a2.ss_sold_date_sk = a4.d_date_sk
          )
      )
      GROUP BY
        a3.i_category,
        a4.d_qoy,
        a4.d_year,
        a2.ss_store_sk
    ) AS a1
  )
  UNION ALL
  (
    SELECT
      CAST(CAST('web' AS STRING) AS STRING) AS c0,
      a5.c0 AS c1,
      a5.c1 AS c2,
      a5.c2 AS c3,
      a5.c3 AS c4,
      a5.c4 AS c5,
      a5.c5 AS c6
    FROM (
      SELECT
        a6.ws_ship_customer_sk AS c0,
        a8.d_year AS c1,
        a8.d_qoy AS c2,
        a7.i_category AS c3,
        SUM(a6.ws_ext_sales_price) AS c4,
        COUNT(*) AS c5
      FROM (
        (
          web_sales AS a6
            INNER JOIN item AS a7
              ON (
                (
                  a6.ws_item_sk = a7.i_item_sk
                ) AND (
                  a6.ws_ship_customer_sk IS NULL
                )
              )
        )
        INNER JOIN date_dim AS a8
          ON (
            a6.ws_sold_date_sk = a8.d_date_sk
          )
      )
      GROUP BY
        a7.i_category,
        a8.d_qoy,
        a8.d_year,
        a6.ws_ship_customer_sk
    ) AS a5
  )
  UNION ALL
  (
    SELECT
      'catalog' AS c0,
      a9.cs_ship_addr_sk AS c1,
      a11.d_year AS c2,
      a11.d_qoy AS c3,
      a10.i_category AS c4,
      SUM(a9.cs_ext_sales_price) AS c5,
      COUNT(*) AS c6
    FROM (
      (
        catalog_sales AS a9
          INNER JOIN item AS a10
            ON (
              (
                a9.cs_item_sk = a10.i_item_sk
              ) AND (
                a9.cs_ship_addr_sk IS NULL
              )
            )
      )
      INNER JOIN date_dim AS a11
        ON (
          a9.cs_sold_date_sk = a11.d_date_sk
        )
    )
    GROUP BY
      a10.i_category,
      a11.d_qoy,
      a11.d_year,
      a9.cs_ship_addr_sk,
      'catalog'
  )
) AS a0
GROUP BY
  a0.c0,
  a0.c1,
  a0.c2,
  a0.c3,
  a0.c4
ORDER BY
  1 ASC NULLS LAST,
  2 ASC NULLS LAST,
  3 ASC NULLS LAST,
  4 ASC NULLS LAST,
  5 ASC NULLS LAST
LIMIT 100