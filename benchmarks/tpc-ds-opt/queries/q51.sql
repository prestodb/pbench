SELECT
  a0.c0 AS ITEM_SK,
  a0.c1 AS D_DATE,
  a0.c2 AS WEB_SALES,
  a0.c3 AS STORE_SALES,
  a0.c4 AS WEB_CUMULATIVE,
  a0.c5 AS STORE_CUMULATIVE
FROM (
  SELECT
    a1.c0 AS c0,
    a1.c1 AS c1,
    a1.c2 AS c2,
    a1.c3 AS c3,
    MAX(a1.c2) OVER (PARTITION BY a1.c0 ORDER BY a1.c1 ASC NULLS LAST) AS c4,
    MAX(a1.c3) OVER (PARTITION BY a1.c0 ORDER BY a1.c1 ASC NULLS LAST) AS c5
  FROM (
    SELECT
      CASE WHEN (
        NOT a2.c0 IS NULL
      ) THEN a2.c0 ELSE a6.c0 END AS c0,
      CASE WHEN (
        NOT a2.c1 IS NULL
      ) THEN a2.c1 ELSE a6.c1 END AS c1,
      a2.c2 AS c2,
      a6.c2 AS c3
    FROM (
      (
        SELECT
          a3.c0 AS c0,
          a3.c1 AS c1,
          SUM(a3.c2) OVER (PARTITION BY a3.c0 ORDER BY a3.c1 ASC NULLS LAST) AS c2
        FROM (
          SELECT
            a4.ws_item_sk AS c0,
            a5.d_date AS c1,
            SUM(a4.ws_sales_price) AS c2
          FROM (
            web_sales AS a4
              INNER JOIN date_dim AS a5
                ON (
                  (
                    a4.ws_sold_date_sk = a5.d_date_sk
                  )
                  AND (
                    (
                      a4.ws_sold_date_sk <= 2451910
                    ) AND (
                      a4.ws_sold_date_sk >= 2451545
                    )
                  )
                  AND (
                    1200 <= a5.d_month_seq
                  )
                  AND (
                    a5.d_month_seq <= 1211
                  )
                )
          )
          GROUP BY
            a4.ws_item_sk,
            a5.d_date
        ) AS a3
      ) AS a2
      FULL OUTER JOIN (
        SELECT
          a7.c0 AS c0,
          a7.c1 AS c1,
          SUM(a7.c2) OVER (PARTITION BY a7.c0 ORDER BY a7.c1 ASC NULLS LAST) AS c2
        FROM (
          SELECT
            a8.ss_item_sk AS c0,
            a9.d_date AS c1,
            SUM(a8.ss_sales_price) AS c2
          FROM (
            store_sales AS a8
              INNER JOIN date_dim AS a9
                ON (
                  (
                    a8.ss_sold_date_sk = a9.d_date_sk
                  )
                  AND (
                    (
                      a8.ss_sold_date_sk <= 2451910
                    ) AND (
                      a8.ss_sold_date_sk >= 2451545
                    )
                  )
                  AND (
                    1200 <= a9.d_month_seq
                  )
                  AND (
                    a9.d_month_seq <= 1211
                  )
                )
          )
          GROUP BY
            a8.ss_item_sk,
            a9.d_date
        ) AS a7
      ) AS a6
        ON (
          a2.c0 = a6.c0
        ) AND (
          a2.c1 = a6.c1
        )
    )
  ) AS a1
) AS a0
WHERE
  (
    a0.c5 < a0.c4
  )
ORDER BY
  1 ASC NULLS LAST,
  2 ASC NULLS LAST
LIMIT 100