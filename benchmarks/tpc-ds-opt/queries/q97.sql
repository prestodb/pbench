SELECT
  SUM(
    CASE
      WHEN (
        (
          NOT a0.c0 IS NULL
        ) AND (
          a3.c0 IS NULL
        )
      )
      THEN 1
      ELSE 0
    END
  ) AS STORE_ONLY,
  SUM(
    CASE
      WHEN (
        (
          a0.c0 IS NULL
        ) AND (
          NOT a3.c0 IS NULL
        )
      )
      THEN 1
      ELSE 0
    END
  ) AS CATALOG_ONLY,
  SUM(
    CASE
      WHEN (
        (
          NOT a0.c0 IS NULL
        ) AND (
          NOT a3.c0 IS NULL
        )
      )
      THEN 1
      ELSE 0
    END
  ) AS STORE_AND_CATALOG
FROM (
  (
    SELECT
      a1.ss_customer_sk AS c0,
      a1.ss_item_sk AS c1
    FROM (
      store_sales AS a1
        INNER JOIN date_dim AS a2
          ON (
            (
              a1.ss_sold_date_sk = a2.d_date_sk
            )
            AND (
              (
                a1.ss_sold_date_sk <= 2451910
              ) AND (
                a1.ss_sold_date_sk >= 2451545
              )
            )
            AND (
              1200 <= a2.d_month_seq
            )
            AND (
              a2.d_month_seq <= 1211
            )
          )
    )
    GROUP BY
      a1.ss_customer_sk,
      a1.ss_item_sk
  ) AS a0
  FULL OUTER JOIN (
    SELECT
      a4.cs_bill_customer_sk AS c0,
      a4.cs_item_sk AS c1
    FROM (
      catalog_sales AS a4
        INNER JOIN date_dim AS a5
          ON (
            (
              a4.cs_sold_date_sk = a5.d_date_sk
            )
            AND (
              (
                a4.cs_sold_date_sk <= 2451910
              ) AND (
                a4.cs_sold_date_sk >= 2451545
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
      a4.cs_bill_customer_sk,
      a4.cs_item_sk
  ) AS a3
    ON (
      a0.c0 = a3.c0
    ) AND (
      a0.c1 = a3.c1
    )
)
LIMIT 100