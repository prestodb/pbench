SELECT
  a0.c0 AS I_ITEM_ID,
  a0.c1 AS I_ITEM_DESC,
  a0.c2 AS S_STATE,
  a0.c3 AS STORE_SALES_QUANTITYCOUNT,
  (
    a0.c13 / a0.c14
  ) AS STORE_SALES_QUANTITYAVE,
  a0.c4 AS STORE_SALES_QUANTITYSTDEV,
  (
    a0.c4 / (
      a0.c13 / a0.c14
    )
  ) AS STORE_SALES_QUANTITYCOV,
  a0.c5 AS AS_STORE_RETURNS_QUANTITYCOUNT,
  (
    a0.c11 / a0.c12
  ) AS AS_STORE_RETURNS_QUANTITYAVE,
  a0.c6 AS AS_STORE_RETURNS_QUANTITYSTDEV,
  (
    a0.c6 / (
      a0.c11 / a0.c12
    )
  ) AS STORE_RETURNS_QUANTITYCOV,
  a0.c7 AS CATALOG_SALES_QUANTITYCOUNT,
  (
    a0.c9 / a0.c10
  ) AS CATALOG_SALES_QUANTITYAVE,
  (
    a0.c8 / (
      a0.c9 / a0.c10
    )
  ) AS CATALOG_SALES_QUANTITYSTDEV,
  (
    a0.c8 / (
      a0.c9 / a0.c10
    )
  ) AS CATALOG_SALES_QUANTITYCOV
FROM (
  SELECT
    a7.i_item_id AS c0,
    a7.i_item_desc AS c1,
    a8.s_state AS c2,
    COUNT(a1.ss_quantity) AS c3,
    STDDEV_SAMP(a1.ss_quantity) AS c4,
    COUNT(a5.sr_return_quantity) AS c5,
    STDDEV_SAMP(a5.sr_return_quantity) AS c6,
    COUNT(a3.cs_quantity) AS c7,
    STDDEV_SAMP(a3.cs_quantity) AS c8,
    SUM(CAST(a3.cs_quantity AS DOUBLE)) AS c9,
    COUNT(CAST(a3.cs_quantity AS DOUBLE)) AS c10,
    SUM(CAST(a5.sr_return_quantity AS DOUBLE)) AS c11,
    COUNT(CAST(a5.sr_return_quantity AS DOUBLE)) AS c12,
    SUM(CAST(a1.ss_quantity AS DOUBLE)) AS c13,
    COUNT(CAST(a1.ss_quantity AS DOUBLE)) AS c14
  FROM (
    (
      (
        store_sales AS a1
          INNER JOIN date_dim AS a2
            ON (
              (
                a2.d_date_sk = a1.ss_sold_date_sk
              )
              AND (
                (
                  a1.ss_sold_date_sk <= 2452001
                ) AND (
                  a1.ss_sold_date_sk >= 2451911
                )
              )
              AND (
                a2.d_quarter_name = '2001Q1'
              )
            )
      )
      INNER JOIN (
        (
          (
            catalog_sales AS a3
              INNER JOIN date_dim AS a4
                ON (
                  (
                    a3.cs_sold_date_sk = a4.d_date_sk
                  )
                  AND (
                    (
                      a3.cs_sold_date_sk <= 2452184
                    ) AND (
                      a3.cs_sold_date_sk >= 2451911
                    )
                  )
                  AND (
                    a4.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3')
                  )
                )
          )
          INNER JOIN (
            store_returns AS a5
              INNER JOIN date_dim AS a6
                ON (
                  (
                    a5.sr_returned_date_sk = a6.d_date_sk
                  )
                  AND (
                    (
                      a5.sr_returned_date_sk <= 2452184
                    )
                    AND (
                      a5.sr_returned_date_sk >= 2451911
                    )
                  )
                  AND (
                    a6.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3')
                  )
                )
          )
            ON (
              a5.sr_customer_sk = a3.cs_bill_customer_sk
            )
            AND (
              a5.sr_item_sk = a3.cs_item_sk
            )
        )
        INNER JOIN item AS a7
          ON (
            a7.i_item_sk = a5.sr_item_sk
          )
      )
        ON (
          a1.ss_ticket_number = a5.sr_ticket_number
        )
        AND (
          a1.ss_item_sk = a5.sr_item_sk
        )
        AND (
          a1.ss_customer_sk = a5.sr_customer_sk
        )
    )
    INNER JOIN store AS a8
      ON (
        a8.s_store_sk = a1.ss_store_sk
      )
  )
  GROUP BY
    a7.i_item_id,
    a7.i_item_desc,
    a8.s_state
) AS a0
ORDER BY
  1 ASC NULLS LAST,
  2 ASC NULLS LAST,
  3 ASC NULLS LAST
LIMIT 100