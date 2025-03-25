SELECT
  a6.i_item_id AS I_ITEM_ID,
  a6.i_item_desc AS I_ITEM_DESC,
  a7.s_store_id AS S_STORE_ID,
  a7.s_store_name AS S_STORE_NAME,
  SUM(a0.ss_net_profit) AS STORE_SALES_PROFIT,
  SUM(a4.sr_net_loss) AS STORE_RETURNS_LOSS,
  SUM(a2.cs_net_profit) AS CATALOG_SALES_PROFIT
FROM (
  (
    (
      store_sales AS a0
        INNER JOIN date_dim AS a1
          ON (
            (
              a1.d_date_sk = a0.ss_sold_date_sk
            )
            AND (
              (
                a0.ss_sold_date_sk <= 2452030
              ) AND (
                a0.ss_sold_date_sk >= 2452001
              )
            )
            AND (
              a1.d_moy = 4
            )
            AND (
              a1.d_year = 2001
            )
          )
    )
    INNER JOIN (
      (
        (
          catalog_sales AS a2
            INNER JOIN date_dim AS a3
              ON (
                (
                  a2.cs_sold_date_sk = a3.d_date_sk
                )
                AND (
                  (
                    a2.cs_sold_date_sk <= 2452214
                  ) AND (
                    a2.cs_sold_date_sk >= 2452001
                  )
                )
                AND (
                  4 <= a3.d_moy
                )
                AND (
                  a3.d_moy <= 10
                )
                AND (
                  a3.d_year = 2001
                )
              )
        )
        INNER JOIN (
          store_returns AS a4
            INNER JOIN date_dim AS a5
              ON (
                (
                  a4.sr_returned_date_sk = a5.d_date_sk
                )
                AND (
                  (
                    a4.sr_returned_date_sk <= 2452214
                  )
                  AND (
                    a4.sr_returned_date_sk >= 2452001
                  )
                )
                AND (
                  4 <= a5.d_moy
                )
                AND (
                  a5.d_moy <= 10
                )
                AND (
                  a5.d_year = 2001
                )
              )
        )
          ON (
            a4.sr_customer_sk = a2.cs_bill_customer_sk
          )
          AND (
            a4.sr_item_sk = a2.cs_item_sk
          )
          AND (
            a5.d_year = a3.d_year
          )
      )
      INNER JOIN item AS a6
        ON (
          a6.i_item_sk = a4.sr_item_sk
        )
    )
      ON (
        a0.ss_ticket_number = a4.sr_ticket_number
      )
      AND (
        a1.d_year = a3.d_year
      )
      AND (
        a0.ss_item_sk = a4.sr_item_sk
      )
      AND (
        a0.ss_customer_sk = a4.sr_customer_sk
      )
  )
  INNER JOIN store AS a7
    ON (
      a7.s_store_sk = a0.ss_store_sk
    )
)
GROUP BY
  a6.i_item_id,
  a6.i_item_desc,
  a7.s_store_id,
  a7.s_store_name
ORDER BY
  1 ASC NULLS LAST,
  2 ASC NULLS LAST,
  3 ASC NULLS LAST,
  4 ASC NULLS LAST
LIMIT 100