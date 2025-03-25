SELECT
  a0.ss_customer_sk AS SS_CUSTOMER_SK,
  SUM(
    CASE
      WHEN (
        NOT a1.sr_return_quantity IS NULL
      )
      THEN (
        (
          a0.ss_quantity - a1.sr_return_quantity
        ) * a0.ss_sales_price
      )
      ELSE (
        a0.ss_quantity * a0.ss_sales_price
      )
    END
  ) AS SUMSALES
FROM (
  store_sales AS a0
    INNER JOIN (
      store_returns AS a1
        INNER JOIN reason AS a2
          ON (
            (
              a2.r_reason_sk = a1.sr_reason_sk
            )
            AND (
              (
                a1.sr_reason_sk <= 28
              ) AND (
                a1.sr_reason_sk >= 28
              )
            )
            AND (
              'reason 28' = a2.r_reason_desc
            )
          )
    )
      ON (
        a1.sr_ticket_number = a0.ss_ticket_number
      )
      AND (
        a1.sr_item_sk = a0.ss_item_sk
      )
)
GROUP BY
  a0.ss_customer_sk
ORDER BY
  2 ASC NULLS LAST,
  1 ASC NULLS LAST
LIMIT 100