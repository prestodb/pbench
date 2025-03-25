SELECT
  SUM(a0.c0) AS `Excess_Discount_Amount`
FROM (
  SELECT
    a1.ws_ext_discount_amt AS c0,
    COUNT(a1.ws_ext_discount_amt) OVER (PARTITION BY a1.ws_item_sk) AS c1,
    SUM(a1.ws_ext_discount_amt) OVER (PARTITION BY a1.ws_item_sk) AS c2
  FROM (
    (
      web_sales AS a1
        INNER JOIN item AS a2
          ON (
            (
              a1.ws_item_sk = a2.i_item_sk
            )
            AND (
              (
                a1.ws_item_sk <= 401779
              ) AND (
                a1.ws_item_sk >= 4226
              )
            )
            AND (
              (
                a1.ws_sold_date_sk <= 2451661
              ) AND (
                a1.ws_sold_date_sk >= 2451571
              )
            )
            AND (
              a2.i_manufact_id = 350
            )
          )
    )
    INNER JOIN date_dim AS a3
      ON (
        (
          a3.d_date_sk = a1.ws_sold_date_sk
        )
        AND (
          CAST('2000-01-27' AS DATE) <= a3.d_date
        )
        AND (
          a3.d_date <= CAST('2000-04-26' AS DATE)
        )
      )
  )
) AS a0
WHERE
  (
    (
      1.3 * CAST((
        a0.c2 / a0.c1
      ) AS DECIMAL(7, 2))
    ) < a0.c0
  )
ORDER BY
  1 ASC NULLS LAST
LIMIT 100