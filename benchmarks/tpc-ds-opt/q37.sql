SELECT
  a2.i_item_id AS I_ITEM_ID,
  a2.i_item_desc AS I_ITEM_DESC,
  a2.i_current_price AS I_CURRENT_PRICE
FROM (
  catalog_sales AS a0
    INNER JOIN (
      (
        inventory AS a1
          INNER JOIN item AS a2
            ON (
              (
                a1.inv_item_sk = a2.i_item_sk
              )
              AND (
                100 <= a1.inv_quantity_on_hand
              )
              AND (
                a1.inv_quantity_on_hand <= 500
              )
              AND (
                (
                  a1.inv_date_sk <= 2451636
                ) AND (
                  a1.inv_date_sk >= 2451576
                )
              )
              AND (
                (
                  a1.inv_item_sk <= 392790
                ) AND (
                  a1.inv_item_sk >= 13939
                )
              )
              AND (
                a2.i_manufact_id IN (677, 940, 694, 808)
              )
              AND (
                68 <= a2.i_current_price
              )
              AND (
                a2.i_current_price <= 98
              )
            )
      )
      INNER JOIN date_dim AS a3
        ON (
          (
            a3.d_date_sk = a1.inv_date_sk
          )
          AND (
            CAST('2000-02-01' AS DATE) <= a3.d_date
          )
          AND (
            a3.d_date <= CAST('2000-04-01' AS DATE)
          )
        )
    )
      ON (
        (
          a1.inv_item_sk = a0.cs_item_sk
        )
        AND (
          (
            a0.cs_item_sk <= 392790
          ) AND (
            a0.cs_item_sk >= 13939
          )
        )
      )
)
GROUP BY
  a2.i_item_id,
  a2.i_item_desc,
  a2.i_current_price
ORDER BY
  1 ASC NULLS LAST
LIMIT 100