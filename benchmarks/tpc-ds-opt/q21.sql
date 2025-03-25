SELECT
  a0.c0 AS W_WAREHOUSE_NAME,
  a0.c1 AS I_ITEM_ID,
  a0.c2 AS INV_BEFORE,
  a0.c3 AS INV_AFTER
FROM (
  SELECT
    a4.w_warehouse_name AS c0,
    a2.i_item_id AS c1,
    SUM(
      CASE
        WHEN (
          a3.d_date < CAST('2000-03-11' AS DATE)
        )
        THEN a1.inv_quantity_on_hand
        ELSE 0
      END
    ) AS c2,
    SUM(
      CASE
        WHEN (
          a3.d_date >= CAST('2000-03-11' AS DATE)
        )
        THEN a1.inv_quantity_on_hand
        ELSE 0
      END
    ) AS c3
  FROM (
    (
      (
        inventory AS a1
          INNER JOIN item AS a2
            ON (
              (
                a2.i_item_sk = a1.inv_item_sk
              )
              AND (
                (
                  a1.inv_date_sk <= 2451645
                ) AND (
                  a1.inv_date_sk >= 2451585
                )
              )
              AND (
                (
                  a1.inv_item_sk <= 401987
                ) AND (
                  a1.inv_item_sk >= 37
                )
              )
              AND (
                0.99 <= a2.i_current_price
              )
              AND (
                a2.i_current_price <= 1.49
              )
            )
      )
      INNER JOIN date_dim AS a3
        ON (
          (
            a1.inv_date_sk = a3.d_date_sk
          )
          AND (
            CAST('2000-02-10' AS DATE) <= a3.d_date
          )
          AND (
            a3.d_date <= CAST('2000-04-10' AS DATE)
          )
        )
    )
    INNER JOIN warehouse AS a4
      ON (
        a1.inv_warehouse_sk = a4.w_warehouse_sk
      )
  )
  GROUP BY
    a4.w_warehouse_name,
    a2.i_item_id
) AS a0
WHERE
  (
    00.66666666666666666666666666666 <= CASE WHEN (
      a0.c2 > 0
    ) THEN (
      a0.c3 / a0.c2
    ) ELSE NULL END
  )
  AND (
    CASE WHEN (
      a0.c2 > 0
    ) THEN (
      a0.c3 / a0.c2
    ) ELSE NULL END <= 01.50000000000000000000000000000
  )
ORDER BY
  1 ASC NULLS LAST,
  2 ASC NULLS LAST
LIMIT 100