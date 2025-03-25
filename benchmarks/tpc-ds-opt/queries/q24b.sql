SELECT
  a0.c1 AS C_LAST_NAME,
  a0.c2 AS C_FIRST_NAME,
  a0.c3 AS S_STORE_NAME,
  a0.c0 AS PAID
FROM (
  SELECT
    SUM(a1.c4) AS c0,
    a1.c0 AS c1,
    a1.c1 AS c2,
    a1.c2 AS c3,
    a1.c5 AS c4
  FROM (
    SELECT
      a2.c0 AS c0,
      a2.c1 AS c1,
      a2.c2 AS c2,
      a2.c3 AS c3,
      a2.c4 AS c4,
      (
        a2.c6 / a2.c5
      ) AS c5
    FROM (
      SELECT
        a3.c0 AS c0,
        a3.c1 AS c1,
        a3.c2 AS c2,
        a3.c3 AS c3,
        a3.c4 AS c4,
        COUNT(a3.c4) OVER () AS c5,
        SUM(a3.c4) OVER () AS c6
      FROM (
        SELECT
          a8.c_last_name AS c0,
          a8.c_first_name AS c1,
          a7.s_store_name AS c2,
          a9.i_color AS c3,
          SUM(a5.ss_net_paid) AS c4
        FROM (
          customer_address AS a4
            INNER JOIN (
              (
                (
                  (
                    store_sales AS a5
                      INNER JOIN store_returns AS a6
                        ON (
                          (
                            a5.ss_ticket_number = a6.sr_ticket_number
                          )
                          AND (
                            a5.ss_item_sk = a6.sr_item_sk
                          )
                          AND (
                            (
                              a5.ss_store_sk <= 1499
                            ) AND (
                              a5.ss_store_sk >= 22
                            )
                          )
                        )
                  )
                  INNER JOIN store AS a7
                    ON (
                      (
                        a5.ss_store_sk = a7.s_store_sk
                      ) AND (
                        a7.s_market_id = 8
                      )
                    )
                )
                INNER JOIN customer AS a8
                  ON (
                    a5.ss_customer_sk = a8.c_customer_sk
                  )
              )
              INNER JOIN item AS a9
                ON (
                  a9.i_item_sk = a5.ss_item_sk
                )
            )
              ON (
                a8.c_birth_country = UPPER(a4.ca_country)
              ) AND (
                a7.s_zip = a4.ca_zip
              )
        )
        GROUP BY
          a8.c_last_name,
          a8.c_first_name,
          a7.s_store_name,
          a9.i_color,
          a4.ca_state,
          a7.s_state,
          a9.i_current_price,
          a9.i_manager_id,
          a9.i_units,
          a9.i_size
      ) AS a3
    ) AS a2
  ) AS a1
  WHERE
    (
      a1.c3 = 'chiffon'
    )
  GROUP BY
    a1.c5,
    a1.c0,
    a1.c1,
    a1.c2
) AS a0
WHERE
  (
    (
      0.05 * CAST(a0.c4 AS DECIMAL(31, 2))
    ) < a0.c0
  )