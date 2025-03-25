SELECT
  SUBSTRING(a3.w_warehouse_name, 1, 20),
  a2.sm_type AS SM_TYPE,
  a4.web_name AS WEB_NAME,
  SUM(
    CASE
      WHEN (
        (
          a0.ws_ship_date_sk - a0.ws_sold_date_sk
        ) <= 30
      )
      THEN 1
      ELSE 0
    END
  ) AS `30 days`,
  SUM(
    CASE
      WHEN (
        (
          (
            a0.ws_ship_date_sk - a0.ws_sold_date_sk
          ) > 30
        )
        AND (
          (
            a0.ws_ship_date_sk - a0.ws_sold_date_sk
          ) <= 60
        )
      )
      THEN 1
      ELSE 0
    END
  ) AS `31-60 days`,
  SUM(
    CASE
      WHEN (
        (
          (
            a0.ws_ship_date_sk - a0.ws_sold_date_sk
          ) > 60
        )
        AND (
          (
            a0.ws_ship_date_sk - a0.ws_sold_date_sk
          ) <= 90
        )
      )
      THEN 1
      ELSE 0
    END
  ) AS `61-90 days`,
  SUM(
    CASE
      WHEN (
        (
          (
            a0.ws_ship_date_sk - a0.ws_sold_date_sk
          ) > 90
        )
        AND (
          (
            a0.ws_ship_date_sk - a0.ws_sold_date_sk
          ) <= 120
        )
      )
      THEN 1
      ELSE 0
    END
  ) AS `91-120 days`,
  SUM(
    CASE
      WHEN (
        (
          a0.ws_ship_date_sk - a0.ws_sold_date_sk
        ) > 120
      )
      THEN 1
      ELSE 0
    END
  ) AS `>120 days`
FROM (
  (
    (
      (
        web_sales AS a0
          INNER JOIN date_dim AS a1
            ON (
              (
                a0.ws_ship_date_sk = a1.d_date_sk
              )
              AND (
                (
                  a0.ws_ship_date_sk <= 2451910
                ) AND (
                  a0.ws_ship_date_sk >= 2451545
                )
              )
              AND (
                1200 <= a1.d_month_seq
              )
              AND (
                a1.d_month_seq <= 1211
              )
            )
      )
      INNER JOIN ship_mode AS a2
        ON (
          a0.ws_ship_mode_sk = a2.sm_ship_mode_sk
        )
    )
    INNER JOIN warehouse AS a3
      ON (
        a0.ws_warehouse_sk = a3.w_warehouse_sk
      )
  )
  INNER JOIN web_site AS a4
    ON (
      a0.ws_web_site_sk = a4.web_site_sk
    )
)
GROUP BY
  SUBSTRING(a3.w_warehouse_name, 1, 20),
  a2.sm_type,
  a4.web_name
ORDER BY
  1 ASC NULLS LAST,
  2 ASC NULLS LAST,
  3 ASC NULLS LAST
LIMIT 100