SELECT
  a3.s_store_name AS S_STORE_NAME,
  a3.s_company_id AS S_COMPANY_ID,
  a3.s_street_number AS S_STREET_NUMBER,
  a3.s_street_name AS S_STREET_NAME,
  a3.s_street_type AS S_STREET_TYPE,
  a3.s_suite_number AS S_SUITE_NUMBER,
  a3.s_city AS S_CITY,
  a3.s_county AS S_COUNTY,
  a3.s_state AS S_STATE,
  a3.s_zip AS S_ZIP,
  SUM(
    CASE
      WHEN (
        (
          a1.sr_returned_date_sk - a0.ss_sold_date_sk
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
            a1.sr_returned_date_sk - a0.ss_sold_date_sk
          ) > 30
        )
        AND (
          (
            a1.sr_returned_date_sk - a0.ss_sold_date_sk
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
            a1.sr_returned_date_sk - a0.ss_sold_date_sk
          ) > 60
        )
        AND (
          (
            a1.sr_returned_date_sk - a0.ss_sold_date_sk
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
            a1.sr_returned_date_sk - a0.ss_sold_date_sk
          ) > 90
        )
        AND (
          (
            a1.sr_returned_date_sk - a0.ss_sold_date_sk
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
          a1.sr_returned_date_sk - a0.ss_sold_date_sk
        ) > 120
      )
      THEN 1
      ELSE 0
    END
  ) AS `>120 days`
FROM (
  (
    store_sales AS a0
      INNER JOIN (
        store_returns AS a1
          INNER JOIN date_dim AS a2
            ON (
              (
                a1.sr_returned_date_sk = a2.d_date_sk
              )
              AND (
                (
                  a1.sr_returned_date_sk <= 2452153
                )
                AND (
                  a1.sr_returned_date_sk >= 2452123
                )
              )
              AND (
                a2.d_year = 2001
              )
              AND (
                a2.d_moy = 8
              )
            )
      )
        ON (
          (
            a0.ss_ticket_number = a1.sr_ticket_number
          )
          AND (
            a0.ss_item_sk = a1.sr_item_sk
          )
          AND (
            a0.ss_customer_sk = a1.sr_customer_sk
          )
          AND (
            NOT a0.ss_sold_date_sk IS NULL
          )
        )
  )
  INNER JOIN store AS a3
    ON (
      a0.ss_store_sk = a3.s_store_sk
    )
)
GROUP BY
  a3.s_store_name,
  a3.s_company_id,
  a3.s_street_number,
  a3.s_street_name,
  a3.s_street_type,
  a3.s_suite_number,
  a3.s_city,
  a3.s_county,
  a3.s_state,
  a3.s_zip
ORDER BY
  1 ASC NULLS LAST,
  2 ASC NULLS LAST,
  3 ASC NULLS LAST,
  4 ASC NULLS LAST,
  5 ASC NULLS LAST,
  6 ASC NULLS LAST,
  7 ASC NULLS LAST,
  8 ASC NULLS LAST,
  9 ASC NULLS LAST,
  10 ASC NULLS LAST
LIMIT 100