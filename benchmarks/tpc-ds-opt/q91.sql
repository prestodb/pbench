SELECT
  a7.cc_call_center_id AS CALL_CENTER,
  a7.cc_name AS CALL_CENTER_NAME,
  a7.cc_manager AS MANAGER,
  SUM(a6.c0) AS RETURNS_LOSS
FROM (
  (
    SELECT
      SUM(a1.cr_net_loss) AS c0,
      a1.cr_call_center_sk AS c1,
      a3.cd_marital_status AS c2,
      a3.cd_education_status AS c3
    FROM (
      customer_address AS a0
        INNER JOIN (
          (
            catalog_returns AS a1
              INNER JOIN (
                (
                  customer AS a2
                    INNER JOIN customer_demographics AS a3
                      ON (
                        (
                          a3.cd_demo_sk = a2.c_current_cdemo_sk
                        )
                        AND (
                          (
                            a2.c_current_cdemo_sk <= 1920792
                          ) AND (
                            a2.c_current_cdemo_sk >= 57
                          )
                        )
                        AND (
                          (
                            (
                              a3.cd_marital_status = 'M'
                            ) AND (
                              a3.cd_education_status = 'Unknown'
                            )
                          )
                          OR (
                            (
                              a3.cd_marital_status = 'W'
                            )
                            AND (
                              a3.cd_education_status = 'Advanced Degree'
                            )
                          )
                        )
                      )
                )
                INNER JOIN household_demographics AS a4
                  ON (
                    (
                      a4.hd_demo_sk = a2.c_current_hdemo_sk
                    )
                    AND (
                      a4.hd_buy_potential LIKE 'Unknown%'
                    )
                  )
              )
                ON (
                  (
                    a1.cr_returning_customer_sk = a2.c_customer_sk
                  )
                  AND (
                    (
                      a1.cr_returned_date_sk <= 2451148
                    )
                    AND (
                      a1.cr_returned_date_sk >= 2451119
                    )
                  )
                )
          )
          INNER JOIN date_dim AS a5
            ON (
              (
                a1.cr_returned_date_sk = a5.d_date_sk
              )
              AND (
                a5.d_year = 1998
              )
              AND (
                a5.d_moy = 11
              )
            )
        )
          ON (
            (
              a0.ca_address_sk = a2.c_current_addr_sk
            )
            AND (
              a0.ca_gmt_offset = -007.00
            )
          )
    )
    GROUP BY
      a1.cr_call_center_sk,
      a3.cd_marital_status,
      a3.cd_education_status
  ) AS a6
  INNER JOIN call_center AS a7
    ON (
      a6.c1 = a7.cc_call_center_sk
    )
)
GROUP BY
  a7.cc_call_center_id,
  a7.cc_name,
  a7.cc_manager,
  a6.c2,
  a6.c3
ORDER BY
  4 DESC