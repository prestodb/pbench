SELECT DISTINCT
  a4.i_product_name AS I_PRODUCT_NAME
FROM (
  (
    SELECT
      COUNT(*) AS c0,
      a1.c0 AS c1
    FROM (
      SELECT DISTINCT
        a2.i_manufact AS c0,
        a3.i_item_sk AS c1
      FROM (
        item AS a2
          INNER JOIN item AS a3
            ON (
              (
                a3.i_manufact = a2.i_manufact
              )
              AND (
                738 <= a2.i_manufact_id
              )
              AND (
                a2.i_manufact_id <= 778
              )
              AND (
                (
                  (
                    (
                      (
                        (
                          (
                            (
                              a3.i_category = 'Women'
                            )
                            AND (
                              (
                                a3.i_color = 'powder'
                              ) OR (
                                a3.i_color = 'khaki'
                              )
                            )
                          )
                          AND (
                            (
                              a3.i_units = 'Ounce'
                            ) OR (
                              a3.i_units = 'Oz'
                            )
                          )
                        )
                        AND (
                          (
                            a3.i_size = 'medium'
                          ) OR (
                            a3.i_size = 'extra large'
                          )
                        )
                      )
                      OR (
                        (
                          (
                            (
                              a3.i_category = 'Women'
                            )
                            AND (
                              (
                                a3.i_color = 'brown'
                              ) OR (
                                a3.i_color = 'honeydew'
                              )
                            )
                          )
                          AND (
                            (
                              a3.i_units = 'Bunch'
                            ) OR (
                              a3.i_units = 'Ton'
                            )
                          )
                        )
                        AND (
                          (
                            a3.i_size = 'N/A'
                          ) OR (
                            a3.i_size = 'small'
                          )
                        )
                      )
                    )
                    OR (
                      (
                        (
                          (
                            a3.i_category = 'Men'
                          )
                          AND (
                            (
                              a3.i_color = 'floral'
                            ) OR (
                              a3.i_color = 'deep'
                            )
                          )
                        )
                        AND (
                          (
                            a3.i_units = 'N/A'
                          ) OR (
                            a3.i_units = 'Dozen'
                          )
                        )
                      )
                      AND (
                        (
                          a3.i_size = 'petite'
                        ) OR (
                          a3.i_size = 'large'
                        )
                      )
                    )
                  )
                  OR (
                    (
                      (
                        (
                          a3.i_category = 'Men'
                        )
                        AND (
                          (
                            a3.i_color = 'light'
                          ) OR (
                            a3.i_color = 'cornflower'
                          )
                        )
                      )
                      AND (
                        (
                          a3.i_units = 'Box'
                        ) OR (
                          a3.i_units = 'Pound'
                        )
                      )
                    )
                    AND (
                      (
                        a3.i_size = 'medium'
                      ) OR (
                        a3.i_size = 'extra large'
                      )
                    )
                  )
                )
                OR (
                  (
                    (
                      (
                        (
                          (
                            (
                              a3.i_category = 'Women'
                            )
                            AND (
                              (
                                a3.i_color = 'midnight'
                              ) OR (
                                a3.i_color = 'snow'
                              )
                            )
                          )
                          AND (
                            (
                              a3.i_units = 'Pallet'
                            ) OR (
                              a3.i_units = 'Gross'
                            )
                          )
                        )
                        AND (
                          (
                            a3.i_size = 'medium'
                          ) OR (
                            a3.i_size = 'extra large'
                          )
                        )
                      )
                      OR (
                        (
                          (
                            (
                              a3.i_category = 'Women'
                            )
                            AND (
                              (
                                a3.i_color = 'cyan'
                              ) OR (
                                a3.i_color = 'papaya'
                              )
                            )
                          )
                          AND (
                            (
                              a3.i_units = 'Cup'
                            ) OR (
                              a3.i_units = 'Dram'
                            )
                          )
                        )
                        AND (
                          (
                            a3.i_size = 'N/A'
                          ) OR (
                            a3.i_size = 'small'
                          )
                        )
                      )
                    )
                    OR (
                      (
                        (
                          (
                            a3.i_category = 'Men'
                          )
                          AND (
                            (
                              a3.i_color = 'orange'
                            ) OR (
                              a3.i_color = 'frosted'
                            )
                          )
                        )
                        AND (
                          (
                            a3.i_units = 'Each'
                          ) OR (
                            a3.i_units = 'Tbl'
                          )
                        )
                      )
                      AND (
                        (
                          a3.i_size = 'petite'
                        ) OR (
                          a3.i_size = 'large'
                        )
                      )
                    )
                  )
                  OR (
                    (
                      (
                        (
                          a3.i_category = 'Men'
                        )
                        AND (
                          (
                            a3.i_color = 'forest'
                          ) OR (
                            a3.i_color = 'ghost'
                          )
                        )
                      )
                      AND (
                        (
                          a3.i_units = 'Lb'
                        ) OR (
                          a3.i_units = 'Bundle'
                        )
                      )
                    )
                    AND (
                      (
                        a3.i_size = 'medium'
                      ) OR (
                        a3.i_size = 'extra large'
                      )
                    )
                  )
                )
              )
            )
      )
    ) AS a1
    GROUP BY
      a1.c0
  ) AS a0
  RIGHT OUTER JOIN item AS a4
    ON (
      a0.c1 = a4.i_manufact
    )
)
WHERE
  (
    a4.i_manufact_id <= 778
  )
  AND (
    738 <= a4.i_manufact_id
  )
  AND (
    0 < COALESCE(a0.c0, 0)
  )
ORDER BY
  1 ASC NULLS LAST
LIMIT 100