SELECT
    DISTINCT A0.I_PRODUCT_NAME "I_PRODUCT_NAME"
FROM
    (
        ITEM A0
        LEFT OUTER JOIN (
            SELECT
                COUNT(*) C0,
                A2.C0 C1
            FROM
                (
                    SELECT
                        DISTINCT A3.I_MANUFACT C0,
                        A4.I_ITEM_SK C1
                    FROM
                        (
                            ITEM A3
                            INNER JOIN ITEM A4 ON (A4.I_MANUFACT = A3.I_MANUFACT)
                        )
                    WHERE
                        (668 <= A3.I_MANUFACT_ID)
                        AND (A3.I_MANUFACT_ID <= 708)
                        AND (
                            (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        A4.I_CATEGORY = 'Women                                             '
                                                    )
                                                    AND (
                                                        (A4.I_COLOR = 'cream               ')
                                                        OR (A4.I_COLOR = 'ghost               ')
                                                    )
                                                )
                                                AND (
                                                    (A4.I_UNITS = 'Ton       ')
                                                    OR (A4.I_UNITS = 'Gross     ')
                                                )
                                            )
                                            AND (
                                                (A4.I_SIZE = 'economy             ')
                                                OR (A4.I_SIZE = 'small               ')
                                            )
                                        )
                                        OR (
                                            (
                                                (
                                                    (
                                                        A4.I_CATEGORY = 'Women                                             '
                                                    )
                                                    AND (
                                                        (A4.I_COLOR = 'midnight            ')
                                                        OR (A4.I_COLOR = 'burlywood           ')
                                                    )
                                                )
                                                AND (
                                                    (A4.I_UNITS = 'Tsp       ')
                                                    OR (A4.I_UNITS = 'Bundle    ')
                                                )
                                            )
                                            AND (
                                                (A4.I_SIZE = 'medium              ')
                                                OR (A4.I_SIZE = 'extra large         ')
                                            )
                                        )
                                    )
                                    OR (
                                        (
                                            (
                                                (
                                                    A4.I_CATEGORY = 'Men                                               '
                                                )
                                                AND (
                                                    (A4.I_COLOR = 'lavender            ')
                                                    OR (A4.I_COLOR = 'azure               ')
                                                )
                                            )
                                            AND (
                                                (A4.I_UNITS = 'Each      ')
                                                OR (A4.I_UNITS = 'Lb        ')
                                            )
                                        )
                                        AND (
                                            (A4.I_SIZE = 'large               ')
                                            OR (A4.I_SIZE = 'N/A                 ')
                                        )
                                    )
                                )
                                OR (
                                    (
                                        (
                                            (
                                                A4.I_CATEGORY = 'Men                                               '
                                            )
                                            AND (
                                                (A4.I_COLOR = 'chocolate           ')
                                                OR (A4.I_COLOR = 'steel               ')
                                            )
                                        )
                                        AND (
                                            (A4.I_UNITS = 'N/A       ')
                                            OR (A4.I_UNITS = 'Dozen     ')
                                        )
                                    )
                                    AND (
                                        (A4.I_SIZE = 'economy             ')
                                        OR (A4.I_SIZE = 'small               ')
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
                                                        A4.I_CATEGORY = 'Women                                             '
                                                    )
                                                    AND (
                                                        (A4.I_COLOR = 'floral              ')
                                                        OR (A4.I_COLOR = 'royal               ')
                                                    )
                                                )
                                                AND (
                                                    (A4.I_UNITS = 'Unknown   ')
                                                    OR (A4.I_UNITS = 'Tbl       ')
                                                )
                                            )
                                            AND (
                                                (A4.I_SIZE = 'economy             ')
                                                OR (A4.I_SIZE = 'small               ')
                                            )
                                        )
                                        OR (
                                            (
                                                (
                                                    (
                                                        A4.I_CATEGORY = 'Women                                             '
                                                    )
                                                    AND (
                                                        (A4.I_COLOR = 'navy                ')
                                                        OR (A4.I_COLOR = 'forest              ')
                                                    )
                                                )
                                                AND (
                                                    (A4.I_UNITS = 'Bunch     ')
                                                    OR (A4.I_UNITS = 'Dram      ')
                                                )
                                            )
                                            AND (
                                                (A4.I_SIZE = 'medium              ')
                                                OR (A4.I_SIZE = 'extra large         ')
                                            )
                                        )
                                    )
                                    OR (
                                        (
                                            (
                                                (
                                                    A4.I_CATEGORY = 'Men                                               '
                                                )
                                                AND (
                                                    (A4.I_COLOR = 'cyan                ')
                                                    OR (A4.I_COLOR = 'indian              ')
                                                )
                                            )
                                            AND (
                                                (A4.I_UNITS = 'Carton    ')
                                                OR (A4.I_UNITS = 'Cup       ')
                                            )
                                        )
                                        AND (
                                            (A4.I_SIZE = 'large               ')
                                            OR (A4.I_SIZE = 'N/A                 ')
                                        )
                                    )
                                )
                                OR (
                                    (
                                        (
                                            (
                                                A4.I_CATEGORY = 'Men                                               '
                                            )
                                            AND (
                                                (A4.I_COLOR = 'coral               ')
                                                OR (A4.I_COLOR = 'pale                ')
                                            )
                                        )
                                        AND (
                                            (A4.I_UNITS = 'Pallet    ')
                                            OR (A4.I_UNITS = 'Gram      ')
                                        )
                                    )
                                    AND (
                                        (A4.I_SIZE = 'economy             ')
                                        OR (A4.I_SIZE = 'small               ')
                                    )
                                )
                            )
                        )
                ) A2
            GROUP BY
                A2.C0
        ) A1 ON (A1.C1 = A0.I_MANUFACT)
    )
WHERE
    (A0.I_MANUFACT_ID <= 708)
    AND (668 <= A0.I_MANUFACT_ID)
    AND (0 < COALESCE(A1.C0, 0))
ORDER BY
    1 ASC
limit
    100;