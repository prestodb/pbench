SELECT
    A0.C0 "I_MANAGER_ID",
    A0.C1 "SUM_SALES",
    (A0.C2 / A0.C3) "AVG_MONTHLY_SALES"
FROM
    (
        SELECT
            A1.C0 C0,
            A1.C1 C1,
            SUM(A1.C1) OVER(PARTITION BY A1.C0) C2,
            COUNT(A1.C1) OVER(PARTITION BY A1.C0) C3
        FROM
            (
                SELECT
                    A3.I_MANAGER_ID C0,
                    SUM(A2.SS_SALES_PRICE) C1
                FROM
                    (
                        (
                            STORE_SALES A2
                            INNER JOIN ITEM A3 ON (A2.SS_ITEM_SK = A3.I_ITEM_SK)
                        )
                        INNER JOIN DATE_DIM A4 ON (A2.SS_SOLD_DATE_SK = A4.D_DATE_SK)
                    )
                WHERE
                    (A2.SS_STORE_SK IS NOT NULL)
                    AND (
                        (
                            (
                                (
                                    A3.I_CATEGORY IN ('Books', 'Children', 'Electronics')
                                )
                                AND (
                                    A3.I_CLASS IN ('personal', 'portable', 'reference', 'self-help')
                                )
                            )
                            AND (
                                A3.I_BRAND IN (
                                    'scholaramalgamalg #14',
                                    'scholaramalgamalg #7',
                                    'exportiunivamalg #9',
                                    'scholaramalgamalg #9'
                                )
                            )
                        )
                        OR (
                            (
                                (A3.I_CATEGORY IN ('Women', 'Music', 'Men'))
                                AND (
                                    A3.I_CLASS IN (
                                        'accessories',
                                        'classical',
                                        'fragrances',
                                        'pants'
                                    )
                                )
                            )
                            AND (
                                A3.I_BRAND IN (
                                    'amalgimporto #1',
                                    'edu packscholar #1',
                                    'exportiimporto #1',
                                    'importoamalg #1'
                                )
                            )
                        )
                    )
                    AND (
                        A4.D_MONTH_SEQ IN (
                            1181,
                            1182,
                            1183,
                            1184,
                            1185,
                            1186,
                            1187,
                            1188,
                            1189,
                            1190,
                            1191,
                            1192
                        )
                    )
                GROUP BY
                    A3.I_MANAGER_ID,
                    A4.D_MOY
            ) A1
    ) A0
WHERE
    (
        0.1 < CASE
            WHEN ((A0.C2 / A0.C3) > 0) THEN (ABS((A0.C1 - (A0.C2 / A0.C3))) / (A0.C2 / A0.C3))
            ELSE NULL
        END
    )
ORDER BY
    1 ASC,
    3 ASC,
    2 ASC
limit
    100;