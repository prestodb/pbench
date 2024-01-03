SELECT
    A0.C0 "I_MANUFACT_ID",
    A0.C1 "SUM_SALES",
    (A0.C2 / A0.C3) "AVG_QUARTERLY_SALES"
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
                    A3.I_MANUFACT_ID C0,
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
                            1197,
                            1198,
                            1199,
                            1200,
                            1201,
                            1202,
                            1203,
                            1204,
                            1205,
                            1206,
                            1207,
                            1208
                        )
                    )
                GROUP BY
                    A3.I_MANUFACT_ID,
                    A4.D_QOY
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
    3 ASC,
    2 ASC,
    1 ASC
limit
    100;