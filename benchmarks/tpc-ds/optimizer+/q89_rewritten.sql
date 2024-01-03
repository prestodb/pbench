SELECT
    A0.C0 "I_CATEGORY",
    A0.C1 "I_CLASS",
    A0.C2 "I_BRAND",
    A0.C3 "S_STORE_NAME",
    A0.C4 "S_COMPANY_NAME",
    A0.C5 "D_MOY",
    A0.C6 "SUM_SALES",
    (A0.C7 / A0.C8) "AVG_MONTHLY_SALES",
    (A0.C6 - (A0.C7 / A0.C8))
FROM
    (
        SELECT
            A1.C0 C0,
            A1.C1 C1,
            A1.C2 C2,
            A1.C3 C3,
            A1.C4 C4,
            A1.C5 C5,
            A1.C6 C6,
            SUM(A1.C6) OVER(PARTITION BY A1.C0, A1.C2, A1.C3, A1.C4) C7,
            COUNT(A1.C6) OVER(PARTITION BY A1.C0, A1.C2, A1.C3, A1.C4) C8
        FROM
            (
                SELECT
                    A3.I_CATEGORY C0,
                    A3.I_CLASS C1,
                    A3.I_BRAND C2,
                    A5.S_STORE_NAME C3,
                    A5.S_COMPANY_NAME C4,
                    A4.D_MOY C5,
                    SUM(A2.SS_SALES_PRICE) C6
                FROM
                    (
                        (
                            (
                                STORE_SALES A2
                                INNER JOIN ITEM A3 ON (A2.SS_ITEM_SK = A3.I_ITEM_SK)
                            )
                            INNER JOIN DATE_DIM A4 ON (A2.SS_SOLD_DATE_SK = A4.D_DATE_SK)
                        )
                        INNER JOIN STORE A5 ON (A2.SS_STORE_SK = A5.S_STORE_SK)
                    )
                WHERE
                    (
                        (
                            (A3.I_CATEGORY IN ('Children', 'Jewelry', 'Home'))
                            AND (A3.I_CLASS IN ('infants', 'birdal', 'flatware'))
                        )
                        OR (
                            (
                                A3.I_CATEGORY IN ('Electronics', 'Music', 'Books')
                            )
                            AND (A3.I_CLASS IN ('audio', 'classical', 'science'))
                        )
                    )
                    AND (A4.D_YEAR = 2001)
                GROUP BY
                    A3.I_CATEGORY,
                    A3.I_CLASS,
                    A3.I_BRAND,
                    A5.S_STORE_NAME,
                    A5.S_COMPANY_NAME,
                    A4.D_MOY
            ) A1
    ) A0
WHERE
    (
        0.1 < CASE
            WHEN (
                (A0.C7 / A0.C8) <> 00000000000000000000000000000.00
            ) THEN (ABS((A0.C6 - (A0.C7 / A0.C8))) / (A0.C7 / A0.C8))
            ELSE NULL
        END
    )
ORDER BY
    9 ASC,
    4 ASC
limit
    100;