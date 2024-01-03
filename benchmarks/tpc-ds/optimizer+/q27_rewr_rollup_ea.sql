SELECT
    A0.C0 "I_ITEM_ID",
    A0.C1 "S_STATE",
    CASE
        WHEN (A0.C10 IS NOT NULL) THEN CASE
            WHEN (A0.C10 IS NULL) THEN NULL
            ELSE CASE
                WHEN (A0.C10 < 2) THEN 0
                ELSE 1
            END
        END
        ELSE 1
    END "G_STATE",
    CAST(
        (
            A0.C8 / CASE
                WHEN (A0.C10 IS NOT NULL) THEN A0.C9
                ELSE 0000000000000000000000000000000.
            END
        ) AS INTEGER
    ) "AGG1",
    (
        A0.C6 / CASE
            WHEN (A0.C10 IS NOT NULL) THEN A0.C7
            ELSE 0000000000000000000000000000000.
        END
    ) "AGG2",
    (
        A0.C4 / CASE
            WHEN (A0.C10 IS NOT NULL) THEN A0.C5
            ELSE 0000000000000000000000000000000.
        END
    ) "AGG3",
    (
        A0.C2 / CASE
            WHEN (A0.C10 IS NOT NULL) THEN A0.C3
            ELSE 0000000000000000000000000000000.
        END
    ) "AGG4"
FROM
    (
        (
            SELECT
                CASE
                    WHEN (A7.C0 = 1) THEN A1.C0
                    WHEN (A7.C0 = 2) THEN A1.C0
                    ELSE NULL
                END C0,
                CASE
                    WHEN (A7.C0 = 1) THEN A1.C1
                    WHEN (A7.C0 = 2) THEN NULL
                    ELSE NULL
                END C1,
                SUM(A1.C9) C2,
                SUM(A1.C8) C3,
                SUM(A1.C7) C4,
                SUM(A1.C6) C5,
                SUM(A1.C5) C6,
                SUM(A1.C4) C7,
                SUM(A1.C3) C8,
                SUM(A1.C2) C9,
                A7.C0 C10
            FROM
                (
                    (
                        SELECT
                            A2.I_ITEM_ID C0,
                            A5.S_STATE C1,
                            COUNT(A3.SS_QUANTITY) C2,
                            SUM(A3.SS_QUANTITY) C3,
                            COUNT(A3.SS_LIST_PRICE) C4,
                            SUM(A3.SS_LIST_PRICE) C5,
                            COUNT(A3.SS_COUPON_AMT) C6,
                            SUM(A3.SS_COUPON_AMT) C7,
                            COUNT(A3.SS_SALES_PRICE) C8,
                            SUM(A3.SS_SALES_PRICE) C9
                        FROM
                            (
                                ITEM A2
                                INNER JOIN (
                                    (
                                        (
                                            STORE_SALES A3
                                            INNER JOIN DATE_DIM A4 ON (A3.SS_SOLD_DATE_SK = A4.D_DATE_SK)
                                        )
                                        INNER JOIN STORE A5 ON (A3.SS_STORE_SK = A5.S_STORE_SK)
                                    )
                                    INNER JOIN CUSTOMER_DEMOGRAPHICS A6 ON (A3.SS_CDEMO_SK = A6.CD_DEMO_SK)
                                ) ON (A3.SS_ITEM_SK = A2.I_ITEM_SK)
                            )
                        WHERE
                            (A4.D_YEAR = 2000)
                            AND (A5.S_STATE = 'TN')
                            AND (A6.CD_GENDER = 'M')
                            AND (A6.CD_MARITAL_STATUS = 'U')
                            AND (A6.CD_EDUCATION_STATUS = 'Secondary           ')
                        GROUP BY
                            A2.I_ITEM_ID,
                            A5.S_STATE
                    ) A1
                    INNER JOIN (
                        VALUES
                            1,
                            2,
                            3
                    ) A7 (C0) ON (MOD(LENGTH(A7.C0), 1) = MOD(LENGTH(A1.C0), 1))
                )
            GROUP BY
                A7.C0,
                CASE
                    WHEN (A7.C0 = 1) THEN A1.C0
                    WHEN (A7.C0 = 2) THEN A1.C0
                    ELSE NULL
                END,
                CASE
                    WHEN (A7.C0 = 1) THEN A1.C1
                    WHEN (A7.C0 = 2) THEN NULL
                    ELSE NULL
                END
        ) A0
        RIGHT OUTER JOIN (
            VALUES
                1,
                2,
                3
        ) A8 (C0) ON (A8.C0 = A0.C10)
    )
WHERE
    (
        (
            (A8.C0 = 3)
            AND (A0.C10 IS NULL)
        )
        OR (A0.C10 IS NOT NULL)
    )
ORDER BY
    1 ASC,
    2 ASC
limit
    100;