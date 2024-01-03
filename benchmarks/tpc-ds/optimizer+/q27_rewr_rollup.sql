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
                    WHEN (A6.C0 = 1) THEN A1.I_ITEM_ID
                    WHEN (A6.C0 = 2) THEN A1.I_ITEM_ID
                    ELSE NULL
                END C0,
                CASE
                    WHEN (A6.C0 = 1) THEN 'TN'
                    WHEN (A6.C0 = 2) THEN NULL
                    ELSE NULL
                END C1,
                SUM(A2.SS_SALES_PRICE) C2,
                COUNT(A2.SS_SALES_PRICE) C3,
                SUM(A2.SS_COUPON_AMT) C4,
                COUNT(A2.SS_COUPON_AMT) C5,
                SUM(A2.SS_LIST_PRICE) C6,
                COUNT(A2.SS_LIST_PRICE) C7,
                SUM(A2.SS_QUANTITY) C8,
                COUNT(A2.SS_QUANTITY) C9,
                A6.C0 C10
            FROM
                (
                    (
                        ITEM A1
                        INNER JOIN (
                            (
                                (
                                    STORE_SALES A2
                                    INNER JOIN DATE_DIM A3 ON (A2.SS_SOLD_DATE_SK = A3.D_DATE_SK)
                                )
                                INNER JOIN STORE A4 ON (A2.SS_STORE_SK = A4.S_STORE_SK)
                            )
                            INNER JOIN CUSTOMER_DEMOGRAPHICS A5 ON (A2.SS_CDEMO_SK = A5.CD_DEMO_SK)
                        ) ON (A2.SS_ITEM_SK = A1.I_ITEM_SK)
                    )
                    INNER JOIN (
                        VALUES
                            1,
                            2,
                            3
                    ) A6 (C0) ON (
                        MOD(LENGTH(A6.C0), 1) = MOD(LENGTH(A1.I_ITEM_ID), 1)
                    )
                )
            WHERE
                (A3.D_YEAR = 2000)
                AND (A4.S_STATE = 'TN')
                AND (A5.CD_EDUCATION_STATUS = 'Secondary           ')
                AND (A5.CD_MARITAL_STATUS = 'U')
                AND (A5.CD_GENDER = 'M')
            GROUP BY
                A6.C0,
                CASE
                    WHEN (A6.C0 = 1) THEN A1.I_ITEM_ID
                    WHEN (A6.C0 = 2) THEN A1.I_ITEM_ID
                    ELSE NULL
                END,
                CASE
                    WHEN (A6.C0 = 1) THEN 'TN'
                    WHEN (A6.C0 = 2) THEN NULL
                    ELSE NULL
                END
        ) A0
        RIGHT OUTER JOIN (
            VALUES
                1,
                2,
                3
        ) A7 (C0) ON (A7.C0 = A0.C10)
    )
WHERE
    (
        (
            (A7.C0 = 3)
            AND (A0.C10 IS NULL)
        )
        OR (A0.C10 IS NOT NULL)
    )
ORDER BY
    1 ASC,
    2 ASC
limit
    100;