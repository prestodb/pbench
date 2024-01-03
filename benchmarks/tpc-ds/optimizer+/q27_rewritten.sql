WITH A3 AS (
    SELECT
        A4.I_ITEM_ID C0,
        A7.S_STATE C1,
        0 C2,
        SUM(A5.SS_SALES_PRICE) C3,
        COUNT(A5.SS_SALES_PRICE) C4,
        SUM(A5.SS_COUPON_AMT) C5,
        COUNT(A5.SS_COUPON_AMT) C6,
        SUM(A5.SS_LIST_PRICE) C7,
        COUNT(A5.SS_LIST_PRICE) C8,
        SUM(A5.SS_QUANTITY) C9,
        COUNT(A5.SS_QUANTITY) C10
    FROM
        (
            ITEM A4
            INNER JOIN (
                (
                    (
                        STORE_SALES A5
                        INNER JOIN DATE_DIM A6 ON (A5.SS_SOLD_DATE_SK = A6.D_DATE_SK)
                    )
                    INNER JOIN STORE A7 ON (A5.SS_STORE_SK = A7.S_STORE_SK)
                )
                INNER JOIN CUSTOMER_DEMOGRAPHICS A8 ON (A5.SS_CDEMO_SK = A8.CD_DEMO_SK)
            ) ON (A5.SS_ITEM_SK = A4.I_ITEM_SK)
        )
    WHERE
        (A6.D_YEAR = 2000)
        AND (A7.S_STATE = 'TN')
        AND (A8.CD_GENDER = 'M')
        AND (A8.CD_MARITAL_STATUS = 'U')
        AND (A8.CD_EDUCATION_STATUS = 'Secondary           ')
    GROUP BY
        A4.I_ITEM_ID,
        A7.S_STATE
),
A2 AS (
    SELECT
        "A9".C0 C0,
        NULL C1,
        1 C2,
        SUM("A9".C3) C3,
        SUM("A9".C4) C4,
        SUM("A9".C5) C5,
        SUM("A9".C6) C6,
        SUM("A9".C7) C7,
        SUM("A9".C8) C8,
        SUM("A9".C9) C9,
        SUM("A9".C10) C10
    FROM
        A3 "A9"
    GROUP BY
        "A9".C0
)
SELECT
    A0.C0 "I_ITEM_ID",
    A0.C1 "S_STATE",
    A0.C2 "G_STATE",
    CAST((A0.C9 / A0.C10) AS INTEGER) "AGG1",
    (A0.C7 / A0.C8) "AGG2",
    (A0.C5 / A0.C6) "AGG3",
    (A0.C3 / A0.C4) "AGG4"
FROM
    (
        (
            SELECT
                NULL C0,
                NULL C1,
                1 C2,
                A1.C0 C3,
                COALESCE(A1.C1, 0000000000000000000000000000000.) C4,
                A1.C2 C5,
                COALESCE(A1.C3, 0000000000000000000000000000000.) C6,
                A1.C4 C7,
                COALESCE(A1.C5, 0000000000000000000000000000000.) C8,
                A1.C6 C9,
                COALESCE(A1.C7, 0000000000000000000000000000000.) C10
            FROM
                (
                    SELECT
                        SUM("A10".C3) C0,
                        SUM("A10".C4) C1,
                        SUM("A10".C5) C2,
                        SUM("A10".C6) C3,
                        SUM("A10".C7) C4,
                        SUM("A10".C8) C5,
                        SUM("A10".C9) C6,
                        SUM("A10".C10) C7
                    FROM
                        A2 "A10"
                ) A1
            limit
                100
        )
        UNION
        ALL (
            SELECT
                "A11".C0 C0,
                "A11".C1 C1,
                "A11".C2 C2,
                "A11".C3 C3,
                "A11".C4 C4,
                "A11".C5 C5,
                "A11".C6 C6,
                "A11".C7 C7,
                "A11".C8 C8,
                "A11".C9 C9,
                "A11".C10 C10
            FROM
                A2 "A11"
        )
        UNION
        ALL (
            SELECT
                "A12".C0 C0,
                "A12".C1 C1,
                "A12".C2 C2,
                "A12".C3 C3,
                "A12".C4 C4,
                "A12".C5 C5,
                "A12".C6 C6,
                "A12".C7 C7,
                "A12".C8 C8,
                "A12".C9 C9,
                "A12".C10 C10
            FROM
                A3 "A12"
        )
    ) A0
ORDER BY
    1 ASC,
    2 ASC
limit
    100;