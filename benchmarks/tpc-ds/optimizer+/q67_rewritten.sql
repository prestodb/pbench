WITH A9 AS (
    SELECT
        A12.I_CATEGORY C0,
        A12.I_CLASS C1,
        A12.I_BRAND C2,
        A12.I_PRODUCT_NAME C3,
        A11.D_YEAR C4,
        A11.D_QOY C5,
        A11.D_MOY C6,
        A13.S_STORE_ID C7,
        SUM(
            COALESCE(
                (A10.SS_SALES_PRICE * A10.SS_QUANTITY),
                0000000000000000.00
            )
        ) C8
    FROM
        (
            (
                (
                    STORE_SALES A10
                    INNER JOIN DATE_DIM A11 ON (A10.SS_SOLD_DATE_SK = A11.D_DATE_SK)
                )
                INNER JOIN ITEM A12 ON (A10.SS_ITEM_SK = A12.I_ITEM_SK)
            )
            INNER JOIN STORE A13 ON (A10.SS_STORE_SK = A13.S_STORE_SK)
        )
    WHERE
        (1194 <= A11.D_MONTH_SEQ)
        AND (A11.D_MONTH_SEQ <= 1205)
    GROUP BY
        A12.I_CATEGORY,
        A12.I_CLASS,
        A12.I_BRAND,
        A12.I_PRODUCT_NAME,
        A11.D_YEAR,
        A11.D_QOY,
        A11.D_MOY,
        A13.S_STORE_ID
),
A8 AS (
    SELECT
        "A14".C0 C0,
        "A14".C1 C1,
        "A14".C2 C2,
        "A14".C3 C3,
        "A14".C4 C4,
        "A14".C5 C5,
        "A14".C6 C6,
        NULL C7,
        SUM("A14".C8) C8
    FROM
        A9 "A14"
    GROUP BY
        "A14".C0,
        "A14".C1,
        "A14".C2,
        "A14".C3,
        "A14".C4,
        "A14".C5,
        "A14".C6
),
A7 AS (
    SELECT
        "A15".C0 C0,
        "A15".C1 C1,
        "A15".C2 C2,
        "A15".C3 C3,
        "A15".C4 C4,
        "A15".C5 C5,
        NULL C6,
        NULL C7,
        SUM("A15".C8) C8
    FROM
        A8 "A15"
    GROUP BY
        "A15".C0,
        "A15".C1,
        "A15".C2,
        "A15".C3,
        "A15".C4,
        "A15".C5
),
A6 AS (
    SELECT
        "A16".C0 C0,
        "A16".C1 C1,
        "A16".C2 C2,
        "A16".C3 C3,
        "A16".C4 C4,
        NULL C5,
        NULL C6,
        NULL C7,
        SUM("A16".C8) C8
    FROM
        A7 "A16"
    GROUP BY
        "A16".C0,
        "A16".C1,
        "A16".C2,
        "A16".C3,
        "A16".C4
),
A5 AS (
    SELECT
        "A17".C0 C0,
        "A17".C1 C1,
        "A17".C2 C2,
        "A17".C3 C3,
        NULL C4,
        NULL C5,
        NULL C6,
        NULL C7,
        SUM("A17".C8) C8
    FROM
        A6 "A17"
    GROUP BY
        "A17".C0,
        "A17".C1,
        "A17".C2,
        "A17".C3
),
A4 AS (
    SELECT
        "A18".C0 C0,
        "A18".C1 C1,
        "A18".C2 C2,
        NULL C3,
        NULL C4,
        NULL C5,
        NULL C6,
        NULL C7,
        SUM("A18".C8) C8
    FROM
        A5 "A18"
    GROUP BY
        "A18".C0,
        "A18".C1,
        "A18".C2
),
A3 AS (
    SELECT
        "A19".C0 C0,
        "A19".C1 C1,
        NULL C2,
        NULL C3,
        NULL C4,
        NULL C5,
        NULL C6,
        NULL C7,
        SUM("A19".C8) C8
    FROM
        A4 "A19"
    GROUP BY
        "A19".C0,
        "A19".C1
),
A2 AS (
    SELECT
        "A20".C0 C0,
        NULL C1,
        NULL C2,
        NULL C3,
        NULL C4,
        NULL C5,
        NULL C6,
        NULL C7,
        SUM("A20".C8) C8
    FROM
        A3 "A20"
    GROUP BY
        "A20".C0
)
SELECT
    A0.C0 "I_CATEGORY",
    A0.C1 "I_CLASS",
    A0.C2 "I_BRAND",
    A0.C3 "I_PRODUCT_NAME",
    A0.C4 "D_YEAR",
    A0.C5 "D_QOY",
    A0.C6 "D_MOY",
    A0.C7 "S_STORE_ID",
    A0.C8 "SUMSALES",
    A0.C9 "RK"
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
            A1.C7 C7,
            A1.C8 C8,
            RANK() OVER(
                PARTITION BY A1.C0
                ORDER BY
                    A1.C8 DESC
            ) C9
        FROM
            (
                (
                    SELECT
                        NULL C0,
                        NULL C1,
                        NULL C2,
                        NULL C3,
                        NULL C4,
                        NULL C5,
                        NULL C6,
                        NULL C7,
                        SUM("A21".C8) C8
                    FROM
                        A2 "A21"
                )
                UNION
                ALL (
                    SELECT
                        "A22".C0 C0,
                        "A22".C1 C1,
                        "A22".C2 C2,
                        "A22".C3 C3,
                        "A22".C4 C4,
                        "A22".C5 C5,
                        "A22".C6 C6,
                        "A22".C7 C7,
                        "A22".C8 C8
                    FROM
                        A2 "A22"
                )
                UNION
                ALL (
                    SELECT
                        "A23".C0 C0,
                        "A23".C1 C1,
                        "A23".C2 C2,
                        "A23".C3 C3,
                        "A23".C4 C4,
                        "A23".C5 C5,
                        "A23".C6 C6,
                        "A23".C7 C7,
                        "A23".C8 C8
                    FROM
                        A3 "A23"
                )
                UNION
                ALL (
                    SELECT
                        "A24".C0 C0,
                        "A24".C1 C1,
                        "A24".C2 C2,
                        "A24".C3 C3,
                        "A24".C4 C4,
                        "A24".C5 C5,
                        "A24".C6 C6,
                        "A24".C7 C7,
                        "A24".C8 C8
                    FROM
                        A4 "A24"
                )
                UNION
                ALL (
                    SELECT
                        "A25".C0 C0,
                        "A25".C1 C1,
                        "A25".C2 C2,
                        "A25".C3 C3,
                        "A25".C4 C4,
                        "A25".C5 C5,
                        "A25".C6 C6,
                        "A25".C7 C7,
                        "A25".C8 C8
                    FROM
                        A5 "A25"
                )
                UNION
                ALL (
                    SELECT
                        "A26".C0 C0,
                        "A26".C1 C1,
                        "A26".C2 C2,
                        "A26".C3 C3,
                        "A26".C4 C4,
                        "A26".C5 C5,
                        "A26".C6 C6,
                        "A26".C7 C7,
                        "A26".C8 C8
                    FROM
                        A6 "A26"
                )
                UNION
                ALL (
                    SELECT
                        "A27".C0 C0,
                        "A27".C1 C1,
                        "A27".C2 C2,
                        "A27".C3 C3,
                        "A27".C4 C4,
                        "A27".C5 C5,
                        "A27".C6 C6,
                        "A27".C7 C7,
                        "A27".C8 C8
                    FROM
                        A7 "A27"
                )
                UNION
                ALL (
                    SELECT
                        "A28".C0 C0,
                        "A28".C1 C1,
                        "A28".C2 C2,
                        "A28".C3 C3,
                        "A28".C4 C4,
                        "A28".C5 C5,
                        "A28".C6 C6,
                        "A28".C7 C7,
                        "A28".C8 C8
                    FROM
                        A8 "A28"
                )
                UNION
                ALL (
                    SELECT
                        "A29".C0 C0,
                        "A29".C1 C1,
                        "A29".C2 C2,
                        "A29".C3 C3,
                        "A29".C4 C4,
                        "A29".C5 C5,
                        "A29".C6 C6,
                        "A29".C7 C7,
                        "A29".C8 C8
                    FROM
                        A9 "A29"
                )
            ) A1
    ) A0
WHERE
    (A0.C9 <= 100)
ORDER BY
    1 ASC,
    2 ASC,
    3 ASC,
    4 ASC,
    5 ASC,
    6 ASC,
    7 ASC,
    8 ASC,
    9 ASC,
    10 ASC
limit
    100;