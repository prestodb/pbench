WITH A5 AS (
    SELECT
        A8.I_PRODUCT_NAME C0,
        A8.I_BRAND C1,
        A8.I_CLASS C2,
        A8.I_CATEGORY C3,
        SUM(A6.INV_QUANTITY_ON_HAND) C4,
        COUNT(A6.INV_QUANTITY_ON_HAND) C5
    FROM
        (
            (
                INVENTORY A6
                INNER JOIN DATE_DIM A7 ON (A6.INV_DATE_SK = A7.D_DATE_SK)
            )
            INNER JOIN ITEM A8 ON (A6.INV_ITEM_SK = A8.I_ITEM_SK)
        )
    WHERE
        (1201 <= A7.D_MONTH_SEQ)
        AND (A7.D_MONTH_SEQ <= 1212)
    GROUP BY
        A8.I_PRODUCT_NAME,
        A8.I_BRAND,
        A8.I_CLASS,
        A8.I_CATEGORY
),
A4 AS (
    SELECT
        "A9".C0 C0,
        "A9".C1 C1,
        "A9".C2 C2,
        NULL C3,
        SUM("A9".C4) C4,
        SUM("A9".C5) C5
    FROM
        A5 "A9"
    GROUP BY
        "A9".C0,
        "A9".C1,
        "A9".C2
),
A3 AS (
    SELECT
        "A10".C0 C0,
        "A10".C1 C1,
        NULL C2,
        NULL C3,
        SUM("A10".C4) C4,
        SUM("A10".C5) C5
    FROM
        A4 "A10"
    GROUP BY
        "A10".C0,
        "A10".C1
),
A2 AS (
    SELECT
        "A11".C0 C0,
        NULL C1,
        NULL C2,
        NULL C3,
        SUM("A11".C4) C4,
        SUM("A11".C5) C5
    FROM
        A3 "A11"
    GROUP BY
        "A11".C0
)
SELECT
    A0.C0 "I_PRODUCT_NAME",
    A0.C1 "I_BRAND",
    A0.C2 "I_CLASS",
    A0.C3 "I_CATEGORY",
    CAST((A0.C4 / A0.C5) AS INTEGER) "QOH"
FROM
    (
        (
            SELECT
                NULL C0,
                NULL C1,
                NULL C2,
                NULL C3,
                A1.C0 C4,
                COALESCE(A1.C1, 0000000000000000000000000000000.) C5
            FROM
                (
                    SELECT
                        SUM("A12".C4) C0,
                        SUM("A12".C5) C1
                    FROM
                        A2 "A12"
                ) A1
        )
        UNION
        ALL (
            SELECT
                "A13".C0 C0,
                "A13".C1 C1,
                "A13".C2 C2,
                "A13".C3 C3,
                "A13".C4 C4,
                "A13".C5 C5
            FROM
                A2 "A13"
        )
        UNION
        ALL (
            SELECT
                "A14".C0 C0,
                "A14".C1 C1,
                "A14".C2 C2,
                "A14".C3 C3,
                "A14".C4 C4,
                "A14".C5 C5
            FROM
                A3 "A14"
        )
        UNION
        ALL (
            SELECT
                "A15".C0 C0,
                "A15".C1 C1,
                "A15".C2 C2,
                "A15".C3 C3,
                "A15".C4 C4,
                "A15".C5 C5
            FROM
                A4 "A15"
        )
        UNION
        ALL (
            SELECT
                "A16".C0 C0,
                "A16".C1 C1,
                "A16".C2 C2,
                "A16".C3 C3,
                "A16".C4 C4,
                "A16".C5 C5
            FROM
                A5 "A16"
        )
    ) A0
ORDER BY
    5 ASC,
    1 ASC,
    2 ASC,
    3 ASC,
    4 ASC
limit
    100;