WITH A5 AS (
    SELECT
        A6.I_ITEM_ID C0,
        A11.CA_COUNTRY C1,
        A11.CA_STATE C2,
        A11.CA_COUNTY C3,
        SUM(CAST(A10.CD_DEP_COUNT AS DECIMAL(12, 2))) C4,
        COUNT(CAST(A10.CD_DEP_COUNT AS DECIMAL(12, 2))) C5,
        SUM(CAST(A7.C_BIRTH_YEAR AS DECIMAL(12, 2))) C6,
        COUNT(CAST(A7.C_BIRTH_YEAR AS DECIMAL(12, 2))) C7,
        SUM(CAST(A8.CS_NET_PROFIT AS DECIMAL(12, 2))) C8,
        COUNT(CAST(A8.CS_NET_PROFIT AS DECIMAL(12, 2))) C9,
        SUM(CAST(A8.CS_SALES_PRICE AS DECIMAL(12, 2))) C10,
        COUNT(CAST(A8.CS_SALES_PRICE AS DECIMAL(12, 2))) C11,
        SUM(CAST(A8.CS_COUPON_AMT AS DECIMAL(12, 2))) C12,
        COUNT(CAST(A8.CS_COUPON_AMT AS DECIMAL(12, 2))) C13,
        SUM(CAST(A8.CS_LIST_PRICE AS DECIMAL(12, 2))) C14,
        COUNT(CAST(A8.CS_LIST_PRICE AS DECIMAL(12, 2))) C15,
        SUM(CAST(A8.CS_QUANTITY AS DECIMAL(12, 2))) C16,
        COUNT(CAST(A8.CS_QUANTITY AS DECIMAL(12, 2))) C17
    FROM
        (
            ITEM A6
            INNER JOIN (
                (
                    CUSTOMER A7
                    INNER JOIN (
                        (
                            CATALOG_SALES A8
                            INNER JOIN DATE_DIM A9 ON (A8.CS_SOLD_DATE_SK = A9.D_DATE_SK)
                        )
                        INNER JOIN CUSTOMER_DEMOGRAPHICS A10 ON (A8.CS_BILL_CDEMO_SK = A10.CD_DEMO_SK)
                    ) ON (A8.CS_BILL_CUSTOMER_SK = A7.C_CUSTOMER_SK)
                )
                INNER JOIN CUSTOMER_ADDRESS A11 ON (
                    CAST(A7.C_CURRENT_ADDR_SK AS BIGINT) = A11.CA_ADDRESS_SK
                )
            ) ON (A8.CS_ITEM_SK = A6.I_ITEM_SK)
        )
    WHERE
        (A7.C_BIRTH_MONTH IN (1, 2, 9, 5, 11, 3))
        AND (A7.C_CURRENT_CDEMO_SK IS NOT NULL)
        AND (A9.D_YEAR = 1998)
        AND (A10.CD_GENDER = 'M')
        AND (A10.CD_EDUCATION_STATUS = 'Primary             ')
        AND (
            A11.CA_STATE IN ('MS', 'NE', 'IA', 'MI', 'GA', 'NY', 'CO')
        )
    GROUP BY
        A6.I_ITEM_ID,
        A11.CA_COUNTRY,
        A11.CA_STATE,
        A11.CA_COUNTY
),
A4 AS (
    SELECT
        "A12".C0 C0,
        "A12".C1 C1,
        "A12".C2 C2,
        NULL C3,
        SUM("A12".C4) C4,
        SUM("A12".C5) C5,
        SUM("A12".C6) C6,
        SUM("A12".C7) C7,
        SUM("A12".C8) C8,
        SUM("A12".C9) C9,
        SUM("A12".C10) C10,
        SUM("A12".C11) C11,
        SUM("A12".C12) C12,
        SUM("A12".C13) C13,
        SUM("A12".C14) C14,
        SUM("A12".C15) C15,
        SUM("A12".C16) C16,
        SUM("A12".C17) C17
    FROM
        A5 "A12"
    GROUP BY
        "A12".C0,
        "A12".C1,
        "A12".C2
),
A3 AS (
    SELECT
        "A13".C0 C0,
        "A13".C1 C1,
        NULL C2,
        NULL C3,
        SUM("A13".C4) C4,
        SUM("A13".C5) C5,
        SUM("A13".C6) C6,
        SUM("A13".C7) C7,
        SUM("A13".C8) C8,
        SUM("A13".C9) C9,
        SUM("A13".C10) C10,
        SUM("A13".C11) C11,
        SUM("A13".C12) C12,
        SUM("A13".C13) C13,
        SUM("A13".C14) C14,
        SUM("A13".C15) C15,
        SUM("A13".C16) C16,
        SUM("A13".C17) C17
    FROM
        A4 "A13"
    GROUP BY
        "A13".C0,
        "A13".C1
),
A2 AS (
    SELECT
        "A14".C0 C0,
        NULL C1,
        NULL C2,
        NULL C3,
        SUM("A14".C4) C4,
        SUM("A14".C5) C5,
        SUM("A14".C6) C6,
        SUM("A14".C7) C7,
        SUM("A14".C8) C8,
        SUM("A14".C9) C9,
        SUM("A14".C10) C10,
        SUM("A14".C11) C11,
        SUM("A14".C12) C12,
        SUM("A14".C13) C13,
        SUM("A14".C14) C14,
        SUM("A14".C15) C15,
        SUM("A14".C16) C16,
        SUM("A14".C17) C17
    FROM
        A3 "A14"
    GROUP BY
        "A14".C0
)
SELECT
    A0.C0 "I_ITEM_ID",
    A0.C1 "CA_COUNTRY",
    A0.C2 "CA_STATE",
    A0.C3 "CA_COUNTY",
    (A0.C16 / A0.C17) "AGG1",
    (A0.C14 / A0.C15) "AGG2",
    (A0.C12 / A0.C13) "AGG3",
    (A0.C10 / A0.C11) "AGG4",
    (A0.C8 / A0.C9) "AGG5",
    (A0.C6 / A0.C7) "AGG6",
    (A0.C4 / A0.C5) "AGG7"
FROM
    (
        (
            SELECT
                NULL C0,
                A1.C0 C1,
                NULL C2,
                A1.C1 C3,
                A1.C2 C4,
                COALESCE(A1.C3, 0000000000000000000000000000000.) C5,
                A1.C4 C6,
                COALESCE(A1.C5, 0000000000000000000000000000000.) C7,
                A1.C6 C8,
                COALESCE(A1.C7, 0000000000000000000000000000000.) C9,
                A1.C8 C10,
                COALESCE(A1.C9, 0000000000000000000000000000000.) C11,
                A1.C10 C12,
                COALESCE(A1.C11, 0000000000000000000000000000000.) C13,
                A1.C12 C14,
                COALESCE(A1.C13, 0000000000000000000000000000000.) C15,
                A1.C14 C16,
                COALESCE(A1.C15, 0000000000000000000000000000000.) C17
            FROM
                (
                    SELECT
                        NULL C0,
                        NULL C1,
                        SUM("A15".C4) C2,
                        SUM("A15".C5) C3,
                        SUM("A15".C6) C4,
                        SUM("A15".C7) C5,
                        SUM("A15".C8) C6,
                        SUM("A15".C9) C7,
                        SUM("A15".C10) C8,
                        SUM("A15".C11) C9,
                        SUM("A15".C12) C10,
                        SUM("A15".C13) C11,
                        SUM("A15".C14) C12,
                        SUM("A15".C15) C13,
                        SUM("A15".C16) C14,
                        SUM("A15".C17) C15
                    FROM
                        A2 "A15"
                ) A1
            limit
                100
        )
        UNION
        ALL (
            SELECT
                "A16".C0 C0,
                "A16".C1 C1,
                "A16".C2 C2,
                "A16".C3 C3,
                "A16".C4 C4,
                "A16".C5 C5,
                "A16".C6 C6,
                "A16".C7 C7,
                "A16".C8 C8,
                "A16".C9 C9,
                "A16".C10 C10,
                "A16".C11 C11,
                "A16".C12 C12,
                "A16".C13 C13,
                "A16".C14 C14,
                "A16".C15 C15,
                "A16".C16 C16,
                "A16".C17 C17
            FROM
                A2 "A16"
        )
        UNION
        ALL (
            SELECT
                "A17".C0 C0,
                "A17".C1 C1,
                "A17".C2 C2,
                "A17".C3 C3,
                "A17".C4 C4,
                "A17".C5 C5,
                "A17".C6 C6,
                "A17".C7 C7,
                "A17".C8 C8,
                "A17".C9 C9,
                "A17".C10 C10,
                "A17".C11 C11,
                "A17".C12 C12,
                "A17".C13 C13,
                "A17".C14 C14,
                "A17".C15 C15,
                "A17".C16 C16,
                "A17".C17 C17
            FROM
                A3 "A17"
        )
        UNION
        ALL (
            SELECT
                "A18".C0 C0,
                "A18".C1 C1,
                "A18".C2 C2,
                "A18".C3 C3,
                "A18".C4 C4,
                "A18".C5 C5,
                "A18".C6 C6,
                "A18".C7 C7,
                "A18".C8 C8,
                "A18".C9 C9,
                "A18".C10 C10,
                "A18".C11 C11,
                "A18".C12 C12,
                "A18".C13 C13,
                "A18".C14 C14,
                "A18".C15 C15,
                "A18".C16 C16,
                "A18".C17 C17
            FROM
                A4 "A18"
        )
        UNION
        ALL (
            SELECT
                "A19".C0 C0,
                "A19".C1 C1,
                "A19".C2 C2,
                "A19".C3 C3,
                "A19".C4 C4,
                "A19".C5 C5,
                "A19".C6 C6,
                "A19".C7 C7,
                "A19".C8 C8,
                "A19".C9 C9,
                "A19".C10 C10,
                "A19".C11 C11,
                "A19".C12 C12,
                "A19".C13 C13,
                "A19".C14 C14,
                "A19".C15 C15,
                "A19".C16 C16,
                "A19".C17 C17
            FROM
                A5 "A19"
        )
    ) A0
ORDER BY
    2 ASC,
    3 ASC,
    4 ASC,
    1 ASC
limit
    100;