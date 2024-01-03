SELECT
    A0.C0 "I_ITEM_ID",
    A0.C1 "CA_COUNTRY",
    A0.C2 "CA_STATE",
    A0.C3 "CA_COUNTY",
    (
        A0.C16 / CASE
            WHEN (A0.C18 IS NOT NULL) THEN A0.C17
            ELSE 0000000000000000000000000000000.
        END
    ) "AGG1",
    (
        A0.C14 / CASE
            WHEN (A0.C18 IS NOT NULL) THEN A0.C15
            ELSE 0000000000000000000000000000000.
        END
    ) "AGG2",
    (
        A0.C12 / CASE
            WHEN (A0.C18 IS NOT NULL) THEN A0.C13
            ELSE 0000000000000000000000000000000.
        END
    ) "AGG3",
    (
        A0.C10 / CASE
            WHEN (A0.C18 IS NOT NULL) THEN A0.C11
            ELSE 0000000000000000000000000000000.
        END
    ) "AGG4",
    (
        A0.C8 / CASE
            WHEN (A0.C18 IS NOT NULL) THEN A0.C9
            ELSE 0000000000000000000000000000000.
        END
    ) "AGG5",
    (
        A0.C6 / CASE
            WHEN (A0.C18 IS NOT NULL) THEN A0.C7
            ELSE 0000000000000000000000000000000.
        END
    ) "AGG6",
    (
        A0.C4 / CASE
            WHEN (A0.C18 IS NOT NULL) THEN A0.C5
            ELSE 0000000000000000000000000000000.
        END
    ) "AGG7"
FROM
    (
        (
            SELECT
                CASE
                    WHEN (A7.C0 = 1) THEN A1.I_ITEM_ID
                    WHEN (A7.C0 = 2) THEN A1.I_ITEM_ID
                    WHEN (A7.C0 = 3) THEN A1.I_ITEM_ID
                    WHEN (A7.C0 = 4) THEN A1.I_ITEM_ID
                    ELSE NULL
                END C0,
                CASE
                    WHEN (A7.C0 = 1) THEN A6.CA_COUNTRY
                    WHEN (A7.C0 = 2) THEN A6.CA_COUNTRY
                    WHEN (A7.C0 = 3) THEN A6.CA_COUNTRY
                    WHEN (A7.C0 = 4) THEN NULL
                    ELSE NULL
                END C1,
                CASE
                    WHEN (A7.C0 = 1) THEN A6.CA_STATE
                    WHEN (A7.C0 = 2) THEN A6.CA_STATE
                    WHEN (A7.C0 = 3) THEN NULL
                    WHEN (A7.C0 = 4) THEN NULL
                    ELSE NULL
                END C2,
                CASE
                    WHEN (A7.C0 = 1) THEN A6.CA_COUNTY
                    WHEN (A7.C0 = 2) THEN NULL
                    WHEN (A7.C0 = 3) THEN NULL
                    WHEN (A7.C0 = 4) THEN NULL
                    ELSE NULL
                END C3,
                SUM(CAST(A5.CD_DEP_COUNT AS DECIMAL(12, 2))) C4,
                COUNT(CAST(A5.CD_DEP_COUNT AS DECIMAL(12, 2))) C5,
                SUM(CAST(A2.C_BIRTH_YEAR AS DECIMAL(12, 2))) C6,
                COUNT(CAST(A2.C_BIRTH_YEAR AS DECIMAL(12, 2))) C7,
                SUM(CAST(A3.CS_NET_PROFIT AS DECIMAL(12, 2))) C8,
                COUNT(CAST(A3.CS_NET_PROFIT AS DECIMAL(12, 2))) C9,
                SUM(CAST(A3.CS_SALES_PRICE AS DECIMAL(12, 2))) C10,
                COUNT(CAST(A3.CS_SALES_PRICE AS DECIMAL(12, 2))) C11,
                SUM(CAST(A3.CS_COUPON_AMT AS DECIMAL(12, 2))) C12,
                COUNT(CAST(A3.CS_COUPON_AMT AS DECIMAL(12, 2))) C13,
                SUM(CAST(A3.CS_LIST_PRICE AS DECIMAL(12, 2))) C14,
                COUNT(CAST(A3.CS_LIST_PRICE AS DECIMAL(12, 2))) C15,
                SUM(CAST(A3.CS_QUANTITY AS DECIMAL(12, 2))) C16,
                COUNT(CAST(A3.CS_QUANTITY AS DECIMAL(12, 2))) C17,
                A7.C0 C18
            FROM
                (
                    (
                        ITEM A1
                        INNER JOIN (
                            (
                                CUSTOMER A2
                                INNER JOIN (
                                    (
                                        CATALOG_SALES A3
                                        INNER JOIN DATE_DIM A4 ON (A3.CS_SOLD_DATE_SK = A4.D_DATE_SK)
                                    )
                                    INNER JOIN CUSTOMER_DEMOGRAPHICS A5 ON (A3.CS_BILL_CDEMO_SK = A5.CD_DEMO_SK)
                                ) ON (A3.CS_BILL_CUSTOMER_SK = A2.C_CUSTOMER_SK)
                            )
                            INNER JOIN CUSTOMER_ADDRESS A6 ON (
                                CAST(A2.C_CURRENT_ADDR_SK AS BIGINT) = A6.CA_ADDRESS_SK
                            )
                        ) ON (A3.CS_ITEM_SK = A1.I_ITEM_SK)
                    )
                    INNER JOIN (
                        VALUES
                            1,
                            2,
                            3,
                            4,
                            5
                    ) A7 (C0) ON (
                        MOD(LENGTH(A7.C0), 1) = MOD(LENGTH(A1.I_ITEM_ID), 1)
                    )
                )
            WHERE
                (A2.C_BIRTH_MONTH IN (1, 2, 9, 5, 11, 3))
                AND (A2.C_CURRENT_CDEMO_SK IS NOT NULL)
                AND (A4.D_YEAR = 1998)
                AND (A5.CD_EDUCATION_STATUS = 'Primary             ')
                AND (A5.CD_GENDER = 'M')
                AND (
                    A6.CA_STATE IN ('MS', 'NE', 'IA', 'MI', 'GA', 'NY', 'CO')
                )
            GROUP BY
                A7.C0,
                CASE
                    WHEN (A7.C0 = 1) THEN A1.I_ITEM_ID
                    WHEN (A7.C0 = 2) THEN A1.I_ITEM_ID
                    WHEN (A7.C0 = 3) THEN A1.I_ITEM_ID
                    WHEN (A7.C0 = 4) THEN A1.I_ITEM_ID
                    ELSE NULL
                END,
                CASE
                    WHEN (A7.C0 = 1) THEN A6.CA_COUNTRY
                    WHEN (A7.C0 = 2) THEN A6.CA_COUNTRY
                    WHEN (A7.C0 = 3) THEN A6.CA_COUNTRY
                    WHEN (A7.C0 = 4) THEN NULL
                    ELSE NULL
                END,
                CASE
                    WHEN (A7.C0 = 1) THEN A6.CA_STATE
                    WHEN (A7.C0 = 2) THEN A6.CA_STATE
                    WHEN (A7.C0 = 3) THEN NULL
                    WHEN (A7.C0 = 4) THEN NULL
                    ELSE NULL
                END,
                CASE
                    WHEN (A7.C0 = 1) THEN A6.CA_COUNTY
                    WHEN (A7.C0 = 2) THEN NULL
                    WHEN (A7.C0 = 3) THEN NULL
                    WHEN (A7.C0 = 4) THEN NULL
                    ELSE NULL
                END
        ) A0
        RIGHT OUTER JOIN (
            VALUES
                1,
                2,
                3,
                4,
                5
        ) A8 (C0) ON (A8.C0 = A0.C18)
    )
WHERE
    (
        (
            (A8.C0 = 5)
            AND (A0.C18 IS NULL)
        )
        OR (A0.C18 IS NOT NULL)
    )
ORDER BY
    2 ASC,
    3 ASC,
    4 ASC,
    1 ASC
limit
    100;