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
                    WHEN (A8.C0 = 1) THEN A1.C0
                    WHEN (A8.C0 = 2) THEN A1.C0
                    WHEN (A8.C0 = 3) THEN A1.C0
                    WHEN (A8.C0 = 4) THEN A1.C0
                    ELSE NULL
                END C0,
                CASE
                    WHEN (A8.C0 = 1) THEN A1.C1
                    WHEN (A8.C0 = 2) THEN A1.C1
                    WHEN (A8.C0 = 3) THEN A1.C1
                    WHEN (A8.C0 = 4) THEN NULL
                    ELSE NULL
                END C1,
                CASE
                    WHEN (A8.C0 = 1) THEN A1.C2
                    WHEN (A8.C0 = 2) THEN A1.C2
                    WHEN (A8.C0 = 3) THEN NULL
                    WHEN (A8.C0 = 4) THEN NULL
                    ELSE NULL
                END C2,
                CASE
                    WHEN (A8.C0 = 1) THEN A1.C3
                    WHEN (A8.C0 = 2) THEN NULL
                    WHEN (A8.C0 = 3) THEN NULL
                    WHEN (A8.C0 = 4) THEN NULL
                    ELSE NULL
                END C3,
                SUM(A1.C17) C4,
                SUM(A1.C16) C5,
                SUM(A1.C15) C6,
                SUM(A1.C14) C7,
                SUM(A1.C13) C8,
                SUM(A1.C12) C9,
                SUM(A1.C11) C10,
                SUM(A1.C10) C11,
                SUM(A1.C9) C12,
                SUM(A1.C8) C13,
                SUM(A1.C7) C14,
                SUM(A1.C6) C15,
                SUM(A1.C5) C16,
                SUM(A1.C4) C17,
                A8.C0 C18
            FROM
                (
                    (
                        SELECT
                            A2.I_ITEM_ID C0,
                            A7.CA_COUNTRY C1,
                            A7.CA_STATE C2,
                            A7.CA_COUNTY C3,
                            COUNT(CAST(A4.CS_QUANTITY AS DECIMAL(12, 2))) C4,
                            SUM(CAST(A4.CS_QUANTITY AS DECIMAL(12, 2))) C5,
                            COUNT(CAST(A4.CS_LIST_PRICE AS DECIMAL(12, 2))) C6,
                            SUM(CAST(A4.CS_LIST_PRICE AS DECIMAL(12, 2))) C7,
                            COUNT(CAST(A4.CS_COUPON_AMT AS DECIMAL(12, 2))) C8,
                            SUM(CAST(A4.CS_COUPON_AMT AS DECIMAL(12, 2))) C9,
                            COUNT(CAST(A4.CS_SALES_PRICE AS DECIMAL(12, 2))) C10,
                            SUM(CAST(A4.CS_SALES_PRICE AS DECIMAL(12, 2))) C11,
                            COUNT(CAST(A4.CS_NET_PROFIT AS DECIMAL(12, 2))) C12,
                            SUM(CAST(A4.CS_NET_PROFIT AS DECIMAL(12, 2))) C13,
                            COUNT(CAST(A3.C_BIRTH_YEAR AS DECIMAL(12, 2))) C14,
                            SUM(CAST(A3.C_BIRTH_YEAR AS DECIMAL(12, 2))) C15,
                            COUNT(CAST(A6.CD_DEP_COUNT AS DECIMAL(12, 2))) C16,
                            SUM(CAST(A6.CD_DEP_COUNT AS DECIMAL(12, 2))) C17
                        FROM
                            (
                                ITEM A2
                                INNER JOIN (
                                    (
                                        CUSTOMER A3
                                        INNER JOIN (
                                            (
                                                CATALOG_SALES A4
                                                INNER JOIN DATE_DIM A5 ON (A4.CS_SOLD_DATE_SK = A5.D_DATE_SK)
                                            )
                                            INNER JOIN CUSTOMER_DEMOGRAPHICS A6 ON (A4.CS_BILL_CDEMO_SK = A6.CD_DEMO_SK)
                                        ) ON (A4.CS_BILL_CUSTOMER_SK = A3.C_CUSTOMER_SK)
                                    )
                                    INNER JOIN CUSTOMER_ADDRESS A7 ON (
                                        CAST(A3.C_CURRENT_ADDR_SK AS BIGINT) = A7.CA_ADDRESS_SK
                                    )
                                ) ON (A4.CS_ITEM_SK = A2.I_ITEM_SK)
                            )
                        WHERE
                            (A3.C_BIRTH_MONTH IN (1, 2, 9, 5, 11, 3))
                            AND (A3.C_CURRENT_CDEMO_SK IS NOT NULL)
                            AND (A5.D_YEAR = 1998)
                            AND (A6.CD_GENDER = 'M')
                            AND (A6.CD_EDUCATION_STATUS = 'Primary             ')
                            AND (
                                A7.CA_STATE IN ('MS', 'NE', 'IA', 'MI', 'GA', 'NY', 'CO')
                            )
                        GROUP BY
                            A2.I_ITEM_ID,
                            A7.CA_COUNTRY,
                            A7.CA_STATE,
                            A7.CA_COUNTY
                    ) A1
                    INNER JOIN (
                        VALUES
                            1,
                            2,
                            3,
                            4,
                            5
                    ) A8 (C0) ON (MOD(LENGTH(A8.C0), 1) = MOD(LENGTH(A1.C0), 1))
                )
            GROUP BY
                A8.C0,
                CASE
                    WHEN (A8.C0 = 1) THEN A1.C0
                    WHEN (A8.C0 = 2) THEN A1.C0
                    WHEN (A8.C0 = 3) THEN A1.C0
                    WHEN (A8.C0 = 4) THEN A1.C0
                    ELSE NULL
                END,
                CASE
                    WHEN (A8.C0 = 1) THEN A1.C1
                    WHEN (A8.C0 = 2) THEN A1.C1
                    WHEN (A8.C0 = 3) THEN A1.C1
                    WHEN (A8.C0 = 4) THEN NULL
                    ELSE NULL
                END,
                CASE
                    WHEN (A8.C0 = 1) THEN A1.C2
                    WHEN (A8.C0 = 2) THEN A1.C2
                    WHEN (A8.C0 = 3) THEN NULL
                    WHEN (A8.C0 = 4) THEN NULL
                    ELSE NULL
                END,
                CASE
                    WHEN (A8.C0 = 1) THEN A1.C3
                    WHEN (A8.C0 = 2) THEN NULL
                    WHEN (A8.C0 = 3) THEN NULL
                    WHEN (A8.C0 = 4) THEN NULL
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
        ) A9 (C0) ON (A9.C0 = A0.C18)
    )
WHERE
    (
        (
            (A9.C0 = 5)
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