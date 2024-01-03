WITH A0 AS (
    (
        SELECT
            A1.C0 C0,
            A1.C1 C1,
            A1.C2 C2,
            A1.C3 C3,
            A1.C4 C4,
            A1.C5 C5,
            's' C6
        FROM
            (
                SELECT
                    A2.C_CUSTOMER_ID C0,
                    A2.C_FIRST_NAME C1,
                    A2.C_LAST_NAME C2,
                    A2.C_EMAIL_ADDRESS C3,
                    A5.C2 C4,
                    SUM(A5.C0) C5
                FROM
                    (
                        CUSTOMER A2
                        INNER JOIN (
                            SELECT
                                SUM(
                                    (
                                        (
                                            (
                                                (A3.SS_EXT_LIST_PRICE - A3.SS_EXT_WHOLESALE_COST) - A3.SS_EXT_DISCOUNT_AMT
                                            ) + A3.SS_EXT_SALES_PRICE
                                        ) / 2
                                    )
                                ) C0,
                                A3.SS_CUSTOMER_SK C1,
                                A4.D_YEAR C2
                            FROM
                                (
                                    STORE_SALES A3
                                    INNER JOIN DATE_DIM A4 ON (A3.SS_SOLD_DATE_SK = A4.D_DATE_SK)
                                )
                            WHERE
                                (A4.D_YEAR IN (2002, 2001))
                            GROUP BY
                                A3.SS_CUSTOMER_SK,
                                A4.D_YEAR A5
                        ) ON (A2.C_CUSTOMER_SK = A5.C1)
                        AND (A5.C2 IN (2002, 2001))
                        AND (A5.SS_SOLD_DATE_SK = A5.D_DATE_SK)
                    )
                GROUP BY
                    A2.C_CUSTOMER_ID,
                    A2.C_FIRST_NAME,
                    A2.C_LAST_NAME,
                    A2.C_EMAIL_ADDRESS,
                    A5.C2,
                    A2.C_PREFERRED_CUST_FLAG,
                    A2.C_BIRTH_COUNTRY,
                    A2.C_LOGIN
            ) A1
        WHERE
            (
                (A1.C4 = 2002)
                OR (
                    (A1.C4 = 2001)
                    AND (0 < A1.C5)
                )
            )
    )
    UNION
    ALL (
        SELECT
            A6.C0 C0,
            A6.C1 C1,
            A6.C2 C2,
            A6.C3 C3,
            A6.C4 C4,
            A6.C5 C5,
            'c' C6
        FROM
            (
                SELECT
                    A7.C_CUSTOMER_ID C0,
                    A7.C_FIRST_NAME C1,
                    A7.C_LAST_NAME C2,
                    A7.C_EMAIL_ADDRESS C3,
                    A9.D_YEAR C4,
                    SUM(
                        (
                            (
                                (
                                    (A8.CS_EXT_LIST_PRICE - A8.CS_EXT_WHOLESALE_COST) - A8.CS_EXT_DISCOUNT_AMT
                                ) + A8.CS_EXT_SALES_PRICE
                            ) / 2
                        )
                    ) C5
                FROM
                    (
                        CUSTOMER A7
                        INNER JOIN (
                            CATALOG_SALES A8
                            INNER JOIN DATE_DIM A9 ON (A8.CS_SOLD_DATE_SK = A9.D_DATE_SK)
                        ) ON (A7.C_CUSTOMER_SK = A8.CS_BILL_CUSTOMER_SK)
                    )
                WHERE
                    (A9.D_YEAR IN (2002, 2001))
                GROUP BY
                    A7.C_CUSTOMER_ID,
                    A7.C_FIRST_NAME,
                    A7.C_LAST_NAME,
                    A7.C_EMAIL_ADDRESS,
                    A9.D_YEAR,
                    A7.C_PREFERRED_CUST_FLAG,
                    A7.C_BIRTH_COUNTRY,
                    A7.C_LOGIN
            ) A6
        WHERE
            (
                (A6.C4 = 2002)
                OR (
                    (A6.C4 = 2001)
                    AND (0 < A6.C5)
                )
            )
    )
    UNION
    ALL (
        SELECT
            A10.C0 C0,
            A10.C1 C1,
            A10.C2 C2,
            A10.C3 C3,
            A10.C4 C4,
            A10.C5 C5,
            'w' C6
        FROM
            (
                SELECT
                    A11.C_CUSTOMER_ID C0,
                    A11.C_FIRST_NAME C1,
                    A11.C_LAST_NAME C2,
                    A11.C_EMAIL_ADDRESS C3,
                    A14.C2 C4,
                    SUM(A14.C0) C5
                FROM
                    (
                        CUSTOMER A11
                        INNER JOIN (
                            SELECT
                                SUM(
                                    (
                                        (
                                            (
                                                (
                                                    A12.WS_EXT_LIST_PRICE - A12.WS_EXT_WHOLESALE_COST
                                                ) - A12.WS_EXT_DISCOUNT_AMT
                                            ) + A12.WS_EXT_SALES_PRICE
                                        ) / 2
                                    )
                                ) C0,
                                A12.WS_BILL_CUSTOMER_SK C1,
                                A13.D_YEAR C2
                            FROM
                                (
                                    WEB_SALES A12
                                    INNER JOIN DATE_DIM A13 ON (A12.WS_SOLD_DATE_SK = A13.D_DATE_SK)
                                )
                            WHERE
                                (A13.D_YEAR IN (2002, 2001))
                            GROUP BY
                                A12.WS_BILL_CUSTOMER_SK,
                                A13.D_YEAR A14
                        ) ON (A11.C_CUSTOMER_SK = A14.C1)
                        AND (A14.C2 IN (2002, 2001))
                        AND (A14.WS_SOLD_DATE_SK = A14.D_DATE_SK)
                    )
                GROUP BY
                    A11.C_CUSTOMER_ID,
                    A11.C_FIRST_NAME,
                    A11.C_LAST_NAME,
                    A11.C_EMAIL_ADDRESS,
                    A14.C2,
                    A11.C_PREFERRED_CUST_FLAG,
                    A11.C_BIRTH_COUNTRY,
                    A11.C_LOGIN
            ) A10
        WHERE
            (
                (A10.C4 = 2002)
                OR (
                    (A10.C4 = 2001)
                    AND (0 < A10.C5)
                )
            )
    )
)
SELECT
    "A15".C0 "CUSTOMER_ID",
    "A15".C1 "CUSTOMER_FIRST_NAME",
    "A15".C2 "CUSTOMER_LAST_NAME",
    "A15".C3 "CUSTOMER_EMAIL_ADDRESS"
FROM
    (
        (
            A0 "A15"
            INNER JOIN (
                (
                    A0 "A16"
                    INNER JOIN (
                        A0 "A17"
                        INNER JOIN A0 "A18" ON ("A18".C4 = "A17".C4)
                        AND ("A17".C0 = "A18".C0)
                    ) ON ("A17".C6 = "A16".C6)
                    AND ("A16".C0 = "A18".C0)
                )
                INNER JOIN A0 "A19" ON ("A17".C4 = "A19".C4)
                AND ("A19".C0 = "A18".C0)
            ) ON ("A15".C4 = "A16".C4)
            AND ("A18".C6 = "A15".C6)
            AND ("A15".C0 = "A18".C0)
            AND (
                CASE
                    WHEN ("A18".C5 > 0) THEN ("A15".C5 / "A18".C5)
                    ELSE NULL
                END < CASE
                    WHEN ("A17".C5 > 0) THEN ("A16".C5 / "A17".C5)
                    ELSE NULL
                END
            )
        )
        INNER JOIN A0 "A20" ON ("A18".C0 = "A20".C0)
        AND ("A16".C4 = "A20".C4)
        AND ("A19".C6 = "A20".C6)
        AND (
            CASE
                WHEN ("A19".C5 > 0) THEN ("A20".C5 / "A19".C5)
                ELSE NULL
            END < CASE
                WHEN ("A17".C5 > 0) THEN ("A16".C5 / "A17".C5)
                ELSE NULL
            END
        )
    )
WHERE
    ("A15".C6 = 's')
    AND ("A15".C4 = 2002)
    AND ("A16".C6 = 'c')
    AND ("A16".C4 = 2002)
    AND ("A17".C6 = 'c')
    AND ("A17".C4 = 2001)
    AND (0 < "A17".C5)
    AND ("A18".C6 = 's')
    AND ("A18".C4 = 2001)
    AND (0 < "A18".C5)
    AND ("A19".C6 = 'w')
    AND ("A19".C4 = 2001)
    AND (0 < "A19".C5)
    AND ("A20".C6 = 'w')
    AND ("A20".C4 = 2002)
ORDER BY
    1 ASC,
    2 ASC,
    3 ASC,
    4 ASC
limit
    100;