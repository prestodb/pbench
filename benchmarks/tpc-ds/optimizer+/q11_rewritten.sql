WITH A0 AS (
    (
        SELECT
            A1.C0 C0,
            A1.C1 C1,
            A1.C2 C2,
            A1.C3 C3,
            A1.C4 C4,
            A1.C5 C5,
            'w' C6
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
                                SUM((A3.WS_EXT_LIST_PRICE - A3.WS_EXT_DISCOUNT_AMT)) C0,
                                A3.WS_BILL_CUSTOMER_SK C1,
                                A4.D_YEAR C2
                            FROM
                                (
                                    WEB_SALES A3
                                    INNER JOIN DATE_DIM A4 ON (A3.WS_SOLD_DATE_SK = A4.D_DATE_SK)
                                )
                            WHERE
                                (A4.D_YEAR IN (2000, 1999))
                            GROUP BY
                                A3.WS_BILL_CUSTOMER_SK,
                                A4.D_YEAR A5
                        ) ON (A2.C_CUSTOMER_SK = A5.C1)
                        AND (A5.C2 IN (2000, 1999))
                        AND (A5.WS_SOLD_DATE_SK = A5.D_DATE_SK)
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
                (A1.C4 = 2000)
                OR (
                    (A1.C4 = 1999)
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
            's' C6
        FROM
            (
                SELECT
                    A7.C_CUSTOMER_ID C0,
                    A7.C_FIRST_NAME C1,
                    A7.C_LAST_NAME C2,
                    A7.C_EMAIL_ADDRESS C3,
                    A10.C2 C4,
                    SUM(A10.C0) C5
                FROM
                    (
                        CUSTOMER A7
                        INNER JOIN (
                            SELECT
                                SUM((A8.SS_EXT_LIST_PRICE - A8.SS_EXT_DISCOUNT_AMT)) C0,
                                A8.SS_CUSTOMER_SK C1,
                                A9.D_YEAR C2
                            FROM
                                (
                                    STORE_SALES A8
                                    INNER JOIN DATE_DIM A9 ON (A8.SS_SOLD_DATE_SK = A9.D_DATE_SK)
                                )
                            WHERE
                                (A9.D_YEAR IN (2000, 1999))
                            GROUP BY
                                A8.SS_CUSTOMER_SK,
                                A9.D_YEAR A10
                        ) ON (A7.C_CUSTOMER_SK = A10.C1)
                        AND (A10.C2 IN (2000, 1999))
                        AND (A10.SS_SOLD_DATE_SK = A10.D_DATE_SK)
                    )
                GROUP BY
                    A7.C_CUSTOMER_ID,
                    A7.C_FIRST_NAME,
                    A7.C_LAST_NAME,
                    A7.C_EMAIL_ADDRESS,
                    A10.C2,
                    A7.C_PREFERRED_CUST_FLAG,
                    A7.C_BIRTH_COUNTRY,
                    A7.C_LOGIN
            ) A6
        WHERE
            (
                (A6.C4 = 2000)
                OR (
                    (A6.C4 = 1999)
                    AND (0 < A6.C5)
                )
            )
    )
)
SELECT
    "A11".C0 "CUSTOMER_ID",
    "A11".C1 "CUSTOMER_FIRST_NAME",
    "A11".C2 "CUSTOMER_LAST_NAME",
    "A11".C3 "CUSTOMER_EMAIL_ADDRESS"
FROM
    (
        (
            A0 "A11"
            INNER JOIN (
                A0 "A12"
                INNER JOIN A0 "A13" ON ("A13".C0 = "A12".C0)
                AND ("A13".C4 = "A12".C4)
            ) ON ("A13".C6 = "A11".C6)
            AND ("A11".C0 = "A13".C0)
        )
        INNER JOIN A0 "A14" ON ("A11".C4 = "A14".C4)
        AND ("A12".C6 = "A14".C6)
        AND ("A14".C0 = "A13".C0)
        AND (
            CASE
                WHEN ("A13".C5 > 0) THEN CAST(("A11".C5 / "A13".C5) AS DECIMAL(31, 1))
                ELSE 000000000000000000000000000000.0
            END < CASE
                WHEN ("A12".C5 > 0) THEN CAST(("A14".C5 / "A12".C5) AS DECIMAL(31, 1))
                ELSE 000000000000000000000000000000.0
            END
        )
    )
WHERE
    ("A11".C6 = 's')
    AND ("A11".C4 = 2000)
    AND ("A12".C6 = 'w')
    AND ("A12".C4 = 1999)
    AND (0 < "A12".C5)
    AND ("A13".C6 = 's')
    AND ("A13".C4 = 1999)
    AND (0 < "A13".C5)
    AND ("A14".C6 = 'w')
    AND ("A14".C4 = 2000)
ORDER BY
    1 ASC,
    2 ASC,
    3 ASC,
    4 ASC
limit
    100;