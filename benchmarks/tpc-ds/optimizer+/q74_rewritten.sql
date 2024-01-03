WITH A0 AS (
    (
        SELECT
            A1.C0 C0,
            A1.C1 C1,
            A1.C2 C2,
            A1.C3 C3,
            A1.C4 C4,
            'w' C5
        FROM
            (
                SELECT
                    A2.C_CUSTOMER_ID C0,
                    A2.C_FIRST_NAME C1,
                    A2.C_LAST_NAME C2,
                    A4.D_YEAR C3,
                    STDDEV_SAMP(A3.WS_NET_PAID) C4
                FROM
                    (
                        CUSTOMER A2
                        INNER JOIN (
                            WEB_SALES A3
                            INNER JOIN DATE_DIM A4 ON (A3.WS_SOLD_DATE_SK = A4.D_DATE_SK)
                        ) ON (A2.C_CUSTOMER_SK = A3.WS_BILL_CUSTOMER_SK)
                    )
                WHERE
                    (A4.D_YEAR IN (2002, 2001))
                GROUP BY
                    A2.C_CUSTOMER_ID,
                    A2.C_FIRST_NAME,
                    A2.C_LAST_NAME,
                    A4.D_YEAR
            ) A1
        WHERE
            (
                (A1.C3 = 2002)
                OR (
                    (A1.C3 = 2001)
                    AND (+ 0.0000000000000000E + 000 < A1.C4)
                )
            )
    )
    UNION
    ALL (
        SELECT
            A5.C0 C0,
            A5.C1 C1,
            A5.C2 C2,
            A5.C3 C3,
            A5.C4 C4,
            's' C5
        FROM
            (
                SELECT
                    A6.C_CUSTOMER_ID C0,
                    A6.C_FIRST_NAME C1,
                    A6.C_LAST_NAME C2,
                    A8.D_YEAR C3,
                    STDDEV_SAMP(A7.SS_NET_PAID) C4
                FROM
                    (
                        CUSTOMER A6
                        INNER JOIN (
                            STORE_SALES A7
                            INNER JOIN DATE_DIM A8 ON (A7.SS_SOLD_DATE_SK = A8.D_DATE_SK)
                        ) ON (A6.C_CUSTOMER_SK = A7.SS_CUSTOMER_SK)
                    )
                WHERE
                    (A8.D_YEAR IN (2002, 2001))
                GROUP BY
                    A6.C_CUSTOMER_ID,
                    A6.C_FIRST_NAME,
                    A6.C_LAST_NAME,
                    A8.D_YEAR
            ) A5
        WHERE
            (
                (A5.C3 = 2002)
                OR (
                    (A5.C3 = 2001)
                    AND (+ 0.0000000000000000E + 000 < A5.C4)
                )
            )
    )
)
SELECT
    "A9".C0 "CUSTOMER_ID",
    "A9".C1 "CUSTOMER_FIRST_NAME",
    "A9".C2 "CUSTOMER_LAST_NAME"
FROM
    (
        (
            A0 "A9"
            INNER JOIN A0 "A10" ON ("A10".C5 = "A9".C5)
            AND ("A9".C0 = "A10".C0)
        )
        INNER JOIN (
            A0 "A11"
            INNER JOIN A0 "A12" ON ("A12".C0 = "A11".C0)
            AND ("A12".C5 = "A11".C5)
        ) ON ("A9".C3 = "A11".C3)
        AND ("A10".C3 = "A12".C3)
        AND ("A11".C0 = "A10".C0)
        AND (
            CASE
                WHEN ("A10".C4 > + 0.0000000000000000E + 000) THEN ("A9".C4 / "A10".C4)
                ELSE NULL
            END < CASE
                WHEN ("A12".C4 > + 0.0000000000000000E + 000) THEN ("A11".C4 / "A12".C4)
                ELSE NULL
            END
        )
    )
WHERE
    ("A9".C5 = 's')
    AND ("A9".C3 = 2002)
    AND ("A10".C5 = 's')
    AND ("A10".C3 = 2001)
    AND (+ 0.0000000000000000E + 000 < "A10".C4)
    AND ("A11".C5 = 'w')
    AND ("A11".C3 = 2002)
    AND ("A12".C5 = 'w')
    AND ("A12".C3 = 2001)
    AND (+ 0.0000000000000000E + 000 < "A12".C4)
ORDER BY
    3 ASC,
    2 ASC,
    1 ASC
limit
    100;