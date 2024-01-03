WITH A0 AS (
    SELECT
        A1.CA_COUNTY C0,
        A3.D_QOY C1,
        A3.D_YEAR C2,
        SUM(A2.SS_EXT_SALES_PRICE) C3
    FROM
        (
            CUSTOMER_ADDRESS A1
            INNER JOIN (
                STORE_SALES A2
                INNER JOIN DATE_DIM A3 ON (A2.SS_SOLD_DATE_SK = A3.D_DATE_SK)
            ) ON (A2.SS_ADDR_SK = A1.CA_ADDRESS_SK)
        )
    WHERE
        (A3.D_QOY IN (2, 3, 1))
        AND (A3.D_YEAR = 1999)
    GROUP BY
        A1.CA_COUNTY,
        A3.D_QOY,
        A3.D_YEAR
),
A5 AS (
    SELECT
        A6.CA_COUNTY C0,
        A8.D_QOY C1,
        SUM(A7.WS_EXT_SALES_PRICE) C2
    FROM
        (
            CUSTOMER_ADDRESS A6
            INNER JOIN (
                WEB_SALES A7
                INNER JOIN DATE_DIM A8 ON (A7.WS_SOLD_DATE_SK = A8.D_DATE_SK)
            ) ON (A7.WS_BILL_ADDR_SK = A6.CA_ADDRESS_SK)
        )
    WHERE
        (A8.D_QOY IN (2, 3, 1))
        AND (A8.D_YEAR = 1999)
    GROUP BY
        A6.CA_COUNTY,
        A8.D_QOY,
        A8.D_YEAR
)
SELECT
    "A10".C0 "CA_COUNTY",
    "A10".C2 "D_YEAR",
    ("A12".C2 / "A13".C2) "WEB_Q1_Q2_INCREASE",
    ("A4".C3 / "A10".C3) "STORE_Q1_Q2_INCREASE",
    ("A9".C2 / "A12".C2) "WEB_Q2_Q3_INCREASE",
    ("A11".C3 / "A4".C3) "STORE_Q2_Q3_INCREASE"
FROM
    (
        (
            (
                (
                    A0 "A4"
                    INNER JOIN A5 "A9" ON ("A9".C0 = "A4".C0)
                )
                INNER JOIN (
                    A0 "A10"
                    INNER JOIN A0 "A11" ON ("A11".C0 = "A10".C0)
                ) ON ("A11".C1 = "A9".C1)
                AND ("A4".C0 = "A10".C0)
            )
            INNER JOIN A5 "A12" ON ("A4".C1 = "A12".C1)
            AND ("A10".C0 = "A12".C0)
            AND (
                CASE
                    WHEN ("A4".C3 > 0) THEN ("A11".C3 / "A4".C3)
                    ELSE NULL
                END < CASE
                    WHEN ("A12".C2 > 0) THEN ("A9".C2 / "A12".C2)
                    ELSE NULL
                END
            )
        )
        INNER JOIN A5 "A13" ON ("A10".C1 = "A13".C1)
        AND ("A12".C0 = "A13".C0)
        AND (
            CASE
                WHEN ("A10".C3 > 0) THEN ("A4".C3 / "A10".C3)
                ELSE NULL
            END < CASE
                WHEN ("A13".C2 > 0) THEN ("A12".C2 / "A13".C2)
                ELSE NULL
            END
        )
    )
WHERE
    ("A4".C1 = 2)
    AND ("A9".C1 = 3)
    AND ("A10".C1 = 1)
    AND ("A11".C1 = 3)
    AND ("A12".C1 = 2)
    AND ("A13".C1 = 1)
ORDER BY
    1 ASC;