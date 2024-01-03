WITH A1 AS (
    SELECT
        A3.WR_RETURNING_CUSTOMER_SK C0,
        A2.CA_STATE C1,
        SUM(A3.WR_RETURN_AMT) C2
    FROM
        (
            CUSTOMER_ADDRESS A2
            INNER JOIN (
                WEB_RETURNS A3
                INNER JOIN DATE_DIM A4 ON (A3.WR_RETURNED_DATE_SK = A4.D_DATE_SK)
            ) ON (A3.WR_RETURNING_ADDR_SK = A2.CA_ADDRESS_SK)
        )
    WHERE
        (A4.D_YEAR = 2000)
    GROUP BY
        A3.WR_RETURNING_CUSTOMER_SK,
        A2.CA_STATE
),
A0 AS (
    SELECT
        A6.C_LAST_REVIEW_DATE_SK C0,
        A6.C_EMAIL_ADDRESS C1,
        A6.C_LOGIN C2,
        A6.C_BIRTH_COUNTRY C3,
        A6.C_BIRTH_YEAR C4,
        A6.C_BIRTH_MONTH C5,
        A6.C_BIRTH_DAY C6,
        A6.C_PREFERRED_CUST_FLAG C7,
        A6.C_LAST_NAME C8,
        A6.C_FIRST_NAME C9,
        A6.C_SALUTATION C10,
        A6.C_CUSTOMER_ID C11,
        "A5".C2 C12,
        "A5".C1 C13
    FROM
        (
            A1 "A5"
            INNER JOIN (
                CUSTOMER A6
                INNER JOIN CUSTOMER_ADDRESS A7 ON (
                    A7.CA_ADDRESS_SK = CAST(A6.C_CURRENT_ADDR_SK AS BIGINT)
                )
            ) ON ("A5".C0 = A6.C_CUSTOMER_SK)
        )
    WHERE
        (A7.CA_STATE = 'KS')
)
SELECT
    "A8".C11 "C_CUSTOMER_ID",
    "A8".C10 "C_SALUTATION",
    "A8".C9 "C_FIRST_NAME",
    "A8".C8 "C_LAST_NAME",
    "A8".C7 "C_PREFERRED_CUST_FLAG",
    "A8".C6 "C_BIRTH_DAY",
    "A8".C5 "C_BIRTH_MONTH",
    "A8".C4 "C_BIRTH_YEAR",
    "A8".C3 "C_BIRTH_COUNTRY",
    "A8".C2 "C_LOGIN",
    "A8".C1 "C_EMAIL_ADDRESS",
    "A8".C0 "C_LAST_REVIEW_DATE_SK",
    "A8".C12 "CTR_TOTAL_RETURN"
FROM
    (
        A0 "A8"
        INNER JOIN (
            SELECT
                SUM("A10".C2) C0,
                COUNT("A10".C2) C1,
                A11.C0 C2
            FROM
                (
                    A1 "A10"
                    INNER JOIN (
                        SELECT
                            DISTINCT "A12".C13 C0
                        FROM
                            A0 "A12"
                    ) A11 ON (A11.C0 = "A10".C1)
                )
            GROUP BY
                A11.C0
        ) A9 ON (A9.C2 = "A8".C13)
        AND (((A9.C0 / A9.C1) * 1.2) < "A8".C12)
    )
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
    10 ASC,
    11 ASC,
    12 ASC,
    13 ASC
limit
    100;