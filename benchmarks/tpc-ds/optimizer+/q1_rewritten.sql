WITH A2 AS (
    SELECT
        A3.SR_CUSTOMER_SK C0,
        A3.SR_STORE_SK C1,
        SUM(A3.SR_RETURN_AMT_INC_TAX) C2
    FROM
        (
            STORE_RETURNS A3
            INNER JOIN DATE_DIM A4 ON (A3.SR_RETURNED_DATE_SK = A4.D_DATE_SK)
        )
    WHERE
        (A4.D_YEAR = 1999)
    GROUP BY
        A3.SR_CUSTOMER_SK,
        A3.SR_STORE_SK
),
A0 AS (
    SELECT
        A1.C_CUSTOMER_ID C0,
        "A5".C2 C1,
        "A5".C1 C2
    FROM
        (
            CUSTOMER A1
            INNER JOIN (
                A2 "A5"
                INNER JOIN STORE A6 ON (A6.S_STORE_SK = "A5".C1)
            ) ON ("A5".C0 = A1.C_CUSTOMER_SK)
        )
    WHERE
        (A6.S_STATE = 'TN')
)
SELECT
    "A7".C0 "C_CUSTOMER_ID"
FROM
    (
        A0 "A7"
        INNER JOIN (
            SELECT
                SUM("A9".C2) C0,
                COUNT("A9".C2) C1,
                A10.C0 C2
            FROM
                (
                    A2 "A9"
                    INNER JOIN (
                        SELECT
                            DISTINCT "A11".C2 C0
                        FROM
                            A0 "A11"
                    ) A10 ON (A10.C0 = "A9".C1)
                )
            GROUP BY
                A10.C0
        ) A8 ON (A8.C2 = "A7".C2)
        AND (((A8.C0 / A8.C1) * 1.2) < "A7".C1)
    )
ORDER BY
    1 ASC
limit
    100;