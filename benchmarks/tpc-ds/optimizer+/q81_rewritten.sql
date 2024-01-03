WITH A1 AS (
    SELECT
        A3.CR_RETURNING_CUSTOMER_SK C0,
        A2.CA_STATE C1,
        SUM(A3.CR_RETURN_AMT_INC_TAX) C2
    FROM
        (
            CUSTOMER_ADDRESS A2
            INNER JOIN (
                CATALOG_RETURNS A3
                INNER JOIN DATE_DIM A4 ON (A3.CR_RETURNED_DATE_SK = A4.D_DATE_SK)
            ) ON (A3.CR_RETURNING_ADDR_SK = A2.CA_ADDRESS_SK)
        )
    WHERE
        (A4.D_YEAR = 1998)
    GROUP BY
        A3.CR_RETURNING_CUSTOMER_SK,
        A2.CA_STATE
),
A0 AS (
    SELECT
        A7.CA_LOCATION_TYPE C0,
        A7.CA_GMT_OFFSET C1,
        A7.CA_COUNTRY C2,
        A7.CA_ZIP C3,
        A7.CA_COUNTY C4,
        A7.CA_CITY C5,
        A7.CA_SUITE_NUMBER C6,
        A7.CA_STREET_TYPE C7,
        A7.CA_STREET_NAME C8,
        A7.CA_STREET_NUMBER C9,
        A6.C_LAST_NAME C10,
        A6.C_FIRST_NAME C11,
        A6.C_SALUTATION C12,
        A6.C_CUSTOMER_ID C13,
        "A5".C2 C14,
        "A5".C1 C15
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
        (A7.CA_STATE = 'TX')
)
SELECT
    "A8".C13 "C_CUSTOMER_ID",
    "A8".C12 "C_SALUTATION",
    "A8".C11 "C_FIRST_NAME",
    "A8".C10 "C_LAST_NAME",
    "A8".C9 "CA_STREET_NUMBER",
    "A8".C8 "CA_STREET_NAME",
    "A8".C7 "CA_STREET_TYPE",
    "A8".C6 "CA_SUITE_NUMBER",
    "A8".C5 "CA_CITY",
    "A8".C4 "CA_COUNTY",
    'TX' "CA_STATE",
    "A8".C3 "CA_ZIP",
    "A8".C2 "CA_COUNTRY",
    "A8".C1 "CA_GMT_OFFSET",
    "A8".C0 "CA_LOCATION_TYPE",
    "A8".C14 "CTR_TOTAL_RETURN"
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
                            DISTINCT "A12".C15 C0
                        FROM
                            A0 "A12"
                    ) A11 ON (A11.C0 = "A10".C1)
                )
            GROUP BY
                A11.C0
        ) A9 ON (A9.C2 = "A8".C15)
        AND (((A9.C0 / A9.C1) * 1.2) < "A8".C14)
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
    12 ASC,
    13 ASC,
    14 ASC,
    15 ASC,
    16 ASC
limit
    100;