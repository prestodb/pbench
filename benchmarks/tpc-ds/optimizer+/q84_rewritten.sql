SELECT
    A1.C_CUSTOMER_ID "CUSTOMER_ID",
    (
        COALESCE(CAST(A1.C_LAST_NAME AS VARCHAR(30)), '') ||(
            ', ' || COALESCE(CAST(A1.C_FIRST_NAME AS VARCHAR(20)), '')
        )
    ) "CUSTOMERNAME"
FROM
    (
        STORE_RETURNS A0
        INNER JOIN (
            (
                CUSTOMER A1
                INNER JOIN (
                    HOUSEHOLD_DEMOGRAPHICS A2
                    INNER JOIN INCOME_BAND A3 ON (A3.IB_INCOME_BAND_SK = A2.HD_INCOME_BAND_SK)
                ) ON (A2.HD_DEMO_SK = A1.C_CURRENT_HDEMO_SK)
            )
            INNER JOIN CUSTOMER_ADDRESS A4 ON (
                CAST(A1.C_CURRENT_ADDR_SK AS BIGINT) = A4.CA_ADDRESS_SK
            )
        ) ON (
            A0.SR_CDEMO_SK = CAST(A1.C_CURRENT_CDEMO_SK AS BIGINT)
        )
    )
WHERE
    (A0.SR_CDEMO_SK IS NOT NULL)
    AND (A1.C_CURRENT_CDEMO_SK IS NOT NULL)
    AND (45626 <= A3.IB_LOWER_BOUND)
    AND (A3.IB_UPPER_BOUND <= 95626)
    AND (A4.CA_CITY = 'White Oak')
ORDER BY
    1 ASC
limit
    100;