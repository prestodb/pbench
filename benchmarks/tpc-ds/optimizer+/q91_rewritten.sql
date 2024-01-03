SELECT
    A6.CC_CALL_CENTER_ID "CALL_CENTER",
    A6.CC_NAME "CALL_CENTER_NAME",
    A6.CC_MANAGER "MANAGER",
    SUM(A3.CR_NET_LOSS) "RETURNS_LOSS"
FROM
    (
        (
            CUSTOMER_ADDRESS A0
            INNER JOIN (
                (
                    (
                        CUSTOMER A1
                        INNER JOIN HOUSEHOLD_DEMOGRAPHICS A2 ON (A2.HD_DEMO_SK = A1.C_CURRENT_HDEMO_SK)
                    )
                    INNER JOIN (
                        CATALOG_RETURNS A3
                        INNER JOIN DATE_DIM A4 ON (A3.CR_RETURNED_DATE_SK = A4.D_DATE_SK)
                    ) ON (A3.CR_RETURNING_CUSTOMER_SK = A1.C_CUSTOMER_SK)
                )
                INNER JOIN CUSTOMER_DEMOGRAPHICS A5 ON (
                    A5.CD_DEMO_SK = CAST(A1.C_CURRENT_CDEMO_SK AS BIGINT)
                )
            ) ON (
                A0.CA_ADDRESS_SK = CAST(A1.C_CURRENT_ADDR_SK AS BIGINT)
            )
        )
        INNER JOIN CALL_CENTER A6 ON (A3.CR_CALL_CENTER_SK = A6.CC_CALL_CENTER_SK)
    )
WHERE
    (A0.CA_GMT_OFFSET = -006.00)
    AND (A2.HD_BUY_POTENTIAL LIKE 'Unknown%')
    AND (A4.D_YEAR = 2002)
    AND (A4.D_MOY = 11)
    AND (
        (
            (A5.CD_MARITAL_STATUS = 'M')
            AND (A5.CD_EDUCATION_STATUS = 'Unknown             ')
        )
        OR (
            (A5.CD_MARITAL_STATUS = 'W')
            AND (A5.CD_EDUCATION_STATUS = 'Advanced Degree     ')
        )
    )
GROUP BY
    A6.CC_CALL_CENTER_ID,
    A6.CC_NAME,
    A6.CC_MANAGER,
    A5.CD_MARITAL_STATUS,
    A5.CD_EDUCATION_STATUS
ORDER BY
    4 DESC;