SELECT
    A3.CA_ZIP "CA_ZIP",
    SUM(A1.CS_SALES_PRICE)
FROM
    (
        (
            CUSTOMER A0
            INNER JOIN (
                CATALOG_SALES A1
                INNER JOIN DATE_DIM A2 ON (A1.CS_SOLD_DATE_SK = A2.D_DATE_SK)
            ) ON (A1.CS_BILL_CUSTOMER_SK = A0.C_CUSTOMER_SK)
        )
        INNER JOIN CUSTOMER_ADDRESS A3 ON (
            CAST(A0.C_CURRENT_ADDR_SK AS BIGINT) = A3.CA_ADDRESS_SK
        )
        AND (
            (
                (
                    SUBSTR(A3.CA_ZIP, 1, 5) IN (
                        '85669',
                        '86197',
                        '88274',
                        '83405',
                        '86475',
                        '85392',
                        '85460',
                        '80348',
                        '81792'
                    )
                )
                OR (A3.CA_STATE IN ('CA', 'WA', 'GA'))
            )
            OR (A1.CS_SALES_PRICE > 500)
        )
    )
WHERE
    (A2.D_QOY = 2)
    AND (A2.D_YEAR = 2002)
GROUP BY
    A3.CA_ZIP
ORDER BY
    1 ASC
limit
    100;