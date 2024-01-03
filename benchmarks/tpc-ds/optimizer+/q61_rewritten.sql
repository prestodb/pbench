SELECT
    A0.C0 "PROMOTIONS",
    A8.C0 "TOTAL",
    (
        (
            CAST(A0.C0 AS DECIMAL(15, 4)) / CAST(A8.C0 AS DECIMAL(15, 4))
        ) * 100
    )
FROM
    (
        SELECT
            SUM(A2.SS_EXT_SALES_PRICE) C0
        FROM
            (
                (
                    CUSTOMER A1
                    INNER JOIN (
                        (
                            (
                                (
                                    STORE_SALES A2
                                    INNER JOIN DATE_DIM A3 ON (A2.SS_SOLD_DATE_SK = A3.D_DATE_SK)
                                )
                                INNER JOIN ITEM A4 ON (A2.SS_ITEM_SK = A4.I_ITEM_SK)
                            )
                            INNER JOIN STORE A5 ON (A2.SS_STORE_SK = A5.S_STORE_SK)
                        )
                        INNER JOIN PROMOTION A6 ON (A2.SS_PROMO_SK = A6.P_PROMO_SK)
                    ) ON (A2.SS_CUSTOMER_SK = A1.C_CUSTOMER_SK)
                )
                INNER JOIN CUSTOMER_ADDRESS A7 ON (
                    A7.CA_ADDRESS_SK = CAST(A1.C_CURRENT_ADDR_SK AS BIGINT)
                )
                AND (A7.CA_GMT_OFFSET = A5.S_GMT_OFFSET)
            )
        WHERE
            (A3.D_YEAR = 2002)
            AND (A3.D_MOY = 11)
            AND (
                A4.I_CATEGORY = 'Sports                                            '
            )
            AND (A5.S_GMT_OFFSET = -006.00)
            AND (
                (
                    (A6.P_CHANNEL_DMAIL = 'Y')
                    OR (A6.P_CHANNEL_EMAIL = 'Y')
                )
                OR (A6.P_CHANNEL_TV = 'Y')
            )
            AND (A7.CA_GMT_OFFSET = -006.00)
    ) A0,
    (
        SELECT
            SUM(A10.SS_EXT_SALES_PRICE) C0
        FROM
            (
                (
                    CUSTOMER A9
                    INNER JOIN (
                        (
                            (
                                STORE_SALES A10
                                INNER JOIN DATE_DIM A11 ON (A10.SS_SOLD_DATE_SK = A11.D_DATE_SK)
                            )
                            INNER JOIN ITEM A12 ON (A10.SS_ITEM_SK = A12.I_ITEM_SK)
                        )
                        INNER JOIN STORE A13 ON (A10.SS_STORE_SK = A13.S_STORE_SK)
                    ) ON (A10.SS_CUSTOMER_SK = A9.C_CUSTOMER_SK)
                )
                INNER JOIN CUSTOMER_ADDRESS A14 ON (
                    A14.CA_ADDRESS_SK = CAST(A9.C_CURRENT_ADDR_SK AS BIGINT)
                )
                AND (A14.CA_GMT_OFFSET = A13.S_GMT_OFFSET)
            )
        WHERE
            (A11.D_YEAR = 2002)
            AND (A11.D_MOY = 11)
            AND (
                A12.I_CATEGORY = 'Sports                                            '
            )
            AND (A13.S_GMT_OFFSET = -006.00)
            AND (A14.CA_GMT_OFFSET = -006.00)
    ) A8
ORDER BY
    1 ASC,
    2 ASC
limit
    100;