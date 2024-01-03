SELECT
    A0.C2 "CA_ZIP",
    A0.C1 "CA_CITY",
    SUM(A0.C3)
FROM
    (
        (
            SELECT
                A5.I_ITEM_ID C0,
                A1.CA_CITY C1,
                A1.CA_ZIP C2,
                A3.WS_SALES_PRICE C3
            FROM
                (
                    CUSTOMER_ADDRESS A1
                    INNER JOIN (
                        (
                            CUSTOMER A2
                            INNER JOIN (
                                WEB_SALES A3
                                INNER JOIN DATE_DIM A4 ON (A3.WS_SOLD_DATE_SK = A4.D_DATE_SK)
                            ) ON (A3.WS_BILL_CUSTOMER_SK = A2.C_CUSTOMER_SK)
                        )
                        INNER JOIN ITEM A5 ON (A3.WS_ITEM_SK = A5.I_ITEM_SK)
                    ) ON (
                        CAST(A2.C_CURRENT_ADDR_SK AS BIGINT) = A1.CA_ADDRESS_SK
                    )
                )
            WHERE
                (A4.D_YEAR = 2000)
                AND (A4.D_QOY = 2)
        ) A0
        LEFT OUTER JOIN ITEM A6 ON (A0.C0 = A6.I_ITEM_ID)
        AND (
            A6.I_ITEM_SK IN (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)
        )
    )
WHERE
    (
        (
            SUBSTR(A0.C2, 1, 5) IN (
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
        OR (A6.I_ITEM_ID IS NOT NULL)
    )
GROUP BY
    A0.C2,
    A0.C1
ORDER BY
    1 ASC,
    2 ASC
limit
    100;