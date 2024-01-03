SELECT
    SUBSTR(A0.C0, 1, 20),
    CAST((A0.C5 / A0.C6) AS INTEGER),
    (A0.C3 / A0.C4),
    (A0.C1 / A0.C2)
FROM
    (
        SELECT
            A7.R_REASON_DESC C0,
            SUM(A4.WR_FEE) C1,
            COUNT(A4.WR_FEE) C2,
            SUM(A4.WR_REFUNDED_CASH) C3,
            COUNT(A4.WR_REFUNDED_CASH) C4,
            SUM(A1.WS_QUANTITY) C5,
            COUNT(A1.WS_QUANTITY) C6
        FROM
            (
                (
                    WEB_SALES A1
                    INNER JOIN DATE_DIM A2 ON (A1.WS_SOLD_DATE_SK = A2.D_DATE_SK)
                )
                INNER JOIN (
                    (
                        CUSTOMER_ADDRESS A3
                        INNER JOIN (
                            (
                                WEB_RETURNS A4
                                INNER JOIN CUSTOMER_DEMOGRAPHICS A5 ON (A5.CD_DEMO_SK = A4.WR_REFUNDED_CDEMO_SK)
                            )
                            INNER JOIN CUSTOMER_DEMOGRAPHICS A6 ON (A6.CD_DEMO_SK = A4.WR_RETURNING_CDEMO_SK)
                            AND (A5.CD_MARITAL_STATUS = A6.CD_MARITAL_STATUS)
                            AND (A5.CD_EDUCATION_STATUS = A6.CD_EDUCATION_STATUS)
                        ) ON (A3.CA_ADDRESS_SK = A4.WR_REFUNDED_ADDR_SK)
                    )
                    INNER JOIN REASON A7 ON (A7.R_REASON_SK = A4.WR_REASON_SK)
                ) ON (A1.WS_ITEM_SK = A4.WR_ITEM_SK)
                AND (A1.WS_ORDER_NUMBER = A4.WR_ORDER_NUMBER)
                AND (
                    (
                        (
                            (
                                (A5.CD_MARITAL_STATUS = 'D')
                                AND (A5.CD_EDUCATION_STATUS = 'Primary             ')
                            )
                            AND (
                                (A1.WS_SALES_PRICE >= 100.00)
                                AND (A1.WS_SALES_PRICE <= 150.00)
                            )
                        )
                        OR (
                            (
                                (A5.CD_MARITAL_STATUS = 'U')
                                AND (A5.CD_EDUCATION_STATUS = 'Unknown             ')
                            )
                            AND (
                                (A1.WS_SALES_PRICE >= 50.00)
                                AND (A1.WS_SALES_PRICE <= 100.00)
                            )
                        )
                    )
                    OR (
                        (
                            (A5.CD_MARITAL_STATUS = 'M')
                            AND (A5.CD_EDUCATION_STATUS = 'Advanced Degree     ')
                        )
                        AND (
                            (A1.WS_SALES_PRICE >= 150.00)
                            AND (A1.WS_SALES_PRICE <= 200.00)
                        )
                    )
                )
                AND (
                    (
                        (
                            (A3.CA_STATE IN ('SC', 'IN', 'VA'))
                            AND (
                                (A1.WS_NET_PROFIT >= 100)
                                AND (A1.WS_NET_PROFIT <= 200)
                            )
                        )
                        OR (
                            (A3.CA_STATE IN ('WA', 'KS', 'KY'))
                            AND (
                                (A1.WS_NET_PROFIT >= 150)
                                AND (A1.WS_NET_PROFIT <= 300)
                            )
                        )
                    )
                    OR (
                        (A3.CA_STATE IN ('SD', 'WI', 'NE'))
                        AND (
                            (A1.WS_NET_PROFIT >= 50)
                            AND (A1.WS_NET_PROFIT <= 250)
                        )
                    )
                )
            )
        WHERE
            (
                (
                    (A1.WS_NET_PROFIT >= 100)
                    AND (A1.WS_NET_PROFIT <= 200)
                )
                OR (
                    (
                        (A1.WS_NET_PROFIT >= 150)
                        AND (A1.WS_NET_PROFIT <= 300)
                    )
                    OR (
                        (A1.WS_NET_PROFIT >= 50)
                        AND (A1.WS_NET_PROFIT <= 250)
                    )
                )
            )
            AND (
                (
                    (A1.WS_SALES_PRICE >= 100.00)
                    AND (A1.WS_SALES_PRICE <= 150.00)
                )
                OR (
                    (
                        (A1.WS_SALES_PRICE >= 50.00)
                        AND (A1.WS_SALES_PRICE <= 100.00)
                    )
                    OR (
                        (A1.WS_SALES_PRICE >= 150.00)
                        AND (A1.WS_SALES_PRICE <= 200.00)
                    )
                )
            )
            AND (A1.WS_WEB_PAGE_SK IS NOT NULL)
            AND (A2.D_YEAR = 2001)
            AND (
                A3.CA_STATE IN (
                    'SC',
                    'IN',
                    'VA',
                    'WA',
                    'KS',
                    'KY',
                    'SD',
                    'WI',
                    'NE'
                )
            )
            AND (A3.CA_COUNTRY = 'United States')
            AND (A5.CD_MARITAL_STATUS IN ('D', 'U', 'M'))
            AND (
                A5.CD_EDUCATION_STATUS IN (
                    'Primary             ',
                    'Unknown             ',
                    'Advanced Degree     '
                )
            )
            AND (
                A6.CD_EDUCATION_STATUS IN (
                    'Primary             ',
                    'Unknown             ',
                    'Advanced Degree     '
                )
            )
            AND (A6.CD_MARITAL_STATUS IN ('D', 'U', 'M'))
        GROUP BY
            A7.R_REASON_DESC
    ) A0
ORDER BY
    1 ASC,
    2 ASC,
    3 ASC,
    4 ASC
limit
    100;