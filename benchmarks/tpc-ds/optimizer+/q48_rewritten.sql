SELECT
    SUM(A0.SS_QUANTITY)
FROM
    (
        (
            (
                STORE_SALES A0
                INNER JOIN DATE_DIM A1 ON (A0.SS_SOLD_DATE_SK = A1.D_DATE_SK)
            )
            INNER JOIN CUSTOMER_ADDRESS A2 ON (A0.SS_ADDR_SK = A2.CA_ADDRESS_SK)
            AND (
                (
                    (
                        (A2.CA_STATE IN ('IL', 'KY', 'OR'))
                        AND (
                            (A0.SS_NET_PROFIT >= 0)
                            AND (A0.SS_NET_PROFIT <= 2000)
                        )
                    )
                    OR (
                        (A2.CA_STATE IN ('VA', 'FL', 'AL'))
                        AND (
                            (A0.SS_NET_PROFIT >= 150)
                            AND (A0.SS_NET_PROFIT <= 3000)
                        )
                    )
                )
                OR (
                    (A2.CA_STATE IN ('OK', 'IA', 'TX'))
                    AND (
                        (A0.SS_NET_PROFIT >= 50)
                        AND (A0.SS_NET_PROFIT <= 25000)
                    )
                )
            )
        )
        INNER JOIN CUSTOMER_DEMOGRAPHICS A3 ON (A3.CD_DEMO_SK = A0.SS_CDEMO_SK)
        AND (
            (
                (
                    (
                        (A3.CD_MARITAL_STATUS = 'W')
                        AND (A3.CD_EDUCATION_STATUS = '2 yr Degree         ')
                    )
                    AND (
                        (A0.SS_SALES_PRICE >= 100.00)
                        AND (A0.SS_SALES_PRICE <= 150.00)
                    )
                )
                OR (
                    (
                        (A3.CD_MARITAL_STATUS = 'S')
                        AND (A3.CD_EDUCATION_STATUS = 'Advanced Degree     ')
                    )
                    AND (
                        (A0.SS_SALES_PRICE >= 50.00)
                        AND (A0.SS_SALES_PRICE <= 100.00)
                    )
                )
            )
            OR (
                (
                    (A3.CD_MARITAL_STATUS = 'D')
                    AND (A3.CD_EDUCATION_STATUS = 'Primary             ')
                )
                AND (
                    (A0.SS_SALES_PRICE >= 150.00)
                    AND (A0.SS_SALES_PRICE <= 200.00)
                )
            )
        )
    )
WHERE
    (
        (
            (A0.SS_NET_PROFIT >= 0)
            AND (A0.SS_NET_PROFIT <= 2000)
        )
        OR (
            (
                (A0.SS_NET_PROFIT >= 150)
                AND (A0.SS_NET_PROFIT <= 3000)
            )
            OR (
                (A0.SS_NET_PROFIT >= 50)
                AND (A0.SS_NET_PROFIT <= 25000)
            )
        )
    )
    AND (
        (
            (A0.SS_SALES_PRICE >= 100.00)
            AND (A0.SS_SALES_PRICE <= 150.00)
        )
        OR (
            (
                (A0.SS_SALES_PRICE >= 50.00)
                AND (A0.SS_SALES_PRICE <= 100.00)
            )
            OR (
                (A0.SS_SALES_PRICE >= 150.00)
                AND (A0.SS_SALES_PRICE <= 200.00)
            )
        )
    )
    AND (A0.SS_STORE_SK IS NOT NULL)
    AND (A1.D_YEAR = 2001)
    AND (
        A2.CA_STATE IN (
            'IL',
            'KY',
            'OR',
            'VA',
            'FL',
            'AL',
            'OK',
            'IA',
            'TX'
        )
    )
    AND (A2.CA_COUNTRY = 'United States')
    AND (A3.CD_MARITAL_STATUS IN ('W', 'S', 'D'))
    AND (
        A3.CD_EDUCATION_STATUS IN (
            '2 yr Degree         ',
            'Advanced Degree     ',
            'Primary             '
        )
    );