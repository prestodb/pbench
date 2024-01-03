SELECT
    CAST((A0.C4 / A0.C5) AS INTEGER),
    (A0.C2 / A0.C3),
    (A0.C0 / A0.C1),
    A0.C0
FROM
    (
        SELECT
            SUM(A2.SS_EXT_WHOLESALE_COST) C0,
            COUNT(A2.SS_EXT_WHOLESALE_COST) C1,
            SUM(A2.SS_EXT_SALES_PRICE) C2,
            COUNT(A2.SS_EXT_SALES_PRICE) C3,
            SUM(A2.SS_QUANTITY) C4,
            COUNT(A2.SS_QUANTITY) C5
        FROM
            (
                CUSTOMER_DEMOGRAPHICS A1
                INNER JOIN (
                    (
                        (
                            STORE_SALES A2
                            INNER JOIN DATE_DIM A3 ON (A2.SS_SOLD_DATE_SK = A3.D_DATE_SK)
                        )
                        INNER JOIN HOUSEHOLD_DEMOGRAPHICS A4 ON (A2.SS_HDEMO_SK = A4.HD_DEMO_SK)
                    )
                    INNER JOIN CUSTOMER_ADDRESS A5 ON (A2.SS_ADDR_SK = A5.CA_ADDRESS_SK)
                    AND (
                        (
                            (
                                (A5.CA_STATE IN ('CO', 'MI', 'MN'))
                                AND (
                                    (A2.SS_NET_PROFIT >= 100)
                                    AND (A2.SS_NET_PROFIT <= 200)
                                )
                            )
                            OR (
                                (A5.CA_STATE IN ('NC', 'NY', 'TX'))
                                AND (
                                    (A2.SS_NET_PROFIT >= 150)
                                    AND (A2.SS_NET_PROFIT <= 300)
                                )
                            )
                        )
                        OR (
                            (A5.CA_STATE IN ('CA', 'NE', 'TN'))
                            AND (
                                (A2.SS_NET_PROFIT >= 50)
                                AND (A2.SS_NET_PROFIT <= 250)
                            )
                        )
                    )
                ) ON (A1.CD_DEMO_SK = A2.SS_CDEMO_SK)
                AND (
                    (
                        (
                            (
                                (
                                    (A1.CD_MARITAL_STATUS = 'U')
                                    AND (A1.CD_EDUCATION_STATUS = '4 yr Degree         ')
                                )
                                AND (
                                    (A2.SS_SALES_PRICE >= 100.00)
                                    AND (A2.SS_SALES_PRICE <= 150.00)
                                )
                            )
                            AND (A4.HD_DEP_COUNT = 3)
                        )
                        OR (
                            (
                                (
                                    (A1.CD_MARITAL_STATUS = 'S')
                                    AND (A1.CD_EDUCATION_STATUS = 'Unknown             ')
                                )
                                AND (
                                    (A2.SS_SALES_PRICE >= 50.00)
                                    AND (A2.SS_SALES_PRICE <= 100.00)
                                )
                            )
                            AND (A4.HD_DEP_COUNT = 1)
                        )
                    )
                    OR (
                        (
                            (
                                (A1.CD_MARITAL_STATUS = 'D')
                                AND (A1.CD_EDUCATION_STATUS = '2 yr Degree         ')
                            )
                            AND (
                                (A2.SS_SALES_PRICE >= 150.00)
                                AND (A2.SS_SALES_PRICE <= 200.00)
                            )
                        )
                        AND (A4.HD_DEP_COUNT = 1)
                    )
                )
            )
        WHERE
            (A1.CD_MARITAL_STATUS IN ('U', 'S', 'D'))
            AND (
                A1.CD_EDUCATION_STATUS IN (
                    '4 yr Degree         ',
                    'Unknown             ',
                    '2 yr Degree         '
                )
            )
            AND (
                (
                    (A2.SS_NET_PROFIT >= 100)
                    AND (A2.SS_NET_PROFIT <= 200)
                )
                OR (
                    (
                        (A2.SS_NET_PROFIT >= 150)
                        AND (A2.SS_NET_PROFIT <= 300)
                    )
                    OR (
                        (A2.SS_NET_PROFIT >= 50)
                        AND (A2.SS_NET_PROFIT <= 250)
                    )
                )
            )
            AND (
                (
                    (A2.SS_SALES_PRICE >= 100.00)
                    AND (A2.SS_SALES_PRICE <= 150.00)
                )
                OR (
                    (
                        (A2.SS_SALES_PRICE >= 50.00)
                        AND (A2.SS_SALES_PRICE <= 100.00)
                    )
                    OR (
                        (A2.SS_SALES_PRICE >= 150.00)
                        AND (A2.SS_SALES_PRICE <= 200.00)
                    )
                )
            )
            AND (A2.SS_STORE_SK IS NOT NULL)
            AND (A3.D_YEAR = 2001)
            AND (A4.HD_DEP_COUNT IN (3, 1))
            AND (
                A5.CA_STATE IN (
                    'CO',
                    'MI',
                    'MN',
                    'NC',
                    'NY',
                    'TX',
                    'CA',
                    'NE',
                    'TN'
                )
            )
            AND (A5.CA_COUNTRY = 'United States')
    ) A0;