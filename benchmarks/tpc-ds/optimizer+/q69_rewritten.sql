SELECT
    A0.C0 "CD_GENDER",
    A0.C1 "CD_MARITAL_STATUS",
    A0.C2 "CD_EDUCATION_STATUS",
    A0.C3 "CNT1",
    A0.C4 "CD_PURCHASE_ESTIMATE",
    A0.C3 "CNT2",
    A0.C5 "CD_CREDIT_RATING",
    A0.C3 "CNT3"
FROM
    (
        SELECT
            A1.C0 C0,
            A1.C1 C1,
            A1.C2 C2,
            COUNT(*) C3,
            A1.C3 C4,
            A1.C4 C5
        FROM
            (
                SELECT
                    A2.C5 C0,
                    A2.C4 C1,
                    A2.C3 C2,
                    A2.C2 C3,
                    A2.C1 C4,
                    A2.C6 C5,
                    A2.C7 C6,
                    A2.C0 C7
                FROM
                    (
                        SELECT
                            DISTINCT A6.C0 C0,
                            A6.C1 C1,
                            A6.C2 C2,
                            A6.C3 C3,
                            A6.C4 C4,
                            A6.C5 C5,
                            A6.C6 C6,
                            A6.C7 C7
                        FROM
                            (
                                (
                                    SELECT
                                        A4.WS_BILL_CUSTOMER_SK C0
                                    FROM
                                        (
                                            WEB_SALES A4
                                            INNER JOIN DATE_DIM A5 ON (A4.WS_SOLD_DATE_SK = A5.D_DATE_SK)
                                        )
                                    WHERE
                                        (A5.D_MOY <= 4)
                                        AND (2 <= A5.D_MOY)
                                        AND (A5.D_YEAR = 2002)
                                ) A3
                                RIGHT OUTER JOIN (
                                    SELECT
                                        DISTINCT A10.C0 C0,
                                        A10.C1 C1,
                                        A10.C2 C2,
                                        A10.C3 C3,
                                        A10.C4 C4,
                                        A10.C5 C5,
                                        A10.C6 C6,
                                        A10.C7 C7
                                    FROM
                                        (
                                            (
                                                SELECT
                                                    A8.CS_SHIP_CUSTOMER_SK C0
                                                FROM
                                                    (
                                                        CATALOG_SALES A8
                                                        INNER JOIN DATE_DIM A9 ON (A8.CS_SOLD_DATE_SK = A9.D_DATE_SK)
                                                    )
                                                WHERE
                                                    (A9.D_MOY <= 4)
                                                    AND (2 <= A9.D_MOY)
                                                    AND (A9.D_YEAR = 2002)
                                            ) A7
                                            RIGHT OUTER JOIN (
                                                SELECT
                                                    A13.C_CUSTOMER_SK C0,
                                                    A15.CD_CREDIT_RATING C1,
                                                    A15.CD_PURCHASE_ESTIMATE C2,
                                                    A15.CD_EDUCATION_STATUS C3,
                                                    A15.CD_MARITAL_STATUS C4,
                                                    A15.CD_GENDER C5,
                                                    A15.CD_DEMO_SK C6,
                                                    A14.CA_ADDRESS_SK C7
                                                FROM
                                                    (
                                                        (
                                                            (
                                                                STORE_SALES A11
                                                                INNER JOIN DATE_DIM A12 ON (A11.SS_SOLD_DATE_SK = A12.D_DATE_SK)
                                                            )
                                                            INNER JOIN (
                                                                CUSTOMER A13
                                                                INNER JOIN CUSTOMER_ADDRESS A14 ON (
                                                                    CAST(A13.C_CURRENT_ADDR_SK AS BIGINT) = A14.CA_ADDRESS_SK
                                                                )
                                                            ) ON (A13.C_CUSTOMER_SK = A11.SS_CUSTOMER_SK)
                                                        )
                                                        INNER JOIN CUSTOMER_DEMOGRAPHICS A15 ON (
                                                            A15.CD_DEMO_SK = CAST(A13.C_CURRENT_CDEMO_SK AS BIGINT)
                                                        )
                                                    )
                                                WHERE
                                                    (A12.D_YEAR = 2002)
                                                    AND (2 <= A12.D_MOY)
                                                    AND (A12.D_MOY <= 4)
                                                    AND (A14.CA_STATE IN ('IN', 'VA', 'MS'))
                                            ) A10 ON (A10.C0 = A7.C0)
                                        )
                                ) A6 ON (A6.C0 = A3.C0)
                            )
                    ) A2
            ) A1
        GROUP BY
            A1.C0,
            A1.C1,
            A1.C2,
            A1.C3,
            A1.C4
    ) A0
ORDER BY
    1 ASC,
    2 ASC,
    3 ASC,
    5 ASC,
    7 ASC
limit
    100;