SELECT
    A0.C0 "CD_GENDER",
    A0.C1 "CD_MARITAL_STATUS",
    A0.C2 "CD_EDUCATION_STATUS",
    A0.C3 "CNT1",
    A0.C4 "CD_PURCHASE_ESTIMATE",
    A0.C3 "CNT2",
    A0.C5 "CD_CREDIT_RATING",
    A0.C3 "CNT3",
    A0.C6 "CD_DEP_COUNT",
    A0.C3 "CNT4",
    A0.C7 "CD_DEP_EMPLOYED_COUNT",
    A0.C3 "CNT5",
    A0.C8 "CD_DEP_COLLEGE_COUNT",
    A0.C3 "CNT6"
FROM
    (
        SELECT
            A1.C0 C0,
            A1.C1 C1,
            A1.C2 C2,
            COUNT(*) C3,
            A1.C3 C4,
            A1.C4 C5,
            A1.C5 C6,
            A1.C6 C7,
            A1.C7 C8
        FROM
            (
                SELECT
                    DISTINCT A2.C8 C0,
                    A2.C7 C1,
                    A2.C6 C2,
                    A2.C5 C3,
                    A2.C4 C4,
                    A2.C3 C5,
                    A2.C2 C6,
                    A2.C1 C7,
                    A2.C9 C8,
                    A2.C10 C9,
                    A2.C0 C10
                FROM
                    (
                        (
                            SELECT
                                A3.C0 C0,
                                A3.C1 C1,
                                A3.C2 C2,
                                A3.C3 C3,
                                A3.C4 C4,
                                A3.C5 C5,
                                A3.C6 C6,
                                A3.C7 C7,
                                A3.C8 C8,
                                A3.C9 C9,
                                A3.C10 C10,
                                A9.C0 C11
                            FROM
                                (
                                    (
                                        SELECT
                                            A6.C_CUSTOMER_SK C0,
                                            A8.CD_DEP_COLLEGE_COUNT C1,
                                            A8.CD_DEP_EMPLOYED_COUNT C2,
                                            A8.CD_DEP_COUNT C3,
                                            A8.CD_CREDIT_RATING C4,
                                            A8.CD_PURCHASE_ESTIMATE C5,
                                            A8.CD_EDUCATION_STATUS C6,
                                            A8.CD_MARITAL_STATUS C7,
                                            A8.CD_GENDER C8,
                                            A8.CD_DEMO_SK C9,
                                            A7.CA_ADDRESS_SK C10
                                        FROM
                                            (
                                                (
                                                    (
                                                        STORE_SALES A4
                                                        INNER JOIN DATE_DIM A5 ON (A4.SS_SOLD_DATE_SK = A5.D_DATE_SK)
                                                    )
                                                    INNER JOIN (
                                                        CUSTOMER A6
                                                        INNER JOIN CUSTOMER_ADDRESS A7 ON (
                                                            CAST(A6.C_CURRENT_ADDR_SK AS BIGINT) = A7.CA_ADDRESS_SK
                                                        )
                                                    ) ON (A6.C_CUSTOMER_SK = A4.SS_CUSTOMER_SK)
                                                )
                                                INNER JOIN CUSTOMER_DEMOGRAPHICS A8 ON (
                                                    A8.CD_DEMO_SK = CAST(A6.C_CURRENT_CDEMO_SK AS BIGINT)
                                                )
                                            )
                                        WHERE
                                            (A5.D_YEAR = 2002)
                                            AND (3 <= A5.D_MOY)
                                            AND (A5.D_MOY <= 6)
                                            AND (
                                                A7.CA_COUNTY IN (
                                                    'Clinton County',
                                                    'Platte County',
                                                    'Franklin County',
                                                    'Louisa County',
                                                    'Harmon County'
                                                )
                                            )
                                    ) A3
                                    LEFT OUTER JOIN (
                                        SELECT
                                            A10.WS_BILL_CUSTOMER_SK C0
                                        FROM
                                            (
                                                WEB_SALES A10
                                                INNER JOIN DATE_DIM A11 ON (A10.WS_SOLD_DATE_SK = A11.D_DATE_SK)
                                            )
                                        WHERE
                                            (A11.D_MOY <= 6)
                                            AND (3 <= A11.D_MOY)
                                            AND (A11.D_YEAR = 2002)
                                    ) A9 ON (A3.C0 = A9.C0)
                                )
                        ) A2
                        LEFT OUTER JOIN (
                            SELECT
                                A13.CS_SHIP_CUSTOMER_SK C0
                            FROM
                                (
                                    CATALOG_SALES A13
                                    INNER JOIN DATE_DIM A14 ON (A13.CS_SOLD_DATE_SK = A14.D_DATE_SK)
                                )
                            WHERE
                                (A14.D_MOY <= 6)
                                AND (3 <= A14.D_MOY)
                                AND (A14.D_YEAR = 2002)
                        ) A12 ON (A2.C0 = A12.C0)
                    )
                WHERE
                    (
                        (A2.C11 IS NOT NULL)
                        OR (A12.C0 IS NOT NULL)
                    )
            ) A1
        GROUP BY
            A1.C0,
            A1.C1,
            A1.C2,
            A1.C3,
            A1.C4,
            A1.C5,
            A1.C6,
            A1.C7
    ) A0
ORDER BY
    1 ASC,
    2 ASC,
    3 ASC,
    5 ASC,
    7 ASC,
    9 ASC,
    11 ASC,
    13 ASC
limit
    100;