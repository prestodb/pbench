SELECT
    A0.C0 "SEGMENT",
    A0.C1 "NUM_CUSTOMERS",
    (A0.C0 * 50) "SEGMENT_BASE"
FROM
    (
        SELECT
            CAST((A1.C0 / 50) AS INTEGER) C0,
            COUNT(*) C1
        FROM
            (
                SELECT
                    SUM(A2.C1) C0
                FROM
                    (
                        SELECT
                            DISTINCT A3.C4 C0,
                            A3.C2 C1,
                            A3.C6 C2,
                            A3.C8 C3,
                            A3.C7 C4,
                            A3.C1 C5,
                            A3.C0 C6,
                            A3.C3 C7
                        FROM
                            (
                                (
                                    SELECT
                                        A5.SS_TICKET_NUMBER C0,
                                        A5.SS_ITEM_SK C1,
                                        A5.SS_EXT_SALES_PRICE C2,
                                        A4.C_CURRENT_ADDR_SK C3,
                                        A4.C_CUSTOMER_SK C4,
                                        A9.D_MONTH_SEQ C5,
                                        A9.D_DATE_SK C6,
                                        A10.CA_ADDRESS_SK C7,
                                        A11.S_STORE_SK C8
                                    FROM
                                        (
                                            (
                                                CUSTOMER A4
                                                INNER JOIN (
                                                    (
                                                        STORE_SALES A5
                                                        INNER JOIN (
                                                            (
                                                                WEB_SALES A6
                                                                INNER JOIN ITEM A7 ON (A6.WS_ITEM_SK = A7.I_ITEM_SK)
                                                            )
                                                            INNER JOIN DATE_DIM A8 ON (A6.WS_SOLD_DATE_SK = A8.D_DATE_SK)
                                                        ) ON (A6.WS_BILL_CUSTOMER_SK = A5.SS_CUSTOMER_SK)
                                                    )
                                                    INNER JOIN DATE_DIM A9 ON (A5.SS_SOLD_DATE_SK = A9.D_DATE_SK)
                                                ) ON (A4.C_CUSTOMER_SK = A6.WS_BILL_CUSTOMER_SK)
                                            )
                                            INNER JOIN (
                                                CUSTOMER_ADDRESS A10
                                                INNER JOIN STORE A11 ON (A10.CA_COUNTY = A11.S_COUNTY)
                                                AND (A10.CA_STATE = A11.S_STATE)
                                            ) ON (
                                                CAST(A4.C_CURRENT_ADDR_SK AS BIGINT) = A10.CA_ADDRESS_SK
                                            )
                                        )
                                    WHERE
                                        (
                                            A7.I_CLASS = 'shirts                                            '
                                        )
                                        AND (
                                            A7.I_CATEGORY = 'Men                                               '
                                        )
                                        AND (A8.D_YEAR = 1998)
                                        AND (A8.D_MOY = 4)
                                )
                                UNION
                                ALL (
                                    SELECT
                                        A13.SS_TICKET_NUMBER C0,
                                        A13.SS_ITEM_SK C1,
                                        A13.SS_EXT_SALES_PRICE C2,
                                        A12.C_CURRENT_ADDR_SK C3,
                                        A12.C_CUSTOMER_SK C4,
                                        A17.D_MONTH_SEQ C5,
                                        A17.D_DATE_SK C6,
                                        A18.CA_ADDRESS_SK C7,
                                        A19.S_STORE_SK C8
                                    FROM
                                        (
                                            (
                                                CUSTOMER A12
                                                INNER JOIN (
                                                    (
                                                        STORE_SALES A13
                                                        INNER JOIN (
                                                            (
                                                                CATALOG_SALES A14
                                                                INNER JOIN ITEM A15 ON (A14.CS_ITEM_SK = A15.I_ITEM_SK)
                                                            )
                                                            INNER JOIN DATE_DIM A16 ON (A14.CS_SOLD_DATE_SK = A16.D_DATE_SK)
                                                        ) ON (A14.CS_BILL_CUSTOMER_SK = A13.SS_CUSTOMER_SK)
                                                    )
                                                    INNER JOIN DATE_DIM A17 ON (A13.SS_SOLD_DATE_SK = A17.D_DATE_SK)
                                                ) ON (A12.C_CUSTOMER_SK = A14.CS_BILL_CUSTOMER_SK)
                                            )
                                            INNER JOIN (
                                                CUSTOMER_ADDRESS A18
                                                INNER JOIN STORE A19 ON (A18.CA_COUNTY = A19.S_COUNTY)
                                                AND (A18.CA_STATE = A19.S_STATE)
                                            ) ON (
                                                CAST(A12.C_CURRENT_ADDR_SK AS BIGINT) = A18.CA_ADDRESS_SK
                                            )
                                        )
                                    WHERE
                                        (
                                            A15.I_CLASS = 'shirts                                            '
                                        )
                                        AND (
                                            A15.I_CATEGORY = 'Men                                               '
                                        )
                                        AND (A16.D_YEAR = 1998)
                                        AND (A16.D_MOY = 4)
                                )
                            ) A3
                        WHERE
                            (
                                (
                                    SELECT
                                        DISTINCT (A20.D_MONTH_SEQ + 1)
                                    FROM
                                        DATE_DIM A20
                                    WHERE
                                        (A20.D_YEAR = 1998)
                                        AND (A20.D_MOY = 4)
                                ) <= A3.C5
                            )
                            AND (
                                A3.C5 <= (
                                    SELECT
                                        DISTINCT (A21.D_MONTH_SEQ + 3)
                                    FROM
                                        DATE_DIM A21
                                    WHERE
                                        (A21.D_YEAR = 1998)
                                        AND (A21.D_MOY = 4)
                                )
                            )
                    ) A2
                GROUP BY
                    A2.C0
            ) A1
        GROUP BY
            CAST((A1.C0 / 50) AS INTEGER)
    ) A0
ORDER BY
    1 ASC
limit
    100;