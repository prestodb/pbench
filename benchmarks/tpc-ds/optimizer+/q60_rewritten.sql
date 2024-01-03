SELECT
    A0.C0 "I_ITEM_ID",
    SUM(A0.C1) "TOTAL_SALES"
FROM
    (
        (
            SELECT
                A1.C0 C0,
                SUM(A1.C1) C1
            FROM
                (
                    SELECT
                        DISTINCT A2.I_ITEM_ID C0,
                        A4.SS_EXT_SALES_PRICE C1,
                        A2.I_ITEM_SK C2,
                        A6.CA_ADDRESS_SK C3,
                        A5.D_DATE_SK C4,
                        A4.SS_ITEM_SK C5,
                        A4.SS_TICKET_NUMBER C6
                    FROM
                        (
                            (
                                ITEM A2
                                INNER JOIN ITEM A3 ON (A2.I_ITEM_ID = A3.I_ITEM_ID)
                            )
                            INNER JOIN (
                                (
                                    STORE_SALES A4
                                    INNER JOIN DATE_DIM A5 ON (A4.SS_SOLD_DATE_SK = A5.D_DATE_SK)
                                )
                                INNER JOIN CUSTOMER_ADDRESS A6 ON (A4.SS_ADDR_SK = A6.CA_ADDRESS_SK)
                            ) ON (A4.SS_ITEM_SK = A2.I_ITEM_SK)
                        )
                    WHERE
                        (
                            A3.I_CATEGORY = 'Shoes                                             '
                        )
                        AND (A5.D_YEAR = 2001)
                        AND (A5.D_MOY = 10)
                        AND (A6.CA_GMT_OFFSET = -006.00)
                ) A1
            GROUP BY
                A1.C0
        )
        UNION
        ALL (
            SELECT
                A7.C0 C0,
                SUM(A7.C1) C1
            FROM
                (
                    SELECT
                        DISTINCT A11.I_ITEM_ID C0,
                        A8.CS_EXT_SALES_PRICE C1,
                        A11.I_ITEM_SK C2,
                        A10.CA_ADDRESS_SK C3,
                        A9.D_DATE_SK C4,
                        A8.CS_ITEM_SK C5,
                        A8.CS_ORDER_NUMBER C6
                    FROM
                        (
                            (
                                (
                                    (
                                        CATALOG_SALES A8
                                        INNER JOIN DATE_DIM A9 ON (A8.CS_SOLD_DATE_SK = A9.D_DATE_SK)
                                    )
                                    INNER JOIN CUSTOMER_ADDRESS A10 ON (A8.CS_BILL_ADDR_SK = A10.CA_ADDRESS_SK)
                                )
                                INNER JOIN ITEM A11 ON (A8.CS_ITEM_SK = A11.I_ITEM_SK)
                            )
                            INNER JOIN ITEM A12 ON (A11.I_ITEM_ID = A12.I_ITEM_ID)
                        )
                    WHERE
                        (A9.D_YEAR = 2001)
                        AND (A9.D_MOY = 10)
                        AND (A10.CA_GMT_OFFSET = -006.00)
                        AND (
                            A12.I_CATEGORY = 'Shoes                                             '
                        )
                ) A7
            GROUP BY
                A7.C0
        )
        UNION
        ALL (
            SELECT
                A13.C0 C0,
                SUM(A13.C1) C1
            FROM
                (
                    SELECT
                        DISTINCT A17.I_ITEM_ID C0,
                        A15.WS_EXT_SALES_PRICE C1,
                        A17.I_ITEM_SK C2,
                        A14.CA_ADDRESS_SK C3,
                        A16.D_DATE_SK C4,
                        A15.WS_ITEM_SK C5,
                        A15.WS_ORDER_NUMBER C6
                    FROM
                        (
                            (
                                (
                                    CUSTOMER_ADDRESS A14
                                    INNER JOIN (
                                        WEB_SALES A15
                                        INNER JOIN DATE_DIM A16 ON (A15.WS_SOLD_DATE_SK = A16.D_DATE_SK)
                                    ) ON (A15.WS_BILL_ADDR_SK = A14.CA_ADDRESS_SK)
                                )
                                INNER JOIN ITEM A17 ON (A15.WS_ITEM_SK = A17.I_ITEM_SK)
                            )
                            INNER JOIN ITEM A18 ON (A17.I_ITEM_ID = A18.I_ITEM_ID)
                        )
                    WHERE
                        (A14.CA_GMT_OFFSET = -006.00)
                        AND (A16.D_YEAR = 2001)
                        AND (A16.D_MOY = 10)
                        AND (
                            A18.I_CATEGORY = 'Shoes                                             '
                        )
                ) A13
            GROUP BY
                A13.C0
        )
    ) A0
GROUP BY
    A0.C0
ORDER BY
    1 ASC
limit
    100;