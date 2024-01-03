WITH A2 AS (
    SELECT
        MAX(A4.C1) C0
    FROM
        (
            CUSTOMER A3
            INNER JOIN (
                SELECT
                    A5.SS_CUSTOMER_SK C0,
                    SUM((A5.SS_QUANTITY * A5.SS_SALES_PRICE)) C1
                FROM
                    (
                        STORE_SALES A5
                        INNER JOIN DATE_DIM A6 ON (A5.SS_SOLD_DATE_SK = A6.D_DATE_SK)
                    )
                WHERE
                    (A6.D_YEAR IN (2000, 2001, 2002, 2003))
                GROUP BY
                    A5.SS_CUSTOMER_SK
            ) A4 ON (A4.C0 = A3.C_CUSTOMER_SK)
        )
),
A8 AS (
    SELECT
        SUM(A11.C0) C0,
        A9.C_CUSTOMER_SK C1
    FROM
        (
            CUSTOMER A9
            INNER JOIN (
                SELECT
                    SUM((A10.SS_QUANTITY * A10.SS_SALES_PRICE)) C0,
                    A10.SS_CUSTOMER_SK C1
                FROM
                    STORE_SALES A10
                GROUP BY
                    A10.SS_CUSTOMER_SK A11
            ) ON (A11.C1 = A9.C_CUSTOMER_SK)
        )
    GROUP BY
        A9.C_CUSTOMER_SK
),
A15 AS (
    SELECT
        COUNT(*) C0,
        A16.SS_ITEM_SK C1
    FROM
        (
            STORE_SALES A16
            INNER JOIN DATE_DIM A17 ON (A16.SS_SOLD_DATE_SK = A17.D_DATE_SK)
        )
    WHERE
        (A17.D_YEAR IN (2000, 2001, 2002, 2003))
    GROUP BY
        A16.SS_ITEM_SK,
        A17.D_DATE
)
SELECT
    SUM(A0.C0)
FROM
    (
        (
            SELECT
                A1.C0 C0
            FROM
                (
                    SELECT
                        DISTINCT (A13.WS_QUANTITY * A13.WS_LIST_PRICE) C0,
                        "A12".C1 C1,
                        A14.D_DATE_SK C2,
                        A13.WS_ITEM_SK C3,
                        A13.WS_ORDER_NUMBER C4
                    FROM
                        (
                            A2 "A7",
                            (
                                A8 "A12"
                                INNER JOIN (
                                    WEB_SALES A13
                                    INNER JOIN DATE_DIM A14 ON (A13.WS_SOLD_DATE_SK = A14.D_DATE_SK)
                                ) ON (A13.WS_BILL_CUSTOMER_SK = "A12".C1)
                            )
                            INNER JOIN A15 "A18" ON (A13.WS_ITEM_SK = "A18".C1)
                        )
                    WHERE
                        (A14.D_YEAR = 2000)
                        AND (A14.D_MOY = 3)
                        AND (
                            (000000000000.9500000000000000000 * "A7".C0) < "A12".C0
                        )
                        AND (4 < "A18".C0)
                ) A1
        )
        UNION
        ALL (
            SELECT
                A19.C0 C0
            FROM
                (
                    SELECT
                        DISTINCT (A22.CS_QUANTITY * A22.CS_LIST_PRICE) C0,
                        "A21".C1 C1,
                        A23.D_DATE_SK C2,
                        A22.CS_ITEM_SK C3,
                        A22.CS_ORDER_NUMBER C4
                    FROM
                        (
                            (
                                A2 "A20",
                                A8 "A21"
                                INNER JOIN (
                                    CATALOG_SALES A22
                                    INNER JOIN DATE_DIM A23 ON (A22.CS_SOLD_DATE_SK = A23.D_DATE_SK)
                                ) ON (A22.CS_BILL_CUSTOMER_SK = "A21".C1)
                            )
                            INNER JOIN A15 "A24" ON (A22.CS_ITEM_SK = "A24".C1)
                        )
                    WHERE
                        (
                            (000000000000.9500000000000000000 * "A20".C0) < "A21".C0
                        )
                        AND (A23.D_YEAR = 2000)
                        AND (A23.D_MOY = 3)
                        AND (4 < "A24".C0)
                ) A19
        )
    ) A0
limit
    100;