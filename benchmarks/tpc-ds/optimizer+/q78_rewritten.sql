SELECT
    A0.C3 "SS_CUSTOMER_SK",
    ROUND(
        (
            CAST(A0.C2 AS DECIMAL(20, 0)) / (COALESCE(A0.C8, 0) + COALESCE(A11.C3, 0))
        ),
        2
    ) "RATIO",
    A0.C2 "STORE_QTY",
    A0.C1 "STORE_WHOLESALE_COST",
    A0.C0 "STORE_SALES_PRICE",
    (COALESCE(A0.C8, 0) + COALESCE(A11.C3, 0)) "OTHER_CHAN_QTY",
    (
        COALESCE(A0.C7, 00000000000000000000000000000.00) + COALESCE(A11.C4, 00000000000000000000000000000.00)
    ) "OTHER_CHAN_WHOLESALE_COST",
    (
        COALESCE(A0.C6, 00000000000000000000000000000.00) + COALESCE(A11.C5, 00000000000000000000000000000.00)
    ) "OTHER_CHAN_SALES_PRICE"
FROM
    (
        (
            SELECT
                A1.C5 C0,
                A1.C4 C1,
                A1.C3 C2,
                A1.C2 C3,
                A1.C1 C4,
                A1.C0 C5,
                A6.C5 C6,
                A6.C4 C7,
                A6.C3 C8
            FROM
                (
                    (
                        SELECT
                            A3.C0 C0,
                            A3.C5 C1,
                            A3.C4 C2,
                            SUM(A3.C3) C3,
                            SUM(A3.C2) C4,
                            SUM(A3.C1) C5
                        FROM
                            (
                                STORE_RETURNS A2
                                RIGHT OUTER JOIN (
                                    SELECT
                                        A5.D_YEAR C0,
                                        A4.SS_SALES_PRICE C1,
                                        A4.SS_WHOLESALE_COST C2,
                                        A4.SS_QUANTITY C3,
                                        A4.SS_CUSTOMER_SK C4,
                                        A4.SS_ITEM_SK C5,
                                        A4.SS_TICKET_NUMBER C6
                                    FROM
                                        (
                                            STORE_SALES A4
                                            INNER JOIN DATE_DIM A5 ON (A4.SS_SOLD_DATE_SK = A5.D_DATE_SK)
                                        )
                                    WHERE
                                        (A5.D_YEAR = 2001)
                                ) A3 ON (A2.SR_TICKET_NUMBER = A3.C6)
                                AND (A3.C5 = A2.SR_ITEM_SK)
                            )
                        GROUP BY
                            A3.C5,
                            A3.C4,
                            A3.C0
                    ) A1
                    LEFT OUTER JOIN (
                        SELECT
                            A8.C0 C0,
                            A8.C5 C1,
                            A8.C4 C2,
                            SUM(A8.C3) C3,
                            SUM(A8.C2) C4,
                            SUM(A8.C1) C5
                        FROM
                            (
                                WEB_RETURNS A7
                                RIGHT OUTER JOIN (
                                    SELECT
                                        A10.D_YEAR C0,
                                        A9.WS_SALES_PRICE C1,
                                        A9.WS_WHOLESALE_COST C2,
                                        A9.WS_QUANTITY C3,
                                        A9.WS_BILL_CUSTOMER_SK C4,
                                        A9.WS_ITEM_SK C5,
                                        A9.WS_ORDER_NUMBER C6
                                    FROM
                                        (
                                            WEB_SALES A9
                                            INNER JOIN DATE_DIM A10 ON (A9.WS_SOLD_DATE_SK = A10.D_DATE_SK)
                                        )
                                    WHERE
                                        (A10.D_YEAR = 2001)
                                ) A8 ON (A7.WR_ORDER_NUMBER = A8.C6)
                                AND (A8.C5 = A7.WR_ITEM_SK)
                            )
                        GROUP BY
                            A8.C5,
                            A8.C4,
                            A8.C0
                    ) A6 ON (A6.C0 = A1.C0)
                    AND (A6.C1 = A1.C1)
                    AND (A6.C2 = A1.C2)
                )
        ) A0
        LEFT OUTER JOIN (
            SELECT
                A13.C0 C0,
                A13.C5 C1,
                A13.C4 C2,
                SUM(A13.C3) C3,
                SUM(A13.C2) C4,
                SUM(A13.C1) C5
            FROM
                (
                    CATALOG_RETURNS A12
                    RIGHT OUTER JOIN (
                        SELECT
                            A15.D_YEAR C0,
                            A14.CS_SALES_PRICE C1,
                            A14.CS_WHOLESALE_COST C2,
                            A14.CS_QUANTITY C3,
                            A14.CS_BILL_CUSTOMER_SK C4,
                            A14.CS_ITEM_SK C5,
                            A14.CS_ORDER_NUMBER C6
                        FROM
                            (
                                CATALOG_SALES A14
                                INNER JOIN DATE_DIM A15 ON (A14.CS_SOLD_DATE_SK = A15.D_DATE_SK)
                            )
                        WHERE
                            (A15.D_YEAR = 2001)
                    ) A13 ON (A12.CR_ORDER_NUMBER = A13.C6)
                    AND (A13.C5 = A12.CR_ITEM_SK)
                )
            GROUP BY
                A13.C5,
                A13.C4,
                A13.C0
        ) A11 ON (A11.C0 = A0.C5)
        AND (A11.C1 = A0.C4)
        AND (A11.C2 = A0.C3)
    )
WHERE
    (
        (COALESCE(A0.C8, 0) > 0)
        OR (COALESCE(A11.C3, 0) > 0)
    )
ORDER BY
    1 ASC,
    3 DESC,
    4 DESC,
    5 DESC,
    6 ASC,
    7 ASC,
    8 ASC,
    2 ASC
limit
    100;