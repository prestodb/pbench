SELECT
    COUNT(DISTINCT A0.C0) "order count",
    SUM(A0.C1) "total shipping cost",
    SUM(A0.C2) "total net profit"
FROM
    (
        SELECT
            A1.C3 C0,
            A1.C2 C1,
            A1.C1 C2,
            A1.C4 C3,
            A1.C5 C4,
            A1.C6 C5,
            A1.C0 C6
        FROM
            (
                SELECT
                    DISTINCT A3.C0 C0,
                    A3.C1 C1,
                    A3.C2 C2,
                    A3.C3 C3,
                    A3.C4 C4,
                    A3.C5 C5,
                    A3.C6 C6
                FROM
                    (
                        WEB_RETURNS A2
                        RIGHT OUTER JOIN (
                            SELECT
                                A5.WS_ITEM_SK C0,
                                A5.WS_NET_PROFIT C1,
                                A5.WS_EXT_SHIP_COST C2,
                                A5.WS_ORDER_NUMBER C3,
                                A7.WEB_SITE_SK C4,
                                A8.CA_ADDRESS_SK C5,
                                A6.D_DATE_SK C6
                            FROM
                                (
                                    WEB_SALES A4
                                    INNER JOIN (
                                        (
                                            (
                                                WEB_SALES A5
                                                INNER JOIN DATE_DIM A6 ON (A5.WS_SHIP_DATE_SK = A6.D_DATE_SK)
                                            )
                                            INNER JOIN WEB_SITE A7 ON (A5.WS_WEB_SITE_SK = A7.WEB_SITE_SK)
                                        )
                                        INNER JOIN CUSTOMER_ADDRESS A8 ON (A5.WS_SHIP_ADDR_SK = A8.CA_ADDRESS_SK)
                                    ) ON (A5.WS_ORDER_NUMBER = A4.WS_ORDER_NUMBER)
                                    AND (A5.WS_WAREHOUSE_SK <> A4.WS_WAREHOUSE_SK)
                                )
                            WHERE
                                (
                                    A6.D_DATE <= DATE_ADD('day', 60, DATE('1999-04-01'))
                                )
                                AND (DATE('1999-04-01') <= A6.D_DATE)
                                AND (A7.WEB_COMPANY_NAME = 'pri')
                                AND (A8.CA_STATE = 'WI')
                        ) A3 ON (A3.C3 = A2.WR_ORDER_NUMBER)
                    )
            ) A1
    ) A0
ORDER BY
    1 ASC
limit
    100;