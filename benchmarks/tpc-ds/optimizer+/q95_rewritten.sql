SELECT
    COUNT(A0.C0) "order count",
    SUM(A0.C1) "total shipping cost",
    SUM(A0.C2) "total net profit"
FROM
    (
        SELECT
            A1.C0 C0,
            SUM(A1.C1) C1,
            SUM(A1.C2) C2
        FROM
            (
                SELECT
                    DISTINCT A6.WS_ORDER_NUMBER C0,
                    A6.WS_EXT_SHIP_COST C1,
                    A6.WS_NET_PROFIT C2,
                    A8.WEB_SITE_SK C3,
                    A9.CA_ADDRESS_SK C4,
                    A7.D_DATE_SK C5,
                    A6.WS_ITEM_SK C6
                FROM
                    (
                        (
                            WEB_SALES A2
                            INNER JOIN (
                                WEB_RETURNS A3
                                INNER JOIN WEB_SALES A4 ON (A4.WS_ORDER_NUMBER = A3.WR_ORDER_NUMBER)
                            ) ON (A4.WS_ORDER_NUMBER = A2.WS_ORDER_NUMBER)
                            AND (A2.WS_WAREHOUSE_SK <> A4.WS_WAREHOUSE_SK)
                        )
                        INNER JOIN (
                            (
                                WEB_SALES A5
                                INNER JOIN (
                                    (
                                        (
                                            WEB_SALES A6
                                            INNER JOIN DATE_DIM A7 ON (A6.WS_SHIP_DATE_SK = A7.D_DATE_SK)
                                        )
                                        INNER JOIN WEB_SITE A8 ON (A6.WS_WEB_SITE_SK = A8.WEB_SITE_SK)
                                    )
                                    INNER JOIN CUSTOMER_ADDRESS A9 ON (A6.WS_SHIP_ADDR_SK = A9.CA_ADDRESS_SK)
                                ) ON (A5.WS_ORDER_NUMBER = A6.WS_ORDER_NUMBER)
                            )
                            INNER JOIN WEB_SALES A10 ON (A6.WS_ORDER_NUMBER = A10.WS_ORDER_NUMBER)
                            AND (A5.WS_WAREHOUSE_SK <> A10.WS_WAREHOUSE_SK)
                        ) ON (A2.WS_ORDER_NUMBER = A10.WS_ORDER_NUMBER)
                    )
                WHERE
                    (
                        A7.D_DATE <= DATE_ADD('day', 60, DATE('2002-05-01'))
                    )
                    AND (DATE('2002-05-01') <= A7.D_DATE)
                    AND (A8.WEB_COMPANY_NAME = 'pri')
                    AND (A9.CA_STATE = 'MA')
            ) A1
        GROUP BY
            A1.C0
    ) A0
ORDER BY
    1 ASC
limit
    100;