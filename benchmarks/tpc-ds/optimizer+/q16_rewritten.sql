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
                        CATALOG_RETURNS A2
                        RIGHT OUTER JOIN (
                            SELECT
                                A5.CS_ITEM_SK C0,
                                A5.CS_NET_PROFIT C1,
                                A5.CS_EXT_SHIP_COST C2,
                                A5.CS_ORDER_NUMBER C3,
                                A8.CC_CALL_CENTER_SK C4,
                                A7.CA_ADDRESS_SK C5,
                                A6.D_DATE_SK C6
                            FROM
                                (
                                    CATALOG_SALES A4
                                    INNER JOIN (
                                        (
                                            (
                                                CATALOG_SALES A5
                                                INNER JOIN DATE_DIM A6 ON (A5.CS_SHIP_DATE_SK = A6.D_DATE_SK)
                                            )
                                            INNER JOIN CUSTOMER_ADDRESS A7 ON (A5.CS_SHIP_ADDR_SK = A7.CA_ADDRESS_SK)
                                        )
                                        INNER JOIN CALL_CENTER A8 ON (A5.CS_CALL_CENTER_SK = A8.CC_CALL_CENTER_SK)
                                    ) ON (A5.CS_ORDER_NUMBER = A4.CS_ORDER_NUMBER)
                                    AND (A5.CS_WAREHOUSE_SK <> A4.CS_WAREHOUSE_SK)
                                )
                            WHERE
                                (
                                    A6.D_DATE <= DATE_ADD('day', 60, DATE('1999-05-01'))
                                )
                                AND (DATE('1999-05-01') <= A6.D_DATE)
                                AND (A7.CA_STATE = 'ID')
                                AND (A8.CC_COUNTY = 'Williamson County')
                        ) A3 ON (A3.C3 = A2.CR_ORDER_NUMBER)
                    )
            ) A1
    ) A0
ORDER BY
    1 ASC
limit
    100;