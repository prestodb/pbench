SELECT
    A1.C4 "W_STATE",
    A1.C0 "I_ITEM_ID",
    SUM(
        CASE
            WHEN (A1.C5 < DATE('2002-05-18')) THEN (
                A1.C1 - COALESCE(
                    CAST(A0.CR_REFUNDED_CASH AS DECIMAL(13, 2)),
                    00000000000.00
                )
            )
            ELSE 000000000000.00
        END
    ) "SALES_BEFORE",
    SUM(
        CASE
            WHEN (A1.C5 >= DATE('2002-05-18')) THEN (
                A1.C1 - COALESCE(
                    CAST(A0.CR_REFUNDED_CASH AS DECIMAL(13, 2)),
                    00000000000.00
                )
            )
            ELSE 000000000000.00
        END
    ) "SALES_AFTER"
FROM
    (
        CATALOG_RETURNS A0
        RIGHT OUTER JOIN (
            SELECT
                A3.I_ITEM_ID C0,
                A2.CS_SALES_PRICE C1,
                A2.CS_ITEM_SK C2,
                A2.CS_ORDER_NUMBER C3,
                A5.W_STATE C4,
                A4.D_DATE C5
            FROM
                (
                    (
                        (
                            CATALOG_SALES A2
                            INNER JOIN ITEM A3 ON (A2.CS_ITEM_SK = A3.I_ITEM_SK)
                        )
                        INNER JOIN DATE_DIM A4 ON (A4.D_DATE_SK = A2.CS_SOLD_DATE_SK)
                    )
                    INNER JOIN WAREHOUSE A5 ON (A5.W_WAREHOUSE_SK = A2.CS_WAREHOUSE_SK)
                )
            WHERE
                (0.99 <= A3.I_CURRENT_PRICE)
                AND (A3.I_CURRENT_PRICE <= 1.49)
                AND (
                    DATE_ADD('day', -30, DATE('2002-05-18')) <= A4.D_DATE
                )
                AND (
                    A4.D_DATE <= DATE_ADD('day', 30, DATE('2002-05-18'))
                )
        ) A1 ON (A1.C3 = A0.CR_ORDER_NUMBER)
        AND (A1.C2 = A0.CR_ITEM_SK)
    )
GROUP BY
    A1.C4,
    A1.C0
ORDER BY
    1 ASC,
    2 ASC
limit
    100;