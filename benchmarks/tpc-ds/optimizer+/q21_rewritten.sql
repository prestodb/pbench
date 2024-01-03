SELECT
    A0.C0 "W_WAREHOUSE_NAME",
    A0.C1 "I_ITEM_ID",
    A0.C2 "INV_BEFORE",
    A0.C3 "INV_AFTER"
FROM
    (
        SELECT
            A4.W_WAREHOUSE_NAME C0,
            A2.I_ITEM_ID C1,
            SUM(
                CASE
                    WHEN (A3.D_DATE < DATE('2000-05-19')) THEN A1.INV_QUANTITY_ON_HAND
                    ELSE 0
                END
            ) C2,
            SUM(
                CASE
                    WHEN (A3.D_DATE >= DATE('2000-05-19')) THEN A1.INV_QUANTITY_ON_HAND
                    ELSE 0
                END
            ) C3
        FROM
            (
                (
                    (
                        INVENTORY A1
                        INNER JOIN ITEM A2 ON (A2.I_ITEM_SK = A1.INV_ITEM_SK)
                    )
                    INNER JOIN DATE_DIM A3 ON (A1.INV_DATE_SK = A3.D_DATE_SK)
                )
                INNER JOIN WAREHOUSE A4 ON (A1.INV_WAREHOUSE_SK = A4.W_WAREHOUSE_SK)
            )
        WHERE
            (0.99 <= A2.I_CURRENT_PRICE)
            AND (A2.I_CURRENT_PRICE <= 1.49)
            AND (
                DATE_ADD('day', -30, DATE('2000-05-19')) <= A3.D_DATE
            )
            AND (
                A3.D_DATE <= DATE_ADD('day', 30, DATE('2000-05-19'))
            )
        GROUP BY
            A4.W_WAREHOUSE_NAME,
            A2.I_ITEM_ID
    ) A0
WHERE
    (
        00.66666666666666666666666666666 <= CASE
            WHEN (A0.C2 > 0) THEN (A0.C3 / A0.C2)
            ELSE NULL
        END
    )
    AND (
        CASE
            WHEN (A0.C2 > 0) THEN (A0.C3 / A0.C2)
            ELSE NULL
        END <= 01.50000000000000000000000000000
    )
ORDER BY
    1 ASC,
    2 ASC
limit
    100;