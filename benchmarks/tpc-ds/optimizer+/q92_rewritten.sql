SELECT
    SUM(A0.C0) "Excess Discount Amount"
FROM
    (
        SELECT
            A1.WS_EXT_DISCOUNT_AMT C0,
            COUNT(A1.WS_EXT_DISCOUNT_AMT) OVER(PARTITION BY A1.WS_ITEM_SK) C1,
            SUM(A1.WS_EXT_DISCOUNT_AMT) OVER(PARTITION BY A1.WS_ITEM_SK) C2
        FROM
            (
                (
                    WEB_SALES A1
                    INNER JOIN ITEM A2 ON (A1.WS_ITEM_SK = A2.I_ITEM_SK)
                )
                INNER JOIN DATE_DIM A3 ON (A3.D_DATE_SK = A1.WS_SOLD_DATE_SK)
            )
        WHERE
            (A2.I_MANUFACT_ID = 914)
            AND (
                A3.D_DATE <= DATE_ADD('day', 90, DATE('2001-01-25'))
            )
            AND (DATE('2001-01-25') <= A3.D_DATE)
    ) A0
WHERE
    ((1.3 * (A0.C2 / A0.C1)) < A0.C0)
ORDER BY
    1 ASC
limit
    100;