SELECT
    SUM(A0.C0) "excess discount amount"
FROM
    (
        SELECT
            A1.CS_EXT_DISCOUNT_AMT C0,
            COUNT(A1.CS_EXT_DISCOUNT_AMT) OVER(PARTITION BY A1.CS_ITEM_SK) C1,
            SUM(A1.CS_EXT_DISCOUNT_AMT) OVER(PARTITION BY A1.CS_ITEM_SK) C2
        FROM
            (
                (
                    CATALOG_SALES A1
                    INNER JOIN ITEM A2 ON (A1.CS_ITEM_SK = A2.I_ITEM_SK)
                )
                INNER JOIN DATE_DIM A3 ON (A3.D_DATE_SK = A1.CS_SOLD_DATE_SK)
            )
        WHERE
            (A2.I_MANUFACT_ID = 283)
            AND (
                A3.D_DATE <= DATE_ADD('day', 90, DATE('1999-02-22'))
            )
            AND (DATE('1999-02-22') <= A3.D_DATE)
    ) A0
WHERE
    ((1.3 * (A0.C2 / A0.C1)) < A0.C0)
limit
    100;