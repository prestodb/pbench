SELECT
    A0.C0 "I_ITEM_ID",
    A0.C1 "I_ITEM_DESC",
    A0.C2 "I_CATEGORY",
    A0.C3 "I_CLASS",
    A0.C4 "I_CURRENT_PRICE",
    A0.C5 "ITEMREVENUE",
    ((A0.C5 * 100) / A0.C6) "REVENUERATIO"
FROM
    (
        SELECT
            A1.C0 C0,
            A1.C1 C1,
            A1.C2 C2,
            A1.C3 C3,
            A1.C4 C4,
            A1.C5 C5,
            SUM(A1.C5) OVER(PARTITION BY A1.C3) C6
        FROM
            (
                SELECT
                    A4.I_ITEM_ID C0,
                    A4.I_ITEM_DESC C1,
                    A4.I_CATEGORY C2,
                    A4.I_CLASS C3,
                    A4.I_CURRENT_PRICE C4,
                    SUM(A2.CS_EXT_SALES_PRICE) C5
                FROM
                    (
                        (
                            CATALOG_SALES A2
                            INNER JOIN DATE_DIM A3 ON (A2.CS_SOLD_DATE_SK = A3.D_DATE_SK)
                        )
                        INNER JOIN ITEM A4 ON (A2.CS_ITEM_SK = A4.I_ITEM_SK)
                    )
                WHERE
                    (
                        A3.D_DATE <= DATE_ADD('day', 30, DATE('2002-04-01'))
                    )
                    AND (DATE('2002-04-01') <= A3.D_DATE)
                    AND (A4.I_CATEGORY IN ('Children', 'Sports', 'Music'))
                GROUP BY
                    A4.I_ITEM_ID,
                    A4.I_ITEM_DESC,
                    A4.I_CATEGORY,
                    A4.I_CLASS,
                    A4.I_CURRENT_PRICE
            ) A1
    ) A0
ORDER BY
    3 ASC,
    4 ASC,
    1 ASC,
    2 ASC,
    7 ASC
limit
    100;