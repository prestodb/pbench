WITH A0 AS (
    SELECT
        A1.SS_STORE_SK C0,
        SUM(A1.SS_SALES_PRICE) C1,
        A1.SS_ITEM_SK C2
    FROM
        (
            STORE_SALES A1
            INNER JOIN DATE_DIM A2 ON (A1.SS_SOLD_DATE_SK = A2.D_DATE_SK)
        )
    WHERE
        (1186 <= A2.D_MONTH_SEQ)
        AND (A2.D_MONTH_SEQ <= 1197)
    GROUP BY
        A1.SS_STORE_SK,
        A1.SS_ITEM_SK
)
SELECT
    A6.S_STORE_NAME "S_STORE_NAME",
    A7.I_ITEM_DESC "I_ITEM_DESC",
    "A3".C1 "REVENUE",
    A7.I_CURRENT_PRICE "I_CURRENT_PRICE",
    A7.I_WHOLESALE_COST "I_WHOLESALE_COST",
    A7.I_BRAND "I_BRAND"
FROM
    (
        (
            A0 "A3"
            INNER JOIN (
                (
                    SELECT
                        "A5".C0 C0,
                        SUM("A5".C1) C1,
                        COUNT("A5".C1) C2
                    FROM
                        A0 "A5"
                    GROUP BY
                        "A5".C0
                ) A4
                INNER JOIN STORE A6 ON (A4.C0 = A6.S_STORE_SK)
            ) ON (A6.S_STORE_SK = "A3".C0)
            AND ("A3".C1 <= (0.1 * (A4.C1 / A4.C2)))
        )
        INNER JOIN ITEM A7 ON (A7.I_ITEM_SK = "A3".C2)
    )
ORDER BY
    1 ASC,
    2 ASC
limit
    100;