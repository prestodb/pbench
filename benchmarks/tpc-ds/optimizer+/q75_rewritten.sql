WITH A0 AS (
    SELECT
        A1.C0 C0,
        A1.C1 C1,
        A1.C2 C2,
        A1.C3 C3,
        A1.C4 C4,
        SUM(A1.C5) C5,
        SUM(A1.C6) C6
    FROM
        (
            SELECT
                DISTINCT A2.C0 C0,
                A2.C1 C1,
                A2.C2 C2,
                A2.C3 C3,
                A2.C4 C4,
                A2.C5 C5,
                A2.C6 C6
            FROM
                (
                    (
                        SELECT
                            A4.C4 C0,
                            A4.C8 C1,
                            A4.C7 C2,
                            A4.C6 C3,
                            A4.C5 C4,
                            (A4.C1 - COALESCE(A3.CR_RETURN_QUANTITY, 0)) C5,
                            (A4.C0 - COALESCE(A3.CR_RETURN_AMOUNT, 00000.00)) C6
                        FROM
                            (
                                CATALOG_RETURNS A3
                                RIGHT OUTER JOIN (
                                    SELECT
                                        A5.CS_EXT_SALES_PRICE C0,
                                        A5.CS_QUANTITY C1,
                                        A5.CS_ORDER_NUMBER C2,
                                        A5.CS_ITEM_SK C3,
                                        A6.D_YEAR C4,
                                        A7.I_MANUFACT_ID C5,
                                        A7.I_CATEGORY_ID C6,
                                        A7.I_CLASS_ID C7,
                                        A7.I_BRAND_ID C8
                                    FROM
                                        (
                                            (
                                                CATALOG_SALES A5
                                                INNER JOIN DATE_DIM A6 ON (A6.D_DATE_SK = A5.CS_SOLD_DATE_SK)
                                            )
                                            INNER JOIN ITEM A7 ON (A7.I_ITEM_SK = A5.CS_ITEM_SK)
                                        )
                                    WHERE
                                        (A6.D_YEAR IN (1999, 2000))
                                        AND (
                                            A7.I_CATEGORY = 'Shoes                                             '
                                        )
                                ) A4 ON (A4.C2 = A3.CR_ORDER_NUMBER)
                                AND (A4.C3 = A3.CR_ITEM_SK)
                            )
                    )
                    UNION
                    ALL (
                        SELECT
                            A9.C4 C0,
                            A9.C8 C1,
                            A9.C7 C2,
                            A9.C6 C3,
                            A9.C5 C4,
                            (A9.C1 - COALESCE(A8.SR_RETURN_QUANTITY, 0)) C5,
                            (A9.C0 - COALESCE(A8.SR_RETURN_AMT, 00000.00)) C6
                        FROM
                            (
                                STORE_RETURNS A8
                                RIGHT OUTER JOIN (
                                    SELECT
                                        A10.SS_EXT_SALES_PRICE C0,
                                        A10.SS_QUANTITY C1,
                                        A10.SS_TICKET_NUMBER C2,
                                        A10.SS_ITEM_SK C3,
                                        A11.D_YEAR C4,
                                        A12.I_MANUFACT_ID C5,
                                        A12.I_CATEGORY_ID C6,
                                        A12.I_CLASS_ID C7,
                                        A12.I_BRAND_ID C8
                                    FROM
                                        (
                                            (
                                                STORE_SALES A10
                                                INNER JOIN DATE_DIM A11 ON (A11.D_DATE_SK = A10.SS_SOLD_DATE_SK)
                                            )
                                            INNER JOIN ITEM A12 ON (A12.I_ITEM_SK = A10.SS_ITEM_SK)
                                        )
                                    WHERE
                                        (A11.D_YEAR IN (1999, 2000))
                                        AND (
                                            A12.I_CATEGORY = 'Shoes                                             '
                                        )
                                ) A9 ON (A9.C2 = A8.SR_TICKET_NUMBER)
                                AND (A9.C3 = A8.SR_ITEM_SK)
                            )
                    )
                    UNION
                    ALL (
                        SELECT
                            A14.C4 C0,
                            A14.C8 C1,
                            A14.C7 C2,
                            A14.C6 C3,
                            A14.C5 C4,
                            (A14.C1 - COALESCE(A13.WR_RETURN_QUANTITY, 0)) C5,
                            (A14.C0 - COALESCE(A13.WR_RETURN_AMT, 00000.00)) C6
                        FROM
                            (
                                WEB_RETURNS A13
                                RIGHT OUTER JOIN (
                                    SELECT
                                        A15.WS_EXT_SALES_PRICE C0,
                                        A15.WS_QUANTITY C1,
                                        A15.WS_ORDER_NUMBER C2,
                                        A15.WS_ITEM_SK C3,
                                        A16.D_YEAR C4,
                                        A17.I_MANUFACT_ID C5,
                                        A17.I_CATEGORY_ID C6,
                                        A17.I_CLASS_ID C7,
                                        A17.I_BRAND_ID C8
                                    FROM
                                        (
                                            (
                                                WEB_SALES A15
                                                INNER JOIN DATE_DIM A16 ON (A16.D_DATE_SK = A15.WS_SOLD_DATE_SK)
                                            )
                                            INNER JOIN ITEM A17 ON (A17.I_ITEM_SK = A15.WS_ITEM_SK)
                                        )
                                    WHERE
                                        (A16.D_YEAR IN (1999, 2000))
                                        AND (
                                            A17.I_CATEGORY = 'Shoes                                             '
                                        )
                                ) A14 ON (A14.C2 = A13.WR_ORDER_NUMBER)
                                AND (A14.C3 = A13.WR_ITEM_SK)
                            )
                    )
                ) A2
        ) A1
    GROUP BY
        A1.C0,
        A1.C1,
        A1.C2,
        A1.C3,
        A1.C4
)
SELECT
    1999 "PREV_YEAR",
    2000 "YEAR",
    "A19".C1 "I_BRAND_ID",
    "A19".C2 "I_CLASS_ID",
    "A19".C3 "I_CATEGORY_ID",
    "A19".C4 "I_MANUFACT_ID",
    "A18".C5 "PREV_YR_CNT",
    "A19".C5 "CURR_YR_CNT",
    ("A19".C5 - "A18".C5) "SALES_CNT_DIFF",
    ("A19".C6 - "A18".C6) "SALES_AMT_DIFF"
FROM
    (
        A0 "A18"
        INNER JOIN A0 "A19" ON ("A19".C1 = "A18".C1)
        AND ("A19".C2 = "A18".C2)
        AND ("A19".C3 = "A18".C3)
        AND ("A19".C4 = "A18".C4)
        AND (
            (
                CAST("A19".C5 AS DECIMAL(17, 2)) / CAST("A18".C5 AS DECIMAL(17, 2))
            ) < 0.9
        )
    )
WHERE
    ("A18".C0 = 1999)
    AND ("A19".C0 = 2000)
ORDER BY
    9 ASC,
    10 ASC
limit
    100;