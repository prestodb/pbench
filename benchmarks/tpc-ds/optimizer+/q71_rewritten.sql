SELECT
    A0.C2 "BRAND_ID",
    A0.C3 "BRAND",
    A0.C1 "T_HOUR",
    A0.C0 "T_MINUTE",
    SUM(A0.C4) "EXT_PRICE"
FROM
    (
        (
            SELECT
                A4.T_MINUTE C0,
                A4.T_HOUR C1,
                A3.I_BRAND_ID C2,
                A3.I_BRAND C3,
                SUM(A1.WS_EXT_SALES_PRICE) C4
            FROM
                (
                    (
                        (
                            WEB_SALES A1
                            INNER JOIN DATE_DIM A2 ON (A2.D_DATE_SK = A1.WS_SOLD_DATE_SK)
                        )
                        INNER JOIN ITEM A3 ON (A1.WS_ITEM_SK = A3.I_ITEM_SK)
                    )
                    INNER JOIN TIME_DIM A4 ON (A1.WS_SOLD_TIME_SK = A4.T_TIME_SK)
                )
            WHERE
                (A2.D_MOY = 11)
                AND (A2.D_YEAR = 2001)
                AND (A3.I_MANAGER_ID = 1)
                AND (
                    A4.T_MEAL_TIME IN ('breakfast           ', 'dinner              ')
                )
            GROUP BY
                A4.T_MINUTE,
                A4.T_HOUR,
                A3.I_BRAND_ID,
                A3.I_BRAND
        )
        UNION
        ALL (
            SELECT
                A8.T_MINUTE C0,
                A8.T_HOUR C1,
                A7.I_BRAND_ID C2,
                A7.I_BRAND C3,
                SUM(A5.CS_EXT_SALES_PRICE) C4
            FROM
                (
                    (
                        (
                            CATALOG_SALES A5
                            INNER JOIN DATE_DIM A6 ON (A6.D_DATE_SK = A5.CS_SOLD_DATE_SK)
                        )
                        INNER JOIN ITEM A7 ON (A5.CS_ITEM_SK = A7.I_ITEM_SK)
                    )
                    INNER JOIN TIME_DIM A8 ON (A5.CS_SOLD_TIME_SK = A8.T_TIME_SK)
                )
            WHERE
                (A6.D_MOY = 11)
                AND (A6.D_YEAR = 2001)
                AND (A7.I_MANAGER_ID = 1)
                AND (
                    A8.T_MEAL_TIME IN ('breakfast           ', 'dinner              ')
                )
            GROUP BY
                A8.T_MINUTE,
                A8.T_HOUR,
                A7.I_BRAND_ID,
                A7.I_BRAND
        )
        UNION
        ALL (
            SELECT
                A12.T_MINUTE C0,
                A12.T_HOUR C1,
                A11.I_BRAND_ID C2,
                A11.I_BRAND C3,
                SUM(A9.SS_EXT_SALES_PRICE) C4
            FROM
                (
                    (
                        (
                            STORE_SALES A9
                            INNER JOIN DATE_DIM A10 ON (A10.D_DATE_SK = A9.SS_SOLD_DATE_SK)
                        )
                        INNER JOIN ITEM A11 ON (A9.SS_ITEM_SK = A11.I_ITEM_SK)
                    )
                    INNER JOIN TIME_DIM A12 ON (A9.SS_SOLD_TIME_SK = A12.T_TIME_SK)
                )
            WHERE
                (A10.D_MOY = 11)
                AND (A10.D_YEAR = 2001)
                AND (A11.I_MANAGER_ID = 1)
                AND (
                    A12.T_MEAL_TIME IN ('breakfast           ', 'dinner              ')
                )
            GROUP BY
                A12.T_MINUTE,
                A12.T_HOUR,
                A11.I_BRAND_ID,
                A11.I_BRAND
        )
    ) A0
GROUP BY
    A0.C3,
    A0.C2,
    A0.C1,
    A0.C0
ORDER BY
    5 DESC,
    1 ASC;