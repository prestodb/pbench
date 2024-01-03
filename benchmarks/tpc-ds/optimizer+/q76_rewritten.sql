SELECT
    A0.C0 "CHANNEL",
    A0.C1 "COL_NAME",
    A0.C2 "D_YEAR",
    A0.C3 "D_QOY",
    A0.C4 "I_CATEGORY",
    SUM(A0.C6) "SALES_CNT",
    SUM(A0.C5) "SALES_AMT"
FROM
    (
        (
            SELECT
                CAST('store' AS VARCHAR(7)) C0,
                CAST(
                    CAST('ss_customer_sk' AS VARCHAR(16)) AS VARCHAR(19)
                ) C1,
                A3.D_YEAR C2,
                A3.D_QOY C3,
                A2.I_CATEGORY C4,
                SUM(A1.SS_EXT_SALES_PRICE) C5,
                COUNT(*) C6
            FROM
                (
                    (
                        STORE_SALES A1
                        INNER JOIN ITEM A2 ON (A1.SS_ITEM_SK = A2.I_ITEM_SK)
                    )
                    INNER JOIN DATE_DIM A3 ON (A1.SS_SOLD_DATE_SK = A3.D_DATE_SK)
                )
            WHERE
                (A1.SS_CUSTOMER_SK IS NULL)
            GROUP BY
                A2.I_CATEGORY,
                A3.D_QOY,
                A3.D_YEAR,
                CAST(
                    CAST('ss_customer_sk' AS VARCHAR(16)) AS VARCHAR(19)
                ),
                CAST('store' AS VARCHAR(7))
        )
        UNION
        ALL (
            SELECT
                CAST(CAST('web' AS VARCHAR(5)) AS VARCHAR(7)) C0,
                CAST('ws_ship_hdemo_sk' AS VARCHAR(19)) C1,
                A6.D_YEAR C2,
                A6.D_QOY C3,
                A4.I_CATEGORY C4,
                SUM(A5.WS_EXT_SALES_PRICE) C5,
                COUNT(*) C6
            FROM
                (
                    ITEM A4
                    INNER JOIN (
                        WEB_SALES A5
                        INNER JOIN DATE_DIM A6 ON (A5.WS_SOLD_DATE_SK = A6.D_DATE_SK)
                    ) ON (A5.WS_ITEM_SK = A4.I_ITEM_SK)
                )
            WHERE
                (A5.WS_SHIP_HDEMO_SK IS NULL)
            GROUP BY
                A4.I_CATEGORY,
                A6.D_QOY,
                A6.D_YEAR,
                CAST('ws_ship_hdemo_sk' AS VARCHAR(19)),
                CAST(CAST('web' AS VARCHAR(5)) AS VARCHAR(7))
        )
        UNION
        ALL (
            SELECT
                'catalog' C0,
                'cs_bill_customer_sk' C1,
                A9.D_YEAR C2,
                A9.D_QOY C3,
                A8.I_CATEGORY C4,
                SUM(A7.CS_EXT_SALES_PRICE) C5,
                COUNT(*) C6
            FROM
                (
                    (
                        CATALOG_SALES A7
                        INNER JOIN ITEM A8 ON (A7.CS_ITEM_SK = A8.I_ITEM_SK)
                    )
                    INNER JOIN DATE_DIM A9 ON (A7.CS_SOLD_DATE_SK = A9.D_DATE_SK)
                )
            WHERE
                (A7.CS_BILL_CUSTOMER_SK IS NULL)
            GROUP BY
                A8.I_CATEGORY,
                A9.D_QOY,
                A9.D_YEAR,
                'cs_bill_customer_sk',
                'catalog'
        )
    ) A0
GROUP BY
    A0.C0,
    A0.C1,
    A0.C2,
    A0.C3,
    A0.C4
ORDER BY
    1 ASC,
    2 ASC,
    3 ASC,
    4 ASC,
    5 ASC
limit
    100;