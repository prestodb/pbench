WITH A2 AS (
    SELECT
        A3.C0 C0,
        A3.C1 C1,
        SUM(A3.C2) C2,
        SUM(A3.C3) C3,
        SUM(A3.C4) C4
    FROM
        (
            (
                SELECT
                    CAST('store channel' AS VARCHAR(15)) C0,
                    CAST(('store' || A4.C0) AS VARCHAR(28)) C1,
                    A4.C1 C2,
                    A4.C2 C3,
                    A4.C3 C4
                FROM
                    (
                        SELECT
                            A6.C4 C0,
                            SUM(A6.C1) C1,
                            SUM(
                                COALESCE(
                                    CAST(A5.SR_RETURN_AMT AS DECIMAL(13, 2)),
                                    00000000000.00
                                )
                            ) C2,
                            SUM(
                                (
                                    A6.C0 - COALESCE(
                                        CAST(A5.SR_NET_LOSS AS DECIMAL(13, 2)),
                                        00000000000.00
                                    )
                                )
                            ) C3
                        FROM
                            (
                                STORE_RETURNS A5
                                RIGHT OUTER JOIN (
                                    SELECT
                                        A7.SS_NET_PROFIT C0,
                                        A7.SS_EXT_SALES_PRICE C1,
                                        A7.SS_TICKET_NUMBER C2,
                                        A7.SS_ITEM_SK C3,
                                        A11.S_STORE_ID C4
                                    FROM
                                        (
                                            (
                                                (
                                                    (
                                                        STORE_SALES A7
                                                        INNER JOIN DATE_DIM A8 ON (A8.D_DATE_SK = A7.SS_SOLD_DATE_SK)
                                                    )
                                                    INNER JOIN PROMOTION A9 ON (A9.P_PROMO_SK = A7.SS_PROMO_SK)
                                                )
                                                INNER JOIN ITEM A10 ON (A10.I_ITEM_SK = A7.SS_ITEM_SK)
                                            )
                                            INNER JOIN STORE A11 ON (A11.S_STORE_SK = A7.SS_STORE_SK)
                                        )
                                    WHERE
                                        (
                                            A8.D_DATE <= DATE_ADD('day', 30, DATE('2002-08-04'))
                                        )
                                        AND (DATE('2002-08-04') <= A8.D_DATE)
                                        AND ('N' = A9.P_CHANNEL_TV)
                                        AND (50 < A10.I_CURRENT_PRICE)
                                ) A6 ON (A6.C3 = A5.SR_ITEM_SK)
                                AND (A6.C2 = A5.SR_TICKET_NUMBER)
                            )
                        GROUP BY
                            A6.C4
                    ) A4
            )
            UNION
            ALL (
                SELECT
                    'catalog channel' C0,
                    ('catalog_page' || A12.C0) C1,
                    A12.C1 C2,
                    A12.C2 C3,
                    A12.C3 C4
                FROM
                    (
                        SELECT
                            A14.C4 C0,
                            SUM(A14.C1) C1,
                            SUM(
                                COALESCE(
                                    CAST(A13.CR_RETURN_AMOUNT AS DECIMAL(13, 2)),
                                    00000000000.00
                                )
                            ) C2,
                            SUM(
                                (
                                    A14.C0 - COALESCE(
                                        CAST(A13.CR_NET_LOSS AS DECIMAL(13, 2)),
                                        00000000000.00
                                    )
                                )
                            ) C3
                        FROM
                            (
                                CATALOG_RETURNS A13
                                RIGHT OUTER JOIN (
                                    SELECT
                                        A15.CS_NET_PROFIT C0,
                                        A15.CS_EXT_SALES_PRICE C1,
                                        A15.CS_ORDER_NUMBER C2,
                                        A15.CS_ITEM_SK C3,
                                        A19.CP_CATALOG_PAGE_ID C4
                                    FROM
                                        (
                                            (
                                                (
                                                    (
                                                        CATALOG_SALES A15
                                                        INNER JOIN DATE_DIM A16 ON (A16.D_DATE_SK = A15.CS_SOLD_DATE_SK)
                                                    )
                                                    INNER JOIN PROMOTION A17 ON (A17.P_PROMO_SK = A15.CS_PROMO_SK)
                                                )
                                                INNER JOIN ITEM A18 ON (A18.I_ITEM_SK = A15.CS_ITEM_SK)
                                            )
                                            INNER JOIN CATALOG_PAGE A19 ON (A19.CP_CATALOG_PAGE_SK = A15.CS_CATALOG_PAGE_SK)
                                        )
                                    WHERE
                                        (
                                            A16.D_DATE <= DATE_ADD('day', 30, DATE('2002-08-04'))
                                        )
                                        AND (DATE('2002-08-04') <= A16.D_DATE)
                                        AND ('N' = A17.P_CHANNEL_TV)
                                        AND (50 < A18.I_CURRENT_PRICE)
                                ) A14 ON (A14.C3 = A13.CR_ITEM_SK)
                                AND (A14.C2 = A13.CR_ORDER_NUMBER)
                            )
                        GROUP BY
                            A14.C4
                    ) A12
            )
            UNION
            ALL (
                SELECT
                    CAST('web channel' AS VARCHAR(15)) C0,
                    CAST(('web_site' || A20.C0) AS VARCHAR(28)) C1,
                    A20.C1 C2,
                    A20.C2 C3,
                    A20.C3 C4
                FROM
                    (
                        SELECT
                            A22.C4 C0,
                            SUM(A22.C1) C1,
                            SUM(
                                COALESCE(
                                    CAST(A21.WR_RETURN_AMT AS DECIMAL(13, 2)),
                                    00000000000.00
                                )
                            ) C2,
                            SUM(
                                (
                                    A22.C0 - COALESCE(
                                        CAST(A21.WR_NET_LOSS AS DECIMAL(13, 2)),
                                        00000000000.00
                                    )
                                )
                            ) C3
                        FROM
                            (
                                WEB_RETURNS A21
                                RIGHT OUTER JOIN (
                                    SELECT
                                        A23.WS_NET_PROFIT C0,
                                        A23.WS_EXT_SALES_PRICE C1,
                                        A23.WS_ORDER_NUMBER C2,
                                        A23.WS_ITEM_SK C3,
                                        A27.WEB_SITE_ID C4
                                    FROM
                                        (
                                            (
                                                (
                                                    (
                                                        WEB_SALES A23
                                                        INNER JOIN DATE_DIM A24 ON (A24.D_DATE_SK = A23.WS_SOLD_DATE_SK)
                                                    )
                                                    INNER JOIN PROMOTION A25 ON (A25.P_PROMO_SK = A23.WS_PROMO_SK)
                                                )
                                                INNER JOIN ITEM A26 ON (A26.I_ITEM_SK = A23.WS_ITEM_SK)
                                            )
                                            INNER JOIN WEB_SITE A27 ON (A27.WEB_SITE_SK = A23.WS_WEB_SITE_SK)
                                        )
                                    WHERE
                                        (
                                            A24.D_DATE <= DATE_ADD('day', 30, DATE('2002-08-04'))
                                        )
                                        AND (DATE('2002-08-04') <= A24.D_DATE)
                                        AND ('N' = A25.P_CHANNEL_TV)
                                        AND (50 < A26.I_CURRENT_PRICE)
                                ) A22 ON (A22.C3 = A21.WR_ITEM_SK)
                                AND (A22.C2 = A21.WR_ORDER_NUMBER)
                            )
                        GROUP BY
                            A22.C4
                    ) A20
            )
        ) A3
    GROUP BY
        A3.C0,
        A3.C1
),
A1 AS (
    SELECT
        "A28".C0 C0,
        NULL C1,
        SUM("A28".C2) C2,
        SUM("A28".C3) C3,
        SUM("A28".C4) C4
    FROM
        A2 "A28"
    GROUP BY
        "A28".C0
)
SELECT
    A0.C0 "CHANNEL",
    A0.C1 "ID",
    A0.C2 "SALES",
    A0.C3 "RETURNS",
    A0.C4 "PROFIT"
FROM
    (
        (
            SELECT
                NULL C0,
                NULL C1,
                SUM("A29".C2) C2,
                SUM("A29".C3) C3,
                SUM("A29".C4) C4
            FROM
                A1 "A29"
        )
        UNION
        ALL (
            SELECT
                "A30".C0 C0,
                "A30".C1 C1,
                "A30".C2 C2,
                "A30".C3 C3,
                "A30".C4 C4
            FROM
                A1 "A30"
        )
        UNION
        ALL (
            SELECT
                "A31".C0 C0,
                "A31".C1 C1,
                "A31".C2 C2,
                "A31".C3 C3,
                "A31".C4 C4
            FROM
                A2 "A31"
        )
    ) A0
ORDER BY
    1 ASC,
    2 ASC
limit
    100;