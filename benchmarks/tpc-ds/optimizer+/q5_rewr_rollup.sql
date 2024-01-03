SELECT
    A1.C0 "CHANNEL",
    A1.C1 "ID",
    A1.C2 "SALES",
    A1.C3 "RETURNS",
    A1.C4 "PROFIT"
FROM
    (
        (
            VALUES
                1,
                2,
                3
        ) A0 (C0)
        LEFT OUTER JOIN (
            SELECT
                CASE
                    WHEN (A2.C0 = 1) THEN A3.C0
                    WHEN (A2.C0 = 2) THEN A3.C0
                    ELSE NULL
                END C0,
                CASE
                    WHEN (A2.C0 = 1) THEN A3.C1
                    WHEN (A2.C0 = 2) THEN NULL
                    ELSE NULL
                END C1,
                SUM(A3.C2) C2,
                SUM(A3.C3) C3,
                SUM(A3.C4) C4,
                A2.C0 C5
            FROM
                (
                    (
                        VALUES
                            1,
                            2,
                            3
                    ) A2 (C0)
                    INNER JOIN (
                        (
                            SELECT
                                CAST('store channel' AS VARCHAR(15)) C0,
                                CAST(('store' || A4.C0) AS VARCHAR(28)) C1,
                                A4.C1 C2,
                                A4.C3 C3,
                                (A4.C2 - A4.C4) C4
                            FROM
                                (
                                    SELECT
                                        A5.C0 C0,
                                        SUM(A5.C4) C1,
                                        SUM(A5.C3) C2,
                                        SUM(A5.C2) C3,
                                        SUM(A5.C1) C4
                                    FROM
                                        (
                                            (
                                                SELECT
                                                    A8.S_STORE_ID C0,
                                                    SUM(A6.SR_NET_LOSS) C1,
                                                    SUM(A6.SR_RETURN_AMT) C2,
                                                    SUM(00000.00) C3,
                                                    SUM(00000.00) C4
                                                FROM
                                                    (
                                                        (
                                                            STORE_RETURNS A6
                                                            INNER JOIN DATE_DIM A7 ON (A6.SR_RETURNED_DATE_SK = A7.D_DATE_SK)
                                                        )
                                                        INNER JOIN STORE A8 ON (A6.SR_STORE_SK = A8.S_STORE_SK)
                                                    )
                                                WHERE
                                                    (
                                                        A7.D_DATE <= DATE_ADD('day', 14, DATE('2001-08-04'))
                                                    )
                                                    AND (DATE('2001-08-04') <= A7.D_DATE)
                                                GROUP BY
                                                    A8.S_STORE_ID
                                            )
                                            UNION
                                            ALL (
                                                SELECT
                                                    A11.S_STORE_ID C0,
                                                    SUM(00000.00) C1,
                                                    SUM(00000.00) C2,
                                                    SUM(A9.SS_NET_PROFIT) C3,
                                                    SUM(A9.SS_EXT_SALES_PRICE) C4
                                                FROM
                                                    (
                                                        (
                                                            STORE_SALES A9
                                                            INNER JOIN DATE_DIM A10 ON (A9.SS_SOLD_DATE_SK = A10.D_DATE_SK)
                                                        )
                                                        INNER JOIN STORE A11 ON (A9.SS_STORE_SK = A11.S_STORE_SK)
                                                    )
                                                WHERE
                                                    (
                                                        A10.D_DATE <= DATE_ADD('day', 14, DATE('2001-08-04'))
                                                    )
                                                    AND (DATE('2001-08-04') <= A10.D_DATE)
                                                GROUP BY
                                                    A11.S_STORE_ID
                                            )
                                        ) A5
                                    GROUP BY
                                        A5.C0
                                ) A4
                        )
                        UNION
                        ALL (
                            SELECT
                                'catalog channel' C0,
                                ('catalog_page' || A12.C0) C1,
                                A12.C1 C2,
                                A12.C3 C3,
                                (A12.C2 - A12.C4) C4
                            FROM
                                (
                                    SELECT
                                        A13.C0 C0,
                                        SUM(A13.C4) C1,
                                        SUM(A13.C3) C2,
                                        SUM(A13.C2) C3,
                                        SUM(A13.C1) C4
                                    FROM
                                        (
                                            (
                                                SELECT
                                                    A16.CP_CATALOG_PAGE_ID C0,
                                                    SUM(A14.CR_NET_LOSS) C1,
                                                    SUM(A14.CR_RETURN_AMOUNT) C2,
                                                    SUM(00000.00) C3,
                                                    SUM(00000.00) C4
                                                FROM
                                                    (
                                                        (
                                                            CATALOG_RETURNS A14
                                                            INNER JOIN DATE_DIM A15 ON (A14.CR_RETURNED_DATE_SK = A15.D_DATE_SK)
                                                        )
                                                        INNER JOIN CATALOG_PAGE A16 ON (A14.CR_CATALOG_PAGE_SK = A16.CP_CATALOG_PAGE_SK)
                                                    )
                                                WHERE
                                                    (
                                                        A15.D_DATE <= DATE_ADD('day', 14, DATE('2001-08-04'))
                                                    )
                                                    AND (DATE('2001-08-04') <= A15.D_DATE)
                                                GROUP BY
                                                    A16.CP_CATALOG_PAGE_ID
                                            )
                                            UNION
                                            ALL (
                                                SELECT
                                                    A19.CP_CATALOG_PAGE_ID C0,
                                                    SUM(00000.00) C1,
                                                    SUM(00000.00) C2,
                                                    SUM(A17.CS_NET_PROFIT) C3,
                                                    SUM(A17.CS_EXT_SALES_PRICE) C4
                                                FROM
                                                    (
                                                        (
                                                            CATALOG_SALES A17
                                                            INNER JOIN DATE_DIM A18 ON (A17.CS_SOLD_DATE_SK = A18.D_DATE_SK)
                                                        )
                                                        INNER JOIN CATALOG_PAGE A19 ON (A17.CS_CATALOG_PAGE_SK = A19.CP_CATALOG_PAGE_SK)
                                                    )
                                                WHERE
                                                    (
                                                        A18.D_DATE <= DATE_ADD('day', 14, DATE('2001-08-04'))
                                                    )
                                                    AND (DATE('2001-08-04') <= A18.D_DATE)
                                                GROUP BY
                                                    A19.CP_CATALOG_PAGE_ID
                                            )
                                        ) A13
                                    GROUP BY
                                        A13.C0
                                ) A12
                        )
                        UNION
                        ALL (
                            SELECT
                                CAST('web channel' AS VARCHAR(15)) C0,
                                CAST(('web_site' || A20.C0) AS VARCHAR(28)) C1,
                                A20.C1 C2,
                                A20.C3 C3,
                                (A20.C2 - A20.C4) C4
                            FROM
                                (
                                    SELECT
                                        A26.WEB_SITE_ID C0,
                                        SUM(A21.C2) C1,
                                        SUM(A21.C3) C2,
                                        SUM(A21.C4) C3,
                                        SUM(A21.C5) C4
                                    FROM
                                        (
                                            (
                                                (
                                                    (
                                                        SELECT
                                                            A23.WS_WEB_SITE_SK C0,
                                                            A22.WR_RETURNED_DATE_SK C1,
                                                            00000.00 C2,
                                                            00000.00 C3,
                                                            A22.WR_RETURN_AMT C4,
                                                            A22.WR_NET_LOSS C5
                                                        FROM
                                                            (
                                                                WEB_RETURNS A22
                                                                LEFT OUTER JOIN WEB_SALES A23 ON (A22.WR_ITEM_SK = A23.WS_ITEM_SK)
                                                                AND (A22.WR_ORDER_NUMBER = A23.WS_ORDER_NUMBER)
                                                            )
                                                    )
                                                    UNION
                                                    ALL (
                                                        SELECT
                                                            A24.WS_WEB_SITE_SK C0,
                                                            A24.WS_SOLD_DATE_SK C1,
                                                            A24.WS_EXT_SALES_PRICE C2,
                                                            A24.WS_NET_PROFIT C3,
                                                            00000.00 C4,
                                                            00000.00 C5
                                                        FROM
                                                            WEB_SALES A24
                                                    )
                                                ) A21
                                                INNER JOIN DATE_DIM A25 ON (A21.C1 = A25.D_DATE_SK)
                                            )
                                            INNER JOIN WEB_SITE A26 ON (A21.C0 = A26.WEB_SITE_SK)
                                        )
                                    WHERE
                                        (
                                            A25.D_DATE <= DATE_ADD('day', 14, DATE('2001-08-04'))
                                        )
                                        AND (DATE('2001-08-04') <= A25.D_DATE)
                                    GROUP BY
                                        A26.WEB_SITE_ID
                                ) A20
                        )
                    ) A3 ON (MOD(LENGTH(A2.C0), 1) = MOD(LENGTH(A3.C0), 1))
                )
            GROUP BY
                A2.C0,
                CASE
                    WHEN (A2.C0 = 1) THEN A3.C0
                    WHEN (A2.C0 = 2) THEN A3.C0
                    ELSE NULL
                END,
                CASE
                    WHEN (A2.C0 = 1) THEN A3.C1
                    WHEN (A2.C0 = 2) THEN NULL
                    ELSE NULL
                END
        ) A1 ON (A0.C0 = A1.C5)
    )
WHERE
    (
        (
            (A0.C0 = 3)
            AND (A1.C5 IS NULL)
        )
        OR (A1.C5 IS NOT NULL)
    )
ORDER BY
    1 ASC,
    2 ASC
limit
    100;