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
                CASE
                    WHEN (A29.C0 = 1) THEN A1.C0
                    WHEN (A29.C0 = 2) THEN A1.C0
                    ELSE NULL
                END C0,
                CASE
                    WHEN (A29.C0 = 1) THEN A1.C1
                    WHEN (A29.C0 = 2) THEN NULL
                    ELSE NULL
                END C1,
                SUM(A1.C2) C2,
                SUM(A1.C3) C3,
                SUM(A1.C4) C4,
                A29.C0 C5
            FROM
                (
                    (
                        SELECT
                            A2.C0 C0,
                            A2.C1 C1,
                            SUM(A2.C2) C2,
                            SUM(A2.C3) C3,
                            SUM(A2.C4) C4
                        FROM
                            (
                                (
                                    SELECT
                                        CAST('store channel' AS VARCHAR(15)) C0,
                                        A8.C0 C1,
                                        A8.C1 C2,
                                        COALESCE(A3.C1, 00000000000000000000000000000.00) C3,
                                        (
                                            A8.C2 - COALESCE(A3.C2, 00000000000000000000000000000.00)
                                        ) C4
                                    FROM
                                        (
                                            (
                                                SELECT
                                                    A7.S_STORE_SK C0,
                                                    A4.C1 C1,
                                                    A4.C2 C2
                                                FROM
                                                    (
                                                        (
                                                            SELECT
                                                                A5.SR_STORE_SK C0,
                                                                SUM(A5.SR_RETURN_AMT) C1,
                                                                SUM(A5.SR_NET_LOSS) C2
                                                            FROM
                                                                (
                                                                    STORE_RETURNS A5
                                                                    INNER JOIN DATE_DIM A6 ON (A5.SR_RETURNED_DATE_SK = A6.D_DATE_SK)
                                                                )
                                                            WHERE
                                                                (
                                                                    A6.D_DATE <= DATE_ADD('day', 30, DATE('2001-08-11'))
                                                                )
                                                                AND (DATE('2001-08-11') <= A6.D_DATE)
                                                            GROUP BY
                                                                A5.SR_STORE_SK
                                                        ) A4
                                                        INNER JOIN STORE A7 ON (A4.C0 = A7.S_STORE_SK)
                                                    )
                                            ) A3
                                            RIGHT OUTER JOIN (
                                                SELECT
                                                    A12.S_STORE_SK C0,
                                                    A9.C1 C1,
                                                    A9.C2 C2
                                                FROM
                                                    (
                                                        (
                                                            SELECT
                                                                A10.SS_STORE_SK C0,
                                                                SUM(A10.SS_EXT_SALES_PRICE) C1,
                                                                SUM(A10.SS_NET_PROFIT) C2
                                                            FROM
                                                                (
                                                                    STORE_SALES A10
                                                                    INNER JOIN DATE_DIM A11 ON (A10.SS_SOLD_DATE_SK = A11.D_DATE_SK)
                                                                )
                                                            WHERE
                                                                (
                                                                    A11.D_DATE <= DATE_ADD('day', 30, DATE('2001-08-11'))
                                                                )
                                                                AND (DATE('2001-08-11') <= A11.D_DATE)
                                                            GROUP BY
                                                                A10.SS_STORE_SK
                                                        ) A9
                                                        INNER JOIN STORE A12 ON (A9.C0 = A12.S_STORE_SK)
                                                    )
                                            ) A8 ON (A8.C0 = A3.C0)
                                        )
                                )
                                UNION
                                ALL (
                                    SELECT
                                        'catalog channel' C0,
                                        A13.C0 C1,
                                        A13.C1 C2,
                                        A16.C0 C3,
                                        (A13.C2 - A16.C1) C4
                                    FROM
                                        (
                                            SELECT
                                                A14.CS_CALL_CENTER_SK C0,
                                                SUM(A14.CS_EXT_SALES_PRICE) C1,
                                                SUM(A14.CS_NET_PROFIT) C2
                                            FROM
                                                (
                                                    CATALOG_SALES A14
                                                    INNER JOIN DATE_DIM A15 ON (A14.CS_SOLD_DATE_SK = A15.D_DATE_SK)
                                                )
                                            WHERE
                                                (
                                                    A15.D_DATE <= DATE_ADD('day', 30, DATE('2001-08-11'))
                                                )
                                                AND (DATE('2001-08-11') <= A15.D_DATE)
                                            GROUP BY
                                                A14.CS_CALL_CENTER_SK
                                        ) A13,
                                        (
                                            SELECT
                                                SUM(A17.CR_RETURN_AMOUNT) C0,
                                                SUM(A17.CR_NET_LOSS) C1
                                            FROM
                                                (
                                                    CATALOG_RETURNS A17
                                                    INNER JOIN DATE_DIM A18 ON (A17.CR_RETURNED_DATE_SK = A18.D_DATE_SK)
                                                )
                                            WHERE
                                                (
                                                    A18.D_DATE <= DATE_ADD('day', 30, DATE('2001-08-11'))
                                                )
                                                AND (DATE('2001-08-11') <= A18.D_DATE)
                                            GROUP BY
                                                A17.CR_CALL_CENTER_SK
                                        ) A16
                                )
                                UNION
                                ALL (
                                    SELECT
                                        CAST('web channel' AS VARCHAR(15)) C0,
                                        A19.C0 C1,
                                        A19.C1 C2,
                                        COALESCE(A24.C1, 00000000000000000000000000000.00) C3,
                                        (
                                            A19.C2 - COALESCE(A24.C2, 00000000000000000000000000000.00)
                                        ) C4
                                    FROM
                                        (
                                            (
                                                SELECT
                                                    A23.WP_WEB_PAGE_SK C0,
                                                    A20.C1 C1,
                                                    A20.C2 C2
                                                FROM
                                                    (
                                                        (
                                                            SELECT
                                                                A21.WS_WEB_PAGE_SK C0,
                                                                SUM(A21.WS_EXT_SALES_PRICE) C1,
                                                                SUM(A21.WS_NET_PROFIT) C2
                                                            FROM
                                                                (
                                                                    WEB_SALES A21
                                                                    INNER JOIN DATE_DIM A22 ON (A21.WS_SOLD_DATE_SK = A22.D_DATE_SK)
                                                                )
                                                            WHERE
                                                                (
                                                                    A22.D_DATE <= DATE_ADD('day', 30, DATE('2001-08-11'))
                                                                )
                                                                AND (DATE('2001-08-11') <= A22.D_DATE)
                                                            GROUP BY
                                                                A21.WS_WEB_PAGE_SK
                                                        ) A20
                                                        INNER JOIN WEB_PAGE A23 ON (A20.C0 = A23.WP_WEB_PAGE_SK)
                                                    )
                                            ) A19
                                            LEFT OUTER JOIN (
                                                SELECT
                                                    A28.WP_WEB_PAGE_SK C0,
                                                    A25.C1 C1,
                                                    A25.C2 C2
                                                FROM
                                                    (
                                                        (
                                                            SELECT
                                                                A26.WR_WEB_PAGE_SK C0,
                                                                SUM(A26.WR_RETURN_AMT) C1,
                                                                SUM(A26.WR_NET_LOSS) C2
                                                            FROM
                                                                (
                                                                    WEB_RETURNS A26
                                                                    INNER JOIN DATE_DIM A27 ON (A26.WR_RETURNED_DATE_SK = A27.D_DATE_SK)
                                                                )
                                                            WHERE
                                                                (
                                                                    A27.D_DATE <= DATE_ADD('day', 30, DATE('2001-08-11'))
                                                                )
                                                                AND (DATE('2001-08-11') <= A27.D_DATE)
                                                            GROUP BY
                                                                A26.WR_WEB_PAGE_SK
                                                        ) A25
                                                        INNER JOIN WEB_PAGE A28 ON (A25.C0 = A28.WP_WEB_PAGE_SK)
                                                    )
                                            ) A24 ON (A19.C0 = A24.C0)
                                        )
                                )
                            ) A2
                        GROUP BY
                            A2.C0,
                            A2.C1
                    ) A1
                    INNER JOIN (
                        VALUES
                            1,
                            2,
                            3
                    ) A29 (C0) ON (MOD(LENGTH(A29.C0), 1) = MOD(LENGTH(A1.C0), 1))
                )
            GROUP BY
                A29.C0,
                CASE
                    WHEN (A29.C0 = 1) THEN A1.C0
                    WHEN (A29.C0 = 2) THEN A1.C0
                    ELSE NULL
                END,
                CASE
                    WHEN (A29.C0 = 1) THEN A1.C1
                    WHEN (A29.C0 = 2) THEN NULL
                    ELSE NULL
                END
        ) A0
        RIGHT OUTER JOIN (
            VALUES
                1,
                2,
                3
        ) A30 (C0) ON (A30.C0 = A0.C5)
    )
WHERE
    (
        (
            (A30.C0 = 3)
            AND (A0.C5 IS NULL)
        )
        OR (A0.C5 IS NOT NULL)
    )
ORDER BY
    1 ASC,
    2 ASC
limit
    100;