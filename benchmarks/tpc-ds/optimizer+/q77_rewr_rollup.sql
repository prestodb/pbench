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
                    WHEN (A28.C0 = 1) THEN A1.C0
                    WHEN (A28.C0 = 2) THEN A1.C0
                    ELSE NULL
                END C0,
                CASE
                    WHEN (A28.C0 = 1) THEN A1.C1
                    WHEN (A28.C0 = 2) THEN NULL
                    ELSE NULL
                END C1,
                SUM(A1.C2) C2,
                SUM(A1.C3) C3,
                SUM(A1.C4) C4,
                A28.C0 C5
            FROM
                (
                    (
                        (
                            SELECT
                                CAST('store channel' AS VARCHAR(15)) C0,
                                A7.C0 C1,
                                A7.C1 C2,
                                COALESCE(A2.C1, 00000000000000000000000000000.00) C3,
                                (
                                    A7.C2 - COALESCE(A2.C2, 00000000000000000000000000000.00)
                                ) C4
                            FROM
                                (
                                    (
                                        SELECT
                                            A6.S_STORE_SK C0,
                                            A3.C1 C1,
                                            A3.C2 C2
                                        FROM
                                            (
                                                (
                                                    SELECT
                                                        A4.SR_STORE_SK C0,
                                                        SUM(A4.SR_RETURN_AMT) C1,
                                                        SUM(A4.SR_NET_LOSS) C2
                                                    FROM
                                                        (
                                                            STORE_RETURNS A4
                                                            INNER JOIN DATE_DIM A5 ON (A4.SR_RETURNED_DATE_SK = A5.D_DATE_SK)
                                                        )
                                                    WHERE
                                                        (
                                                            A5.D_DATE <= DATE_ADD('day', 30, DATE('2001-08-11'))
                                                        )
                                                        AND (DATE('2001-08-11') <= A5.D_DATE)
                                                    GROUP BY
                                                        A4.SR_STORE_SK
                                                ) A3
                                                INNER JOIN STORE A6 ON (A3.C0 = A6.S_STORE_SK)
                                            )
                                    ) A2
                                    RIGHT OUTER JOIN (
                                        SELECT
                                            A11.S_STORE_SK C0,
                                            A8.C1 C1,
                                            A8.C2 C2
                                        FROM
                                            (
                                                (
                                                    SELECT
                                                        A9.SS_STORE_SK C0,
                                                        SUM(A9.SS_EXT_SALES_PRICE) C1,
                                                        SUM(A9.SS_NET_PROFIT) C2
                                                    FROM
                                                        (
                                                            STORE_SALES A9
                                                            INNER JOIN DATE_DIM A10 ON (A9.SS_SOLD_DATE_SK = A10.D_DATE_SK)
                                                        )
                                                    WHERE
                                                        (
                                                            A10.D_DATE <= DATE_ADD('day', 30, DATE('2001-08-11'))
                                                        )
                                                        AND (DATE('2001-08-11') <= A10.D_DATE)
                                                    GROUP BY
                                                        A9.SS_STORE_SK
                                                ) A8
                                                INNER JOIN STORE A11 ON (A8.C0 = A11.S_STORE_SK)
                                            )
                                    ) A7 ON (A7.C0 = A2.C0)
                                )
                        )
                        UNION
                        ALL (
                            SELECT
                                'catalog channel' C0,
                                A12.C0 C1,
                                A12.C1 C2,
                                A15.C0 C3,
                                (A12.C2 - A15.C1) C4
                            FROM
                                (
                                    SELECT
                                        A13.CS_CALL_CENTER_SK C0,
                                        SUM(A13.CS_EXT_SALES_PRICE) C1,
                                        SUM(A13.CS_NET_PROFIT) C2
                                    FROM
                                        (
                                            CATALOG_SALES A13
                                            INNER JOIN DATE_DIM A14 ON (A13.CS_SOLD_DATE_SK = A14.D_DATE_SK)
                                        )
                                    WHERE
                                        (
                                            A14.D_DATE <= DATE_ADD('day', 30, DATE('2001-08-11'))
                                        )
                                        AND (DATE('2001-08-11') <= A14.D_DATE)
                                    GROUP BY
                                        A13.CS_CALL_CENTER_SK
                                ) A12,
                                (
                                    SELECT
                                        SUM(A16.CR_RETURN_AMOUNT) C0,
                                        SUM(A16.CR_NET_LOSS) C1
                                    FROM
                                        (
                                            CATALOG_RETURNS A16
                                            INNER JOIN DATE_DIM A17 ON (A16.CR_RETURNED_DATE_SK = A17.D_DATE_SK)
                                        )
                                    WHERE
                                        (
                                            A17.D_DATE <= DATE_ADD('day', 30, DATE('2001-08-11'))
                                        )
                                        AND (DATE('2001-08-11') <= A17.D_DATE)
                                    GROUP BY
                                        A16.CR_CALL_CENTER_SK
                                ) A15
                        )
                        UNION
                        ALL (
                            SELECT
                                CAST('web channel' AS VARCHAR(15)) C0,
                                A18.C0 C1,
                                A18.C1 C2,
                                COALESCE(A23.C1, 00000000000000000000000000000.00) C3,
                                (
                                    A18.C2 - COALESCE(A23.C2, 00000000000000000000000000000.00)
                                ) C4
                            FROM
                                (
                                    (
                                        SELECT
                                            A22.WP_WEB_PAGE_SK C0,
                                            A19.C1 C1,
                                            A19.C2 C2
                                        FROM
                                            (
                                                (
                                                    SELECT
                                                        A20.WS_WEB_PAGE_SK C0,
                                                        SUM(A20.WS_EXT_SALES_PRICE) C1,
                                                        SUM(A20.WS_NET_PROFIT) C2
                                                    FROM
                                                        (
                                                            WEB_SALES A20
                                                            INNER JOIN DATE_DIM A21 ON (A20.WS_SOLD_DATE_SK = A21.D_DATE_SK)
                                                        )
                                                    WHERE
                                                        (
                                                            A21.D_DATE <= DATE_ADD('day', 30, DATE('2001-08-11'))
                                                        )
                                                        AND (DATE('2001-08-11') <= A21.D_DATE)
                                                    GROUP BY
                                                        A20.WS_WEB_PAGE_SK
                                                ) A19
                                                INNER JOIN WEB_PAGE A22 ON (A19.C0 = A22.WP_WEB_PAGE_SK)
                                            )
                                    ) A18
                                    LEFT OUTER JOIN (
                                        SELECT
                                            A27.WP_WEB_PAGE_SK C0,
                                            A24.C1 C1,
                                            A24.C2 C2
                                        FROM
                                            (
                                                (
                                                    SELECT
                                                        A25.WR_WEB_PAGE_SK C0,
                                                        SUM(A25.WR_RETURN_AMT) C1,
                                                        SUM(A25.WR_NET_LOSS) C2
                                                    FROM
                                                        (
                                                            WEB_RETURNS A25
                                                            INNER JOIN DATE_DIM A26 ON (A25.WR_RETURNED_DATE_SK = A26.D_DATE_SK)
                                                        )
                                                    WHERE
                                                        (
                                                            A26.D_DATE <= DATE_ADD('day', 30, DATE('2001-08-11'))
                                                        )
                                                        AND (DATE('2001-08-11') <= A26.D_DATE)
                                                    GROUP BY
                                                        A25.WR_WEB_PAGE_SK
                                                ) A24
                                                INNER JOIN WEB_PAGE A27 ON (A24.C0 = A27.WP_WEB_PAGE_SK)
                                            )
                                    ) A23 ON (A18.C0 = A23.C0)
                                )
                        )
                    ) A1
                    INNER JOIN (
                        VALUES
                            1,
                            2,
                            3
                    ) A28 (C0) ON (MOD(LENGTH(A28.C0), 1) = MOD(LENGTH(A1.C0), 1))
                )
            GROUP BY
                A28.C0,
                CASE
                    WHEN (A28.C0 = 1) THEN A1.C0
                    WHEN (A28.C0 = 2) THEN A1.C0
                    ELSE NULL
                END,
                CASE
                    WHEN (A28.C0 = 1) THEN A1.C1
                    WHEN (A28.C0 = 2) THEN NULL
                    ELSE NULL
                END
        ) A0
        RIGHT OUTER JOIN (
            VALUES
                1,
                2,
                3
        ) A29 (C0) ON (A29.C0 = A0.C5)
    )
WHERE
    (
        (
            (A29.C0 = 3)
            AND (A0.C5 IS NULL)
        )
        OR (A0.C5 IS NOT NULL)
    )
ORDER BY
    1 ASC,
    2 ASC
limit
    100;