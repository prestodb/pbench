SELECT
    A1.C0 as `CHANNEL`,
    A1.C1 as `ID`,
    A1.C2 as `SALES`,
    A1.C3 as `RETURNS`,
    A1.C4 as `PROFIT`
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
                A2.C0 C0,
                A2.C1 C1,
                SUM(A2.C2) C2,
                SUM(A2.C3) C3,
                SUM(A2.C4) C4,
                A2.C5 C5
            FROM
                (
                    SELECT
                        CASE
                            WHEN (A31.C0 < 3) THEN A3.C0
                            ELSE NULL
                        END C0,
                        CASE
                            WHEN (A31.C0 < 2) THEN A3.C1
                            ELSE NULL
                        END C1,
                        A3.C2 C2,
                        A3.C3 C3,
                        A3.C4 C4,
                        A31.C0 C5
                    FROM
                        (
                            (
                                SELECT
                                    A4.C0 C0,
                                    A4.C1 C1,
                                    SUM(A4.C2) C2,
                                    SUM(A4.C3) C3,
                                    SUM(A4.C4) C4
                                FROM
                                    (
                                        (
                                            SELECT
                                                CAST('store channel' AS VARCHAR(15)) C0,
                                                A10.C0 C1,
                                                A10.C1 C2,
                                                COALESCE(A5.C1, 00000000000000000000000000000.00) C3,
                                                (
                                                    A10.C2 - COALESCE(A5.C2, 00000000000000000000000000000.00)
                                                ) C4
                                            FROM
                                                (
                                                    (
                                                        SELECT
                                                            A9.S_STORE_SK C0,
                                                            A6.C1 C1,
                                                            A6.C2 C2
                                                        FROM
                                                            (
                                                                (
                                                                    SELECT
                                                                        A7.SR_STORE_SK C0,
                                                                        SUM(A7.SR_RETURN_AMT) C1,
                                                                        SUM(A7.SR_NET_LOSS) C2
                                                                    FROM
                                                                        (
                                                                            store_returns A7
                                                                            INNER JOIN date_dim A8 ON (
                                                                                (A7.SR_RETURNED_DATE_SK = A8.D_DATE_SK)
                                                                                AND (
                                                                                    (A7.SR_RETURNED_DATE_SK <= 2451810)
                                                                                    AND (A7.SR_RETURNED_DATE_SK >= 2451780)
                                                                                )
                                                                                AND (DATE('2000-08-23') <= A8.D_DATE)
                                                                                AND (A8.D_DATE <= DATE('2000-09-22'))
                                                                            )
                                                                        )
                                                                    GROUP BY
                                                                        A7.SR_STORE_SK
                                                                ) A6
                                                                INNER JOIN store A9 ON (A6.C0 = A9.S_STORE_SK)
                                                            )
                                                    ) A5
                                                    RIGHT OUTER JOIN (
                                                        SELECT
                                                            A14.S_STORE_SK C0,
                                                            A11.C1 C1,
                                                            A11.C2 C2
                                                        FROM
                                                            (
                                                                (
                                                                    SELECT
                                                                        A12.SS_STORE_SK C0,
                                                                        SUM(A12.SS_EXT_SALES_PRICE) C1,
                                                                        SUM(A12.SS_NET_PROFIT) C2
                                                                    FROM
                                                                        (
                                                                            store_sales A12
                                                                            INNER JOIN date_dim A13 ON (
                                                                                (A12.SS_SOLD_DATE_SK = A13.D_DATE_SK)
                                                                                AND (
                                                                                    (A12.SS_SOLD_DATE_SK <= 2451810)
                                                                                    AND (A12.SS_SOLD_DATE_SK >= 2451780)
                                                                                )
                                                                                AND (DATE('2000-08-23') <= A13.D_DATE)
                                                                                AND (A13.D_DATE <= DATE('2000-09-22'))
                                                                            )
                                                                        )
                                                                    GROUP BY
                                                                        A12.SS_STORE_SK
                                                                ) A11
                                                                INNER JOIN store A14 ON (A11.C0 = A14.S_STORE_SK)
                                                            )
                                                    ) A10 ON (A10.C0 = A5.C0)
                                                )
                                        )
                                        UNION
                                        ALL (
                                            SELECT
                                                'catalog channel' C0,
                                                A15.C0 C1,
                                                A15.C1 C2,
                                                A18.C0 C3,
                                                (A15.C2 - A18.C1) C4
                                            FROM
                                                (
                                                    (
                                                        SELECT
                                                            A16.CS_CALL_CENTER_SK C0,
                                                            SUM(A16.CS_EXT_SALES_PRICE) C1,
                                                            SUM(A16.CS_NET_PROFIT) C2
                                                        FROM
                                                            (
                                                                catalog_sales A16
                                                                INNER JOIN date_dim A17 ON (
                                                                    (A16.CS_SOLD_DATE_SK = A17.D_DATE_SK)
                                                                    AND (
                                                                        (A16.CS_SOLD_DATE_SK <= 2451810)
                                                                        AND (A16.CS_SOLD_DATE_SK >= 2451780)
                                                                    )
                                                                    AND (DATE('2000-08-23') <= A17.D_DATE)
                                                                    AND (A17.D_DATE <= DATE('2000-09-22'))
                                                                )
                                                            )
                                                        GROUP BY
                                                            A16.CS_CALL_CENTER_SK
                                                    ) A15
                                                    INNER JOIN (
                                                        SELECT
                                                            SUM(A19.CR_RETURN_AMOUNT) C0,
                                                            SUM(A19.CR_NET_LOSS) C1
                                                        FROM
                                                            (
                                                                catalog_returns A19
                                                                INNER JOIN date_dim A20 ON (
                                                                    (A19.CR_RETURNED_DATE_SK = A20.D_DATE_SK)
                                                                    AND (
                                                                        (A19.CR_RETURNED_DATE_SK <= 2451810)
                                                                        AND (A19.CR_RETURNED_DATE_SK >= 2451780)
                                                                    )
                                                                    AND (DATE('2000-08-23') <= A20.D_DATE)
                                                                    AND (A20.D_DATE <= DATE('2000-09-22'))
                                                                )
                                                            )
                                                        GROUP BY
                                                            A19.CR_CALL_CENTER_SK
                                                    ) A18 ON (1 = 1)
                                                )
                                        )
                                        UNION
                                        ALL (
                                            SELECT
                                                CAST('web channel' AS VARCHAR(15)) C0,
                                                A26.C0 C1,
                                                A26.C1 C2,
                                                COALESCE(A21.C1, 00000000000000000000000000000.00) C3,
                                                (
                                                    A26.C2 - COALESCE(A21.C2, 00000000000000000000000000000.00)
                                                ) C4
                                            FROM
                                                (
                                                    (
                                                        SELECT
                                                            A25.WP_WEB_PAGE_SK C0,
                                                            A22.C1 C1,
                                                            A22.C2 C2
                                                        FROM
                                                            (
                                                                (
                                                                    SELECT
                                                                        A23.WR_WEB_PAGE_SK C0,
                                                                        SUM(A23.WR_RETURN_AMT) C1,
                                                                        SUM(A23.WR_NET_LOSS) C2
                                                                    FROM
                                                                        (
                                                                            web_returns A23
                                                                            INNER JOIN date_dim A24 ON (
                                                                                (A23.WR_RETURNED_DATE_SK = A24.D_DATE_SK)
                                                                                AND (
                                                                                    (A23.WR_RETURNED_DATE_SK <= 2451810)
                                                                                    AND (A23.WR_RETURNED_DATE_SK >= 2451780)
                                                                                )
                                                                                AND (DATE('2000-08-23') <= A24.D_DATE)
                                                                                AND (A24.D_DATE <= DATE('2000-09-22'))
                                                                            )
                                                                        )
                                                                    GROUP BY
                                                                        A23.WR_WEB_PAGE_SK
                                                                ) A22
                                                                INNER JOIN web_page A25 ON (A22.C0 = A25.WP_WEB_PAGE_SK)
                                                            )
                                                    ) A21
                                                    RIGHT OUTER JOIN (
                                                        SELECT
                                                            A30.WP_WEB_PAGE_SK C0,
                                                            A27.C1 C1,
                                                            A27.C2 C2
                                                        FROM
                                                            (
                                                                (
                                                                    SELECT
                                                                        A28.WS_WEB_PAGE_SK C0,
                                                                        SUM(A28.WS_EXT_SALES_PRICE) C1,
                                                                        SUM(A28.WS_NET_PROFIT) C2
                                                                    FROM
                                                                        (
                                                                            web_sales A28
                                                                            INNER JOIN date_dim A29 ON (
                                                                                (A28.WS_SOLD_DATE_SK = A29.D_DATE_SK)
                                                                                AND (
                                                                                    (A28.WS_SOLD_DATE_SK <= 2451810)
                                                                                    AND (A28.WS_SOLD_DATE_SK >= 2451780)
                                                                                )
                                                                                AND (DATE('2000-08-23') <= A29.D_DATE)
                                                                                AND (A29.D_DATE <= DATE('2000-09-22'))
                                                                            )
                                                                        )
                                                                    GROUP BY
                                                                        A28.WS_WEB_PAGE_SK
                                                                ) A27
                                                                INNER JOIN web_page A30 ON (A27.C0 = A30.WP_WEB_PAGE_SK)
                                                            )
                                                    ) A26 ON (A26.C0 = A21.C0)
                                                )
                                        )
                                    ) A4
                                GROUP BY
                                    A4.C0,
                                    A4.C1
                            ) A3
                            INNER JOIN (
                                VALUES
                                    1,
                                    2,
                                    3
                            ) A31 (C0) ON (1 = 1)
                        )
                ) A2
            GROUP BY
                A2.C5,
                A2.C0,
                A2.C1
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
    100