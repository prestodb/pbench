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
                    A9.C0 C1,
                    A9.C1 C2,
                    COALESCE(A4.C1, 00000000000000000000000000000.00) C3,
                    (
                        A9.C2 - COALESCE(A4.C2, 00000000000000000000000000000.00)
                    ) C4
                FROM
                    (
                        (
                            SELECT
                                A8.S_STORE_SK C0,
                                A5.C1 C1,
                                A5.C2 C2
                            FROM
                                (
                                    (
                                        SELECT
                                            A6.SR_STORE_SK C0,
                                            SUM(A6.SR_RETURN_AMT) C1,
                                            SUM(A6.SR_NET_LOSS) C2
                                        FROM
                                            (
                                                STORE_RETURNS A6
                                                INNER JOIN DATE_DIM A7 ON (A6.SR_RETURNED_DATE_SK = A7.D_DATE_SK)
                                            )
                                        WHERE
                                            (
                                                A7.D_DATE <= DATE_ADD('day', 30, DATE('2001-08-11'))
                                            )
                                            AND (DATE('2001-08-11') <= A7.D_DATE)
                                        GROUP BY
                                            A6.SR_STORE_SK
                                    ) A5
                                    INNER JOIN STORE A8 ON (A5.C0 = A8.S_STORE_SK)
                                )
                        ) A4
                        RIGHT OUTER JOIN (
                            SELECT
                                A13.S_STORE_SK C0,
                                A10.C1 C1,
                                A10.C2 C2
                            FROM
                                (
                                    (
                                        SELECT
                                            A11.SS_STORE_SK C0,
                                            SUM(A11.SS_EXT_SALES_PRICE) C1,
                                            SUM(A11.SS_NET_PROFIT) C2
                                        FROM
                                            (
                                                STORE_SALES A11
                                                INNER JOIN DATE_DIM A12 ON (A11.SS_SOLD_DATE_SK = A12.D_DATE_SK)
                                            )
                                        WHERE
                                            (
                                                A12.D_DATE <= DATE_ADD('day', 30, DATE('2001-08-11'))
                                            )
                                            AND (DATE('2001-08-11') <= A12.D_DATE)
                                        GROUP BY
                                            A11.SS_STORE_SK
                                    ) A10
                                    INNER JOIN STORE A13 ON (A10.C0 = A13.S_STORE_SK)
                                )
                        ) A9 ON (A9.C0 = A4.C0)
                    )
            )
            UNION
            ALL (
                SELECT
                    'catalog channel' C0,
                    A14.C0 C1,
                    A14.C1 C2,
                    A17.C0 C3,
                    (A14.C2 - A17.C1) C4
                FROM
                    (
                        SELECT
                            A15.CS_CALL_CENTER_SK C0,
                            SUM(A15.CS_EXT_SALES_PRICE) C1,
                            SUM(A15.CS_NET_PROFIT) C2
                        FROM
                            (
                                CATALOG_SALES A15
                                INNER JOIN DATE_DIM A16 ON (A15.CS_SOLD_DATE_SK = A16.D_DATE_SK)
                            )
                        WHERE
                            (
                                A16.D_DATE <= DATE_ADD('day', 30, DATE('2001-08-11'))
                            )
                            AND (DATE('2001-08-11') <= A16.D_DATE)
                        GROUP BY
                            A15.CS_CALL_CENTER_SK
                    ) A14,
                    (
                        SELECT
                            SUM(A18.CR_RETURN_AMOUNT) C0,
                            SUM(A18.CR_NET_LOSS) C1
                        FROM
                            (
                                CATALOG_RETURNS A18
                                INNER JOIN DATE_DIM A19 ON (A18.CR_RETURNED_DATE_SK = A19.D_DATE_SK)
                            )
                        WHERE
                            (
                                A19.D_DATE <= DATE_ADD('day', 30, DATE('2001-08-11'))
                            )
                            AND (DATE('2001-08-11') <= A19.D_DATE)
                        GROUP BY
                            A18.CR_CALL_CENTER_SK
                    ) A17
            )
            UNION
            ALL (
                SELECT
                    CAST('web channel' AS VARCHAR(15)) C0,
                    A20.C0 C1,
                    A20.C1 C2,
                    COALESCE(A25.C1, 00000000000000000000000000000.00) C3,
                    (
                        A20.C2 - COALESCE(A25.C2, 00000000000000000000000000000.00)
                    ) C4
                FROM
                    (
                        (
                            SELECT
                                A24.WP_WEB_PAGE_SK C0,
                                A21.C1 C1,
                                A21.C2 C2
                            FROM
                                (
                                    (
                                        SELECT
                                            A22.WS_WEB_PAGE_SK C0,
                                            SUM(A22.WS_EXT_SALES_PRICE) C1,
                                            SUM(A22.WS_NET_PROFIT) C2
                                        FROM
                                            (
                                                WEB_SALES A22
                                                INNER JOIN DATE_DIM A23 ON (A22.WS_SOLD_DATE_SK = A23.D_DATE_SK)
                                            )
                                        WHERE
                                            (
                                                A23.D_DATE <= DATE_ADD('day', 30, DATE('2001-08-11'))
                                            )
                                            AND (DATE('2001-08-11') <= A23.D_DATE)
                                        GROUP BY
                                            A22.WS_WEB_PAGE_SK
                                    ) A21
                                    INNER JOIN WEB_PAGE A24 ON (A21.C0 = A24.WP_WEB_PAGE_SK)
                                )
                        ) A20
                        LEFT OUTER JOIN (
                            SELECT
                                A29.WP_WEB_PAGE_SK C0,
                                A26.C1 C1,
                                A26.C2 C2
                            FROM
                                (
                                    (
                                        SELECT
                                            A27.WR_WEB_PAGE_SK C0,
                                            SUM(A27.WR_RETURN_AMT) C1,
                                            SUM(A27.WR_NET_LOSS) C2
                                        FROM
                                            (
                                                WEB_RETURNS A27
                                                INNER JOIN DATE_DIM A28 ON (A27.WR_RETURNED_DATE_SK = A28.D_DATE_SK)
                                            )
                                        WHERE
                                            (
                                                A28.D_DATE <= DATE_ADD('day', 30, DATE('2001-08-11'))
                                            )
                                            AND (DATE('2001-08-11') <= A28.D_DATE)
                                        GROUP BY
                                            A27.WR_WEB_PAGE_SK
                                    ) A26
                                    INNER JOIN WEB_PAGE A29 ON (A26.C0 = A29.WP_WEB_PAGE_SK)
                                )
                        ) A25 ON (A20.C0 = A25.C0)
                    )
            )
        ) A3
    GROUP BY
        A3.C0,
        A3.C1
),
A1 AS (
    SELECT
        "A30".C0 C0,
        NULL C1,
        SUM("A30".C2) C2,
        SUM("A30".C3) C3,
        SUM("A30".C4) C4
    FROM
        A2 "A30"
    GROUP BY
        "A30".C0
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
                SUM("A31".C2) C2,
                SUM("A31".C3) C3,
                SUM("A31".C4) C4
            FROM
                A1 "A31"
        )
        UNION
        ALL (
            SELECT
                "A32".C0 C0,
                "A32".C1 C1,
                "A32".C2 C2,
                "A32".C3 C3,
                "A32".C4 C4
            FROM
                A1 "A32"
        )
        UNION
        ALL (
            SELECT
                "A33".C0 C0,
                "A33".C1 C1,
                "A33".C2 C2,
                "A33".C3 C3,
                "A33".C4 C4
            FROM
                A2 "A33"
        )
    ) A0
ORDER BY
    1 ASC,
    2 ASC
limit
    100;